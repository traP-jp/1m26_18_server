use std::time::Duration;

use rand::RngExt;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::domain::model::{CompleteSongData, SongData};
use crate::domain::room::{HOST_GRACE_PERIOD, Room, WaitingRoom};
use crate::repository::room::{
    InsertHostError, InsertParticipantError, RoomRepository, SetReadyError, ShakeError,
    StartLiveError,
};
use crate::services::song::{SongService, SongServiceError};

#[derive(Clone)]
pub struct RoomService {
    repo: RoomRepository,
    song_service: SongService,
}

impl RoomService {
    pub fn new(repo: RoomRepository, song_service: SongService) -> Self {
        Self { repo, song_service }
    }

    pub async fn create_room(&self, song_url: &str) -> Result<(String, Uuid), CreateRoomError> {
        let song_data = self.song_service.get_song(song_url).await?;
        let complete = match song_data {
            SongData::Complete(c) => c,
            SongData::Incomplete(_) => return Err(CreateRoomError::SongNotComplete),
        };

        for _ in 0..100 {
            let mut rng = rand::rng();
            let n: u16 = rng.random_range(0..=9999);
            let room_id = format!("{n:04}");
            if self.repo.exists(&room_id) {
                continue;
            }
            let host_token = Uuid::now_v7();
            let room = Room::Waiting(WaitingRoom::new(complete, host_token.to_string()));
            self.repo.insert(room_id.clone(), room);
            return Ok((room_id, host_token));
        }

        Err(CreateRoomError::RoomIdExhausted)
    }

    /// Assigns a unique participant ID and registers the connection in the room.
    /// Fails if the room does not exist or its host has not joined yet.
    pub fn join_room(
        &self,
        room_id: &str,
        connection: wtransport::Connection,
    ) -> Result<Uuid, InsertParticipantError> {
        let participant_id = Uuid::now_v7();
        self.repo
            .insert_participant(room_id, participant_id, connection)?;

        tracing::info!(room_id = %room_id, participant_id = %participant_id, "participant joined room");

        Ok(participant_id)
    }

    /// Removes a participant from the room (on disconnect).
    pub fn leave_room(&self, room_id: &str, participant_id: &Uuid) {
        self.repo.remove_participant(room_id, participant_id);
        tracing::info!(room_id = %room_id, participant_id = %participant_id, "participant left room");
    }

    /// Marks a participant as ready (participant's own report). Returns
    /// whether this call caused the transition (i.e. the participant was not
    /// ready before); a repeated report is idempotent and returns `Ok(false)`.
    pub fn set_ready(&self, room_id: &str, participant_id: &Uuid) -> Result<bool, SetReadyError> {
        let newly_ready = self.repo.set_ready(room_id, participant_id)?;
        if newly_ready {
            tracing::info!(room_id = %room_id, participant_id = %participant_id, "participant is ready");
        } else {
            tracing::debug!(room_id = %room_id, participant_id = %participant_id, "participant is already ready");
        }
        Ok(newly_ready)
    }

    /// Validates the host token for a room (before accepting a WebTransport session).
    pub fn validate_host_token(&self, room_id: &str, token: &str) -> Result<(), InsertHostError> {
        self.repo.validate_host_token(room_id, token)
    }

    /// Returns whether the room's host is currently connected. Participants
    /// may join a room only while this is `true`; during the post-disconnect
    /// grace period it is `false` so new joins are blocked.
    pub fn host_joined(&self, room_id: &str) -> bool {
        self.repo.host_joined(room_id)
    }

    /// Validates the host token and registers the connection as the room's host.
    /// Handles the initial join, a reconnect within the grace period, and a
    /// takeover while a host is still connected (same token). A reconnect or
    /// takeover issues a fresh host id and cancels the pending grace-period
    /// removal. On takeover the previous host connection is closed
    /// immediately; its connection task then exits and its stale
    /// `disconnect_host` becomes a no-op via the host-id check.
    pub fn join_room_as_host(
        &self,
        room_id: &str,
        token: &str,
        connection: wtransport::Connection,
    ) -> Result<Uuid, InsertHostError> {
        let host_id = Uuid::now_v7();
        let replaced = self.repo.insert_host(room_id, token, host_id, connection)?;
        self.repo.cancel_host_grace(room_id);

        if let Some(old_connection) = replaced {
            old_connection.close(wtransport::VarInt::from_u32(409), b"host replaced");
            tracing::info!(room_id = %room_id, host_id = %host_id, "host replaced previous host");
        } else {
            tracing::info!(room_id = %room_id, host_id = %host_id, "host joined room");
        }

        Ok(host_id)
    }

    /// Returns a clone of the room host's connection, if the host has joined.
    pub fn host_connection(&self, room_id: &str) -> Option<wtransport::Connection> {
        self.repo.host_connection(room_id)
    }

    /// Returns clones of the room participants' connections, along with their
    /// ids, if the host has joined.
    pub fn participant_connections(
        &self,
        room_id: &str,
    ) -> Option<Vec<(Uuid, wtransport::Connection)>> {
        self.repo.participant_connections(room_id)
    }

    /// Transitions the room to live with the start time (unix microseconds)
    /// announced by the host.
    pub fn start_live(&self, room_id: &str, start_time: u64) -> Result<(), StartLiveError> {
        self.repo.start_live(room_id, start_time)?;
        tracing::info!(room_id = %room_id, start_time, "live started");
        Ok(())
    }

    /// Marks the host as disconnected and schedules room removal after
    /// `grace` unless the same-token host reconnects first.
    ///
    /// The sync-rate update task is intentionally kept alive during the grace
    /// period so a reconnected host resumes receiving reports; it is only
    /// cancelled by the eventual `remove_room`.
    pub fn disconnect_host(&self, room_id: &str, host_id: &Uuid, grace: Duration) {
        if !self.repo.disconnect_host(room_id, host_id) {
            return;
        }
        tracing::info!(room_id = %room_id, host_id = %host_id, "host disconnected, grace period started");

        let service = self.clone();
        let room_id_owned = room_id.to_string();
        let host_id_owned = *host_id;
        let token = CancellationToken::new();
        self.repo
            .set_host_grace_cancel(room_id_owned.clone(), host_id_owned, token.clone());
        tokio::spawn(async move {
            tokio::select! {
                () = sleep(grace) => {},
                () = token.cancelled() => {
                    tracing::debug!(room_id = %room_id_owned, "host grace timer cancelled (reconnected)");
                    return;
                }
            }
            if !service
                .repo
                .take_host_grace_if_same(&room_id_owned, &host_id_owned, &token)
            {
                // Reconnected (or room recreated): a newer generation owns the slot.
                return;
            }
            service.remove_room(&room_id_owned);
            tracing::info!(room_id = %room_id_owned, "room removed after host grace expiry");
        });
    }

    /// Same as [`RoomService::disconnect_host`] with [`HOST_GRACE_PERIOD`].
    pub fn disconnect_host_with_default_grace(&self, room_id: &str, host_id: &Uuid) {
        self.disconnect_host(room_id, host_id, HOST_GRACE_PERIOD);
    }

    /// Removes the room and closes all remaining participant connections.
    /// Called after the host grace period expires (or directly in tests).
    /// The room's sync-rate update task is cancelled via the repository so it
    /// does not linger after removal.
    pub fn remove_room(&self, room_id: &str) {
        if let Some(room) = self.repo.remove_room(room_id) {
            if let Some(participants) = room.participants() {
                for (participant_id, participant) in participants {
                    participant
                        .connection()
                        .close(wtransport::VarInt::from_u32(410), b"room closed");
                    tracing::info!(room_id = %room_id, participant_id = %participant_id, "participant connection closed");
                }
            }
            tracing::info!(room_id = %room_id, "room removed");
        }
    }

    pub fn exists(&self, room_id: &str) -> bool {
        self.repo.exists(room_id)
    }

    /// Returns a clone of the room's song data. `None` if the room does not exist.
    pub fn get_room_song(&self, room_id: &str) -> Option<CompleteSongData> {
        self.repo.get_song(room_id)
    }

    /// Records one participant device-shake report (sent unreliably as a
    /// datagram).
    pub fn record_shake(
        &self,
        room_id: &str,
        participant_id: &Uuid,
        detected_at: u64,
    ) -> Result<(), ShakeError> {
        self.repo
            .record_shake(room_id, *participant_id, detected_at)?;
        tracing::debug!(
            room_id = %room_id,
            participant_id = %participant_id,
            detected_at,
            "participant device shake recorded"
        );
        Ok(())
    }

    /// The room's overall sync rate (0-100) of the device shakes attributed
    /// to the beat starting at `beat_at`, or `None` if no valid shake falls
    /// within the beat's tolerance window.
    pub fn sync_rate(&self, room_id: &str, beat_at: u64) -> Option<u8> {
        self.repo.sync_rate(room_id, beat_at)
    }

    /// Absolute start times (unix microseconds) of the live's beats, used to
    /// schedule per-beat sync-rate reports.
    pub fn beat_schedule(&self, room_id: &str) -> Option<Vec<u64>> {
        self.repo.beat_schedule(room_id)
    }

    /// Registers the cancellation token for the room's sync-rate update task.
    pub fn set_sync_cancel(&self, room_id: String, token: CancellationToken) {
        self.repo.set_sync_cancel(room_id, token);
    }

    /// Drops the stored sync-rate cancellation token only if it matches
    /// `token` (see `RoomRepository::remove_sync_cancel_if_same`).
    pub fn remove_sync_cancel_if_same(&self, room_id: &str, token: &CancellationToken) {
        self.repo.remove_sync_cancel_if_same(room_id, token);
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreateRoomError {
    #[error(transparent)]
    SongService(#[from] SongServiceError),
    #[error("song data is incomplete")]
    SongNotComplete,
    #[error("failed to generate unique room id")]
    RoomIdExhausted,
}
