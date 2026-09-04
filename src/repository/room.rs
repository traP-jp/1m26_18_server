use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use crate::domain::model::CompleteSongData;
use crate::domain::room::{Host, Participant, Room, ShakeOutcome};

#[derive(Clone, Default)]
pub struct RoomRepository {
    inner: Arc<RwLock<HashMap<String, Room>>>,
    /// Cancellation tokens for per-room `run_sync_rate_updates` tasks.
    /// Kept outside `Room` so the domain layer stays free of tokio types.
    sync_cancels: Arc<RwLock<HashMap<String, CancellationToken>>>,
}

impl RoomRepository {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            sync_cancels: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn exists(&self, room_id: &str) -> bool {
        self.inner.read().contains_key(room_id)
    }

    pub fn insert(&self, room_id: String, room: Room) {
        self.inner.write().insert(room_id, room);
    }

    /// Returns a clone of the room's song data. `None` if the room does not exist.
    pub fn get_song(&self, room_id: &str) -> Option<CompleteSongData> {
        self.inner
            .read()
            .get(room_id)
            .map(|room| room.song().clone())
    }

    /// Returns whether the room's host has joined. Participants may join a
    /// room only after its host.
    pub fn host_joined(&self, room_id: &str) -> bool {
        matches!(self.inner.read().get(room_id), Some(Room::HostJoined(_)))
    }

    /// Registers a participant in the room. Returns an error if the room does
    /// not exist, its host has not joined yet, or the live has already
    /// started.
    pub fn insert_participant(
        &self,
        room_id: &str,
        participant_id: Uuid,
        connection: wtransport::Connection,
    ) -> Result<(), InsertParticipantError> {
        let mut map = self.inner.write();
        match map.get_mut(room_id) {
            Some(room) => {
                if matches!(room, Room::Live(_)) {
                    return Err(InsertParticipantError::LiveStarted);
                }
                match room.participants_mut() {
                    Some(participants) => {
                        participants.insert(participant_id, Participant::new(connection));
                        Ok(())
                    }
                    None => Err(InsertParticipantError::HostNotJoined),
                }
            }
            None => Err(InsertParticipantError::RoomNotFound),
        }
    }

    /// Removes a participant from the room. Does nothing if the room or participant does not exist.
    pub fn remove_participant(&self, room_id: &str, participant_id: &Uuid) {
        let mut map = self.inner.write();
        if let Some(room) = map.get_mut(room_id)
            && let Some(participants) = room.participants_mut()
        {
            participants.remove(participant_id);
        }
    }

    /// Marks a participant as ready. Returns whether this call caused the
    /// transition (i.e. the participant was not ready before); a repeated
    /// report is idempotent and returns `Ok(false)`.
    pub fn set_ready(&self, room_id: &str, participant_id: &Uuid) -> Result<bool, SetReadyError> {
        let mut map = self.inner.write();
        match map.get_mut(room_id) {
            Some(room) => {
                let joined = room.host_joined_mut().ok_or(SetReadyError::HostNotJoined)?;
                joined
                    .set_ready(participant_id)
                    .ok_or(SetReadyError::ParticipantNotFound)
            }
            None => Err(SetReadyError::RoomNotFound),
        }
    }

    /// Validates the host token against the room state.
    pub fn validate_host_token(&self, room_id: &str, token: &str) -> Result<(), InsertHostError> {
        match self.inner.read().get(room_id) {
            Some(Room::Waiting(waiting)) => {
                if waiting.host_token() == token {
                    Ok(())
                } else {
                    Err(InsertHostError::InvalidToken)
                }
            }
            Some(Room::HostJoined(_) | Room::Live(_)) => Err(InsertHostError::HostAlreadyJoined),
            None => Err(InsertHostError::RoomNotFound),
        }
    }

    /// Validates the host token and transitions the room to host-joined, atomically.
    pub fn insert_host(
        &self,
        room_id: &str,
        token: &str,
        host_id: Uuid,
        connection: wtransport::Connection,
    ) -> Result<(), InsertHostError> {
        let mut map = self.inner.write();
        let waiting = match map.remove(room_id) {
            Some(Room::Waiting(waiting)) => waiting,
            Some(room @ (Room::HostJoined(_) | Room::Live(_))) => {
                map.insert(room_id.to_string(), room);
                return Err(InsertHostError::HostAlreadyJoined);
            }
            None => return Err(InsertHostError::RoomNotFound),
        };
        if waiting.host_token() != token {
            map.insert(room_id.to_string(), Room::Waiting(waiting));
            return Err(InsertHostError::InvalidToken);
        }
        map.insert(
            room_id.to_string(),
            Room::HostJoined(Box::new(waiting.join_host(Host::new(host_id, connection)))),
        );
        Ok(())
    }

    /// Returns a clone of the room host's connection. `None` if the room does
    /// not exist or the host has not joined yet.
    pub fn host_connection(&self, room_id: &str) -> Option<wtransport::Connection> {
        match self.inner.read().get(room_id) {
            Some(Room::HostJoined(joined)) => Some(joined.host().connection().clone()),
            Some(Room::Live(live)) => Some(live.host().connection().clone()),
            _ => None,
        }
    }

    /// Returns clones of the room participants' connections, along with their
    /// ids. `None` if the room does not exist or its host has not joined yet.
    pub fn participant_connections(
        &self,
        room_id: &str,
    ) -> Option<Vec<(Uuid, wtransport::Connection)>> {
        self.inner
            .read()
            .get(room_id)
            .and_then(Room::participants)
            .map(|participants| {
                participants
                    .iter()
                    .map(|(id, participant)| (*id, participant.connection().clone()))
                    .collect()
            })
    }

    /// Removes and returns the room. Returns `None` if the room does not exist.
    /// Any sync-rate update task is cancelled first so it does not linger.
    pub fn remove_room(&self, room_id: &str) -> Option<Room> {
        self.cancel_sync_updates(room_id);
        self.inner.write().remove(room_id)
    }

    /// Registers the cancellation token for the room's sync-rate update task.
    ///
    /// If the room is already gone (e.g. the host disconnected concurrently
    /// with `LiveStart`), the token is cancelled immediately instead of being
    /// stored so the spawned task exits without sleeping through the song.
    pub fn set_sync_cancel(&self, room_id: String, token: CancellationToken) {
        if !self.inner.read().contains_key(&room_id) {
            token.cancel();
            return;
        }
        self.sync_cancels.write().insert(room_id, token);
    }

    /// Cancels and drops the room's sync-rate update task, if any.
    pub fn cancel_sync_updates(&self, room_id: &str) {
        if let Some(token) = self.sync_cancels.write().remove(room_id) {
            token.cancel();
        }
    }

    /// Drops the stored token only if it is the same cancellation graph as
    /// `token`. Prevents a finished task from deleting a recreated room's
    /// (same id) token.
    pub fn remove_sync_cancel_if_same(&self, room_id: &str, token: &CancellationToken) {
        let mut cancels = self.sync_cancels.write();
        if cancels.get(room_id).is_some_and(|stored| stored == token) {
            cancels.remove(room_id);
        }
    }

    /// Transitions the room to live with the given start time (unix
    /// microseconds) announced by the host. Returns an error if the room does
    /// not exist, its host has not joined yet, or the live has already
    /// started.
    pub fn start_live(&self, room_id: &str, start_time: u64) -> Result<(), StartLiveError> {
        let mut map = self.inner.write();
        match map.remove(room_id) {
            Some(Room::HostJoined(joined)) => {
                map.insert(
                    room_id.to_string(),
                    Room::Live(Box::new(joined.start_live(start_time))),
                );
                Ok(())
            }
            Some(room @ Room::Waiting(_)) => {
                map.insert(room_id.to_string(), room);
                Err(StartLiveError::HostNotJoined)
            }
            Some(room @ Room::Live(_)) => {
                map.insert(room_id.to_string(), room);
                Err(StartLiveError::AlreadyLive)
            }
            None => Err(StartLiveError::RoomNotFound),
        }
    }

    /// Records one participant device-shake report.
    pub fn record_shake(
        &self,
        room_id: &str,
        participant_id: Uuid,
        detected_at: u64,
    ) -> Result<(), ShakeError> {
        let mut map = self.inner.write();
        match map.get_mut(room_id) {
            Some(room) => {
                let live = room.live_mut().ok_or(ShakeError::NotLive)?;
                match live.record_shake(participant_id, detected_at) {
                    ShakeOutcome::Recorded => Ok(()),
                    ShakeOutcome::UnknownParticipant => Err(ShakeError::ParticipantNotFound),
                }
            }
            None => Err(ShakeError::RoomNotFound),
        }
    }

    /// The room's overall sync rate (0-100) of the device shakes attributed
    /// to the beat starting at `beat_at`, or `None` if no valid shake falls
    /// within the beat's tolerance window. `None` is also returned when the
    /// room does not exist or its live has not started.
    pub fn sync_rate(&self, room_id: &str, beat_at: u64) -> Option<u8> {
        match self.inner.read().get(room_id) {
            Some(Room::Live(live)) => live.sync_rate(beat_at),
            _ => None,
        }
    }

    /// Absolute start times (unix microseconds) of the live's beats, used to
    /// schedule per-beat sync-rate reports. `None` if the room does not exist
    /// or its live has not started.
    pub fn beat_schedule(&self, room_id: &str) -> Option<Vec<u64>> {
        match self.inner.read().get(room_id) {
            Some(Room::Live(live)) => Some(live.beat_schedule()),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn participant_count(&self, room_id: &str) -> Option<usize> {
        self.inner
            .read()
            .get(room_id)
            .and_then(Room::participants)
            .map(HashMap::len)
    }

    #[cfg(test)]
    pub fn host_id(&self, room_id: &str) -> Option<Uuid> {
        match self.inner.read().get(room_id) {
            Some(Room::HostJoined(joined)) => Some(joined.host().id()),
            Some(Room::Live(live)) => Some(live.host().id()),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn start_time(&self, room_id: &str) -> Option<u64> {
        match self.inner.read().get(room_id) {
            Some(Room::Live(live)) => Some(live.start_time()),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn participant_is_ready(&self, room_id: &str, participant_id: &Uuid) -> Option<bool> {
        self.inner
            .read()
            .get(room_id)
            .and_then(Room::participants)
            .and_then(|participants| participants.get(participant_id))
            .map(Participant::is_ready)
    }

    #[cfg(test)]
    pub fn participant_shake_count(&self, room_id: &str, participant_id: &Uuid) -> Option<usize> {
        match self.inner.read().get(room_id) {
            Some(Room::Live(live)) => live.shake_count(participant_id),
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn has_sync_cancel(&self, room_id: &str) -> bool {
        self.sync_cancels.read().contains_key(room_id)
    }

    #[cfg(test)]
    pub fn sync_cancel_token(&self, room_id: &str) -> Option<CancellationToken> {
        self.sync_cancels.read().get(room_id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::domain::room::WaitingRoom;

    fn waiting_room() -> Room {
        let song = serde_json::from_value(serde_json::json!({
            "artist": "artist",
            "durationMs": 1000.0,
            "beats": [{"startsAtMs": 0.0, "endsAtMs": 500.0}],
            "phrases": [],
            "segments": [{"isChorus": false, "startsAtMs": 0.0, "endsAtMs": 1000.0}],
            "title": "title"
        }))
        .expect("valid dummy song JSON");
        Room::Waiting(WaitingRoom::new(song, "token".to_string()))
    }

    #[test]
    fn set_sync_cancel_on_missing_room_cancels_immediately() {
        let repo = RoomRepository::new();
        let token = CancellationToken::new();
        repo.set_sync_cancel("0000".to_string(), token.clone());
        assert!(token.is_cancelled());
        assert!(!repo.has_sync_cancel("0000"));
    }

    #[test]
    fn remove_room_cancels_sync_task() {
        let repo = RoomRepository::new();
        repo.insert("0001".to_string(), waiting_room());
        let token = CancellationToken::new();
        repo.set_sync_cancel("0001".to_string(), token.clone());
        assert!(repo.has_sync_cancel("0001"));
        assert!(!token.is_cancelled());

        let _ = repo.remove_room("0001");
        assert!(token.is_cancelled());
        assert!(!repo.has_sync_cancel("0001"));
    }

    #[test]
    fn remove_sync_cancel_if_same_keeps_recreated_room_token() {
        let repo = RoomRepository::new();
        repo.insert("0002".to_string(), waiting_room());
        let old = CancellationToken::new();
        repo.set_sync_cancel("0002".to_string(), old.clone());

        // Room id is reused for a new live: the new task overwrites the token.
        let new = CancellationToken::new();
        repo.set_sync_cancel("0002".to_string(), new.clone());

        // The finished old task must not delete the new room's token.
        repo.remove_sync_cancel_if_same("0002", &old);
        assert!(repo.has_sync_cancel("0002"));
        assert_eq!(repo.sync_cancel_token("0002"), Some(new.clone()));

        repo.remove_sync_cancel_if_same("0002", &new);
        assert!(!repo.has_sync_cancel("0002"));
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InsertParticipantError {
    #[error("room not found")]
    RoomNotFound,
    #[error("host has not joined yet")]
    HostNotJoined,
    #[error("live has already started")]
    LiveStarted,
}

#[derive(Debug, thiserror::Error)]
pub enum SetReadyError {
    #[error("room not found")]
    RoomNotFound,
    #[error("host has not joined yet")]
    HostNotJoined,
    #[error("participant not found")]
    ParticipantNotFound,
}

#[derive(Debug, thiserror::Error)]
pub enum InsertHostError {
    #[error("room not found")]
    RoomNotFound,
    #[error("invalid host token")]
    InvalidToken,
    #[error("host already joined")]
    HostAlreadyJoined,
}

#[derive(Debug, thiserror::Error)]
pub enum StartLiveError {
    #[error("room not found")]
    RoomNotFound,
    #[error("host has not joined yet")]
    HostNotJoined,
    #[error("live has already started")]
    AlreadyLive,
}

#[derive(Debug, thiserror::Error)]
pub enum ShakeError {
    #[error("room not found")]
    RoomNotFound,
    #[error("live has not started")]
    NotLive,
    #[error("participant not found")]
    ParticipantNotFound,
}
