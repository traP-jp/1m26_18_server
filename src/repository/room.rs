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
    /// Cancellation tokens for per-room host grace-period timers.
    /// `(disconnected host id, token)` is stored so a stale timer cannot
    /// remove a reconnected or recreated room.
    host_grace_cancels: Arc<RwLock<HashMap<String, (Uuid, CancellationToken)>>>,
}

impl RoomRepository {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
            sync_cancels: Arc::new(RwLock::new(HashMap::new())),
            host_grace_cancels: Arc::new(RwLock::new(HashMap::new())),
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

    /// Returns whether the room's host is currently connected. Participants
    /// may join a room only while this is `true`: before the first host join
    /// and during the post-disconnect grace period it is `false` so new
    /// joins are blocked (the absent host could not be notified).
    pub fn host_joined(&self, room_id: &str) -> bool {
        self.inner
            .read()
            .get(room_id)
            .is_some_and(Room::is_host_connected)
    }

    /// Registers a participant in the room. Returns an error if the room does
    /// not exist, its host is not currently connected (not yet joined or in
    /// the post-disconnect grace period), or the live has already started.
    pub fn insert_participant(
        &self,
        room_id: &str,
        participant_id: Uuid,
        connection: wtransport::Connection,
    ) -> Result<(), InsertParticipantError> {
        let mut map = self.inner.write();
        match map.get_mut(room_id) {
            Some(room) => {
                if !room.is_host_connected() {
                    return Err(InsertParticipantError::HostNotJoined);
                }
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
    ///
    /// The initial join (`Waiting` + matching token), a reconnect during the
    /// grace period (disconnected + matching token), and a takeover while a
    /// host is still connected (connected + matching token) are accepted. A
    /// mismatched token is rejected with [`InsertHostError::InvalidToken`].
    pub fn validate_host_token(&self, room_id: &str, token: &str) -> Result<(), InsertHostError> {
        match self.inner.read().get(room_id) {
            Some(Room::Waiting(waiting)) => {
                if waiting.host_token() == token {
                    Ok(())
                } else {
                    Err(InsertHostError::InvalidToken)
                }
            }
            Some(room @ (Room::HostJoined(_) | Room::Live(_))) => {
                if room.host_token().is_some_and(|t| t == token) {
                    Ok(())
                } else {
                    Err(InsertHostError::InvalidToken)
                }
            }
            None => Err(InsertHostError::RoomNotFound),
        }
    }

    /// Validates the host token and registers the host connection,
    /// atomically. Handles the initial join (`Waiting` -> `HostJoined`), a
    /// reconnect during the grace period (same token, host slot empty), and a
    /// takeover while a host is still connected (same token, host slot
    /// occupied). Participants / live state are preserved on reconnect and
    /// takeover. A reconnect or initial join issues a fresh host id; callers
    /// must cancel the pending grace timer (see `cancel_host_grace`).
    ///
    /// Returns the previous host connection when an active host was replaced;
    /// the caller is responsible for closing it immediately.
    pub fn insert_host(
        &self,
        room_id: &str,
        token: &str,
        host_id: Uuid,
        connection: wtransport::Connection,
    ) -> Result<Option<wtransport::Connection>, InsertHostError> {
        let mut map = self.inner.write();
        match map.get_mut(room_id) {
            Some(Room::Waiting(_)) => {}
            Some(room @ (Room::HostJoined(_) | Room::Live(_))) => {
                if room.host_token().is_some_and(|t| t == token) {
                    // Grace-period reconnect or active-host takeover: keep
                    // participants / live state.
                    let old = room.replace_host(Host::new(host_id, connection));
                    return Ok(old.map(|host| host.connection().clone()));
                }
                return Err(InsertHostError::InvalidToken);
            }
            None => return Err(InsertHostError::RoomNotFound),
        }
        // Initial join: consume the `Waiting` room. The borrow above has ended.
        let waiting = match map.remove(room_id) {
            Some(Room::Waiting(waiting)) => waiting,
            // Re-checked above; another task cannot interleave thanks to the
            // write lock, so any other state is unreachable.
            _ => return Err(InsertHostError::RoomNotFound),
        };
        if waiting.host_token() != token {
            map.insert(room_id.to_string(), Room::Waiting(waiting));
            return Err(InsertHostError::InvalidToken);
        }
        map.insert(
            room_id.to_string(),
            Room::HostJoined(Box::new(waiting.join_host(Host::new(host_id, connection)))),
        );
        Ok(None)
    }

    /// Marks the host as disconnected (grace period start). Returns `true`
    /// only if `host_id` matches the currently connected host; otherwise the
    /// call is a no-op (e.g. a stale connection task racing a reconnect or a
    /// replaced host racing a takeover).
    pub fn disconnect_host(&self, room_id: &str, host_id: &Uuid) -> bool {
        match self.inner.write().get_mut(room_id) {
            Some(room) => room.disconnect_host(host_id),
            None => false,
        }
    }

    /// Registers the cancellation token for the room's host grace-period
    /// timer. Overwrites any previous timer for the room.
    pub fn set_host_grace_cancel(&self, room_id: String, host_id: Uuid, token: CancellationToken) {
        self.host_grace_cancels
            .write()
            .insert(room_id, (host_id, token));
    }

    /// Cancels and drops the room's host grace-period timer, if any. Called
    /// when the host reconnects within the grace period.
    pub fn cancel_host_grace(&self, room_id: &str) {
        if let Some((_, token)) = self.host_grace_cancels.write().remove(room_id) {
            token.cancel();
        }
    }

    /// Drops the stored grace timer only if it is the same generation as
    /// `token` (same cancellation graph) and its disconnected host id still
    /// matches `host_id`. Returns `true` when the caller owns the current
    /// generation and may proceed with room removal.
    pub fn take_host_grace_if_same(
        &self,
        room_id: &str,
        host_id: &Uuid,
        token: &CancellationToken,
    ) -> bool {
        let mut cancels = self.host_grace_cancels.write();
        match cancels.get(room_id) {
            Some((stored_id, stored)) if stored_id == host_id && stored == token => {
                cancels.remove(room_id);
                true
            }
            _ => false,
        }
    }

    /// Returns a clone of the room host's connection. `None` if the room does
    /// not exist, the host has not joined yet, or the host is in the
    /// post-disconnect grace period.
    pub fn host_connection(&self, room_id: &str) -> Option<wtransport::Connection> {
        match self.inner.read().get(room_id) {
            Some(Room::HostJoined(joined)) => joined.host().map(|h| h.connection().clone()),
            Some(Room::Live(live)) => live.host().map(|h| h.connection().clone()),
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
    /// Any pending host grace timer entry is dropped as well.
    pub fn remove_room(&self, room_id: &str) -> Option<Room> {
        self.cancel_sync_updates(room_id);
        self.host_grace_cancels.write().remove(room_id);
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
            Some(Room::HostJoined(joined)) => joined.host().map(Host::id),
            Some(Room::Live(live)) => live.host().map(Host::id),
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

    #[cfg(test)]
    pub fn has_host_grace(&self, room_id: &str) -> bool {
        self.host_grace_cancels.read().contains_key(room_id)
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
