use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use uuid::Uuid;

use crate::domain::room::{
    CALIBRATION_SOUND_COUNT, DetectionError, DetectionOutcome, Host, Participant, Room,
};

#[derive(Clone, Default)]
pub struct RoomRepository {
    inner: Arc<RwLock<HashMap<String, Room>>>,
}

impl RoomRepository {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    pub fn exists(&self, room_id: &str) -> bool {
        self.inner.read().contains_key(room_id)
    }

    pub fn insert(&self, room_id: String, room: Room) {
        self.inner.write().insert(room_id, room);
    }

    /// Returns whether the room's host has joined. Participants may join a
    /// room only after its host.
    pub fn host_joined(&self, room_id: &str) -> bool {
        matches!(self.inner.read().get(room_id), Some(Room::HostJoined(_)))
    }

    /// Registers a participant in the room. Returns an error if the room does
    /// not exist or its host has not joined yet.
    pub fn insert_participant(
        &self,
        room_id: &str,
        participant_id: Uuid,
        connection: wtransport::Connection,
    ) -> Result<(), InsertParticipantError> {
        let mut map = self.inner.write();
        match map.get_mut(room_id) {
            Some(room) => match room.participants_mut() {
                Some(participants) => {
                    participants.insert(participant_id, Participant::new(connection));
                    Ok(())
                }
                None => Err(InsertParticipantError::HostNotJoined),
            },
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
            Some(Room::HostJoined(_)) => Err(InsertHostError::HostAlreadyJoined),
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
            Some(room @ Room::HostJoined(_)) => {
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
            _ => None,
        }
    }

    /// Removes and returns the room. Returns `None` if the room does not exist.
    pub fn remove_room(&self, room_id: &str) -> Option<Room> {
        self.inner.write().remove(room_id)
    }

    /// Starts (or restarts) a latency calibration round with the given host
    /// sound times. Overwrites any in-progress round; already determined lags
    /// are kept until they are determined again.
    pub fn start_calibration(
        &self,
        room_id: &str,
        host_times: [u64; CALIBRATION_SOUND_COUNT],
    ) -> Result<(), CalibrationError> {
        let mut map = self.inner.write();
        match map.get_mut(room_id) {
            Some(room) => {
                let joined = room
                    .host_joined_mut()
                    .ok_or(CalibrationError::HostNotJoined)?;
                joined.start_calibration(host_times);
                Ok(())
            }
            None => Err(CalibrationError::RoomNotFound),
        }
    }

    /// Records one participant sound detection (with the detected sound's
    /// index) for the room's current calibration round. When the participant
    /// has reported every host sound, the median difference is stored as
    /// their lag.
    pub fn record_detection(
        &self,
        room_id: &str,
        participant_id: &Uuid,
        sound_index: usize,
        detected_at: u64,
    ) -> Result<DetectionOutcome, CalibrationError> {
        let mut map = self.inner.write();
        match map.get_mut(room_id) {
            Some(room) => {
                let joined = room
                    .host_joined_mut()
                    .ok_or(CalibrationError::HostNotJoined)?;
                let calibration = joined
                    .calibration_mut()
                    .ok_or(CalibrationError::NoActiveCalibration)?;
                let outcome = calibration
                    .record_detection(*participant_id, sound_index, detected_at)
                    .map_err(CalibrationError::Detection)?;
                if let DetectionOutcome::Completed { lag } = outcome {
                    joined.insert_lag(*participant_id, lag);
                }
                Ok(outcome)
            }
            None => Err(CalibrationError::RoomNotFound),
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
            _ => None,
        }
    }

    #[cfg(test)]
    pub fn participant_lag(&self, room_id: &str, participant_id: &Uuid) -> Option<i64> {
        match self.inner.read().get(room_id) {
            Some(Room::HostJoined(joined)) => joined.lag(participant_id),
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
}

#[derive(Debug, thiserror::Error)]
pub enum InsertParticipantError {
    #[error("room not found")]
    RoomNotFound,
    #[error("host has not joined yet")]
    HostNotJoined,
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
pub enum CalibrationError {
    #[error("room not found")]
    RoomNotFound,
    #[error("host has not joined yet")]
    HostNotJoined,
    #[error("no calibration round in progress")]
    NoActiveCalibration,
    #[error("invalid sound detection: {0}")]
    Detection(#[from] DetectionError),
}
