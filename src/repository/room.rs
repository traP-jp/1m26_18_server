use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use uuid::Uuid;

use crate::domain::room::{Host, Room};

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

    /// Registers a participant in the room. Returns an error if the room does not exist.
    pub fn insert_participant(
        &self,
        room_id: &str,
        participant_id: Uuid,
        connection: wtransport::Connection,
    ) -> Result<(), InsertParticipantError> {
        let mut map = self.inner.write();
        match map.get_mut(room_id) {
            Some(room) => {
                room.participants_mut().insert(participant_id, connection);
                Ok(())
            }
            None => Err(InsertParticipantError::RoomNotFound),
        }
    }

    /// Removes a participant from the room. Does nothing if the room or participant does not exist.
    pub fn remove_participant(&self, room_id: &str, participant_id: &Uuid) {
        let mut map = self.inner.write();
        if let Some(room) = map.get_mut(room_id) {
            room.participants_mut().remove(participant_id);
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
        match map.get(room_id) {
            Some(Room::Waiting(waiting)) => {
                if waiting.host_token() != token {
                    return Err(InsertHostError::InvalidToken);
                }
            }
            Some(Room::HostJoined(_)) => return Err(InsertHostError::HostAlreadyJoined),
            None => return Err(InsertHostError::RoomNotFound),
        }
        let Room::Waiting(waiting) = map.remove(room_id).unwrap() else {
            unreachable!("variant checked above")
        };
        map.insert(
            room_id.to_string(),
            Room::HostJoined(waiting.join_host(Host::new(host_id, connection))),
        );
        Ok(())
    }

    /// Removes and returns the room. Returns `None` if the room does not exist.
    pub fn remove_room(&self, room_id: &str) -> Option<Room> {
        self.inner.write().remove(room_id)
    }

    #[cfg(test)]
    pub fn participant_count(&self, room_id: &str) -> Option<usize> {
        self.inner
            .read()
            .get(room_id)
            .map(Room::participants)
            .map(HashMap::len)
    }

    #[cfg(test)]
    pub fn host_id(&self, room_id: &str) -> Option<Uuid> {
        match self.inner.read().get(room_id) {
            Some(Room::HostJoined(joined)) => Some(joined.host().id()),
            _ => None,
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InsertParticipantError {
    #[error("room not found")]
    RoomNotFound,
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
