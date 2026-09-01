use std::{collections::HashMap, sync::Arc};

use parking_lot::RwLock;
use uuid::Uuid;

use crate::domain::room::Room;

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
            Some(Room::Waiting(waiting)) => {
                waiting.insert_participant(participant_id, connection);
                Ok(())
            }
            None => Err(InsertParticipantError::RoomNotFound),
        }
    }

    /// Removes a participant from the room. Does nothing if the room or participant does not exist.
    pub fn remove_participant(&self, room_id: &str, participant_id: &Uuid) {
        let mut map = self.inner.write();
        if let Some(Room::Waiting(waiting)) = map.get_mut(room_id) {
            waiting.remove_participant(participant_id);
        }
    }

    #[cfg(test)]
    pub fn participant_count(&self, room_id: &str) -> Option<usize> {
        self.inner
            .read()
            .get(room_id)
            .map(|Room::Waiting(w)| w.participants().len())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum InsertParticipantError {
    #[error("room not found")]
    RoomNotFound,
}
