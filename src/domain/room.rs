use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::model::CompleteSongData;

pub enum Room {
    Waiting(WaitingRoom),
}

pub struct WaitingRoom {
    song: CompleteSongData,
    participants: HashMap<Uuid, wtransport::Connection>,
}

impl WaitingRoom {
    pub fn new(song: CompleteSongData) -> Self {
        Self {
            song,
            participants: HashMap::new(),
        }
    }

    pub fn song(&self) -> &CompleteSongData {
        &self.song
    }

    pub fn participants(&self) -> &HashMap<Uuid, wtransport::Connection> {
        &self.participants
    }

    pub fn insert_participant(&mut self, participant_id: Uuid, connection: wtransport::Connection) {
        self.participants.insert(participant_id, connection);
    }

    pub fn remove_participant(&mut self, participant_id: &Uuid) -> Option<wtransport::Connection> {
        self.participants.remove(participant_id)
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateRoomRequest {
    pub song_url: String,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateRoomResponse {
    pub room_id: String,
}

/// Message sent from the client to the server over WebTransport.
#[derive(Deserialize, Serialize, Debug)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum ClientMessage {
    Join,
}

/// Message sent from the server to the client over WebTransport.
#[derive(Deserialize, Serialize, Debug)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum ServerMessage {
    Joined { participant_id: Uuid },
    Error { message: String },
}
