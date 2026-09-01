use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::model::CompleteSongData;

pub enum Room {
    Waiting(WaitingRoom),
    HostJoined(HostJoinedRoom),
}

impl Room {
    pub fn participants(&self) -> &HashMap<Uuid, wtransport::Connection> {
        match self {
            Room::Waiting(waiting) => &waiting.participants,
            Room::HostJoined(joined) => &joined.participants,
        }
    }

    pub(crate) fn participants_mut(&mut self) -> &mut HashMap<Uuid, wtransport::Connection> {
        match self {
            Room::Waiting(waiting) => &mut waiting.participants,
            Room::HostJoined(joined) => &mut joined.participants,
        }
    }
}

pub struct Host {
    id: Uuid,
    connection: wtransport::Connection,
}

impl Host {
    pub fn new(id: Uuid, connection: wtransport::Connection) -> Self {
        Self { id, connection }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn connection(&self) -> &wtransport::Connection {
        &self.connection
    }
}

pub struct WaitingRoom {
    host_token: String,
    song: CompleteSongData,
    participants: HashMap<Uuid, wtransport::Connection>,
}

impl WaitingRoom {
    pub fn new(song: CompleteSongData, host_token: String) -> Self {
        Self {
            host_token,
            song,
            participants: HashMap::new(),
        }
    }

    pub fn song(&self) -> &CompleteSongData {
        &self.song
    }

    pub fn host_token(&self) -> &str {
        &self.host_token
    }

    pub fn join_host(self, host: Host) -> HostJoinedRoom {
        HostJoinedRoom {
            host,
            song: self.song,
            participants: self.participants,
        }
    }
}

pub struct HostJoinedRoom {
    host: Host,
    song: CompleteSongData,
    participants: HashMap<Uuid, wtransport::Connection>,
}

impl HostJoinedRoom {
    pub fn host(&self) -> &Host {
        &self.host
    }

    pub fn song(&self) -> &CompleteSongData {
        &self.song
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
    pub host_token: Uuid,
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
