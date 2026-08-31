use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::domain::model::CompleteSongData;

pub enum Room {
    Waiting(WaitingRoom),
}

pub struct WaitingRoom {
    song: CompleteSongData,
}

impl WaitingRoom {
    pub fn new(song: CompleteSongData) -> Self {
        Self { song }
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
}
