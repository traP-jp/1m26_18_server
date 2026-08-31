use rand::RngExt;

use crate::domain::model::SongData;
use crate::domain::room::{Room, WaitingRoom};
use crate::repository::room::RoomRepository;
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

    pub async fn create_room(&self, song_url: &str) -> Result<String, CreateRoomError> {
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
            let room = Room::Waiting(WaitingRoom::new(complete));
            self.repo.insert(room_id.clone(), room);
            return Ok(room_id);
        }

        Err(CreateRoomError::RoomIdExhausted)
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
