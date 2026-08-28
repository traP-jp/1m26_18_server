use crate::domain::lyrics::split_lyrics_with_songle;
use crate::domain::model::{CompleteSongData, FetchedSongData, SongData, StoredSong};
use crate::repository::song::SongRepository;
use crate::services::{songle, textalive};

#[derive(Debug, thiserror::Error)]
pub enum SongServiceError {
    #[error(transparent)]
    TextAlive(#[from] textalive::TextAliveError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    LyricsSplit(#[from] crate::domain::lyrics::LyricsSplitError),
}

#[derive(Clone)]
pub struct SongService {
    repo: SongRepository,
}

impl SongService {
    pub fn new(repo: SongRepository) -> Self {
        Self { repo }
    }

    pub async fn get_song(&self, url: &str) -> Result<SongData, SongServiceError> {
        let fetched = textalive::fetch_song_data(url).await?;
        match fetched {
            FetchedSongData::Complete(c) => Ok(SongData::Complete(c)),
            FetchedSongData::Incomplete(inc) => {
                if let Some(stored) = self.repo.find_by_url(url).await? {
                    let complete = CompleteSongData::new(
                        stored.artist,
                        inc.duration_ms,
                        inc.beats,
                        stored.phrases,
                        inc.segments,
                        stored.title,
                    );
                    Ok(SongData::Complete(complete))
                } else {
                    Ok(SongData::Incomplete(
                        crate::domain::model::IncompleteSongData::new(
                            inc.duration_ms,
                            inc.beats,
                            inc.segments,
                        ),
                    ))
                }
            }
        }
    }

    pub async fn create_song(
        &self,
        song_url: &str,
        title: String,
        artist: String,
        lyrics: String,
        lyrics_json_url: String,
    ) -> Result<CompleteSongData, CreateSongError> {
        let fetched = textalive::fetch_song_data(song_url)
            .await
            .map_err(CreateSongError::TextAlive)?;

        let inc = match fetched {
            FetchedSongData::Complete(_) => return Err(CreateSongError::AlreadyComplete),
            FetchedSongData::Incomplete(i) => i,
        };

        let songle_raw = songle::fetch_songle_lyrics(&lyrics_json_url)
            .await
            .map_err(CreateSongError::Songle)?;

        let phrases =
            split_lyrics_with_songle(&lyrics, &songle_raw).map_err(CreateSongError::LyricsSplit)?;

        let stored = StoredSong {
            url: song_url.to_string(),
            title: title.clone(),
            artist: artist.clone(),
            phrases: phrases.clone(),
        };

        self.repo.insert(&stored).await.map_err(|e| {
            if let sqlx::Error::Database(ref db) = e {
                if db.constraint() == Some("PRIMARY") || db.constraint() == Some("fk_phrases_song")
                {
                    return CreateSongError::Conflict;
                }
                let msg = db.message().to_string();
                if msg.contains("Duplicate entry") {
                    return CreateSongError::Conflict;
                }
            }
            CreateSongError::Sqlx(e)
        })?;

        let song_data = CompleteSongData::new(
            artist,
            inc.duration_ms,
            inc.beats,
            phrases,
            inc.segments,
            title,
        );

        Ok(song_data)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreateSongError {
    #[error(transparent)]
    TextAlive(#[from] textalive::TextAliveError),
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),
    #[error(transparent)]
    LyricsSplit(#[from] crate::domain::lyrics::LyricsSplitError),
    #[error(transparent)]
    Songle(#[from] songle::SongleError),
    #[error("song is already public (complete data available)")]
    AlreadyComplete,
    #[error("song already exists")]
    Conflict,
}
