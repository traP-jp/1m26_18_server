use sqlx::{MySqlPool, QueryBuilder};

use crate::domain::model::{Phrase, StoredSong};

#[derive(Clone)]
pub struct SongRepository {
    pool: MySqlPool,
}

impl SongRepository {
    pub fn new(pool: MySqlPool) -> Self {
        Self { pool }
    }

    pub async fn find_by_url(&self, url: &str) -> Result<Option<StoredSong>, sqlx::Error> {
        struct SongRow {
            url: String,
            title: String,
            artist: String,
        }

        let row = sqlx::query_as!(
            SongRow,
            "SELECT url, title, artist FROM songs WHERE url = ?",
            url
        )
        .fetch_optional(&self.pool)
        .await?;

        let Some(r) = row else {
            return Ok(None);
        };

        let phrases = sqlx::query_as!(
            Phrase,
            "SELECT text, starts_at_ms, ends_at_ms FROM phrases WHERE song_url = ? ORDER BY idx ASC",
            url
        )
        .fetch_all(&self.pool)
        .await?;

        Ok(Some(StoredSong {
            url: r.url,
            title: r.title,
            artist: r.artist,
            phrases,
        }))
    }

    pub async fn insert(&self, song: &StoredSong) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            "INSERT INTO songs (url, title, artist) VALUES (?, ?, ?)",
            &song.url,
            &song.title,
            &song.artist
        )
        .execute(&mut *tx)
        .await?;

        if !song.phrases.is_empty() {
            let mut qb = QueryBuilder::new(
                "INSERT INTO phrases (song_url, idx, text, starts_at_ms, ends_at_ms) ",
            );
            qb.push_values(song.phrases.iter().enumerate(), |mut b, (idx, phrase)| {
                b.push_bind(&song.url)
                    .push_bind(idx as i32)
                    .push_bind(&phrase.text)
                    .push_bind(phrase.starts_at_ms)
                    .push_bind(phrase.ends_at_ms);
            });
            qb.build().execute(&mut *tx).await?;
        }

        tx.commit().await?;
        Ok(())
    }
}
