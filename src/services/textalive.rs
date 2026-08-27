use std::{env, fs, io};

use tokio::process::Command;
use uuid::Uuid;

use crate::domain::model::SongData;

#[derive(Debug, thiserror::Error)]
pub enum TextAliveError {
    #[error(transparent)]
    Io(#[from] io::Error),

    #[error("fetching song data: {0}")]
    FetchSong(String),

    #[error("parsing song data: {0}")]
    Parse(#[from] serde_json::Error),

    #[error("temporary file path could not be converted to str")]
    TempFilePath,
}

#[tracing::instrument]
pub async fn fetch_song_data(url: &str) -> Result<SongData, TextAliveError> {
    let tmp_file_path = env::temp_dir().join(format!("{}.json", Uuid::now_v7()));
    let tmp_file_path_str = tmp_file_path
        .to_str()
        .ok_or_else(|| TextAliveError::TempFilePath)?;
    let output = Command::new("deno")
        .arg("run")
        .arg("--allow-env")
        .arg("--allow-net=api.textalive.jp,songle.jp")
        .arg(format!("--allow-write={}", tmp_file_path_str))
        .arg("dist/textalive.js")
        .arg(url)
        .arg(tmp_file_path_str)
        .output()
        .await?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);

        return Err(TextAliveError::FetchSong(stderr.to_string()));
    }

    let json_str = fs::read_to_string(tmp_file_path_str)?;
    let song_data: SongData = serde_json::from_str(&json_str)?;

    fs::remove_file(tmp_file_path_str)?;

    Ok(song_data)
}
