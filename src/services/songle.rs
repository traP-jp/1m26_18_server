use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct SongleChar {
    pub start_time: f32,
    pub end_time: f32,
}

#[derive(Debug, Deserialize)]
pub struct SongleLyricsRaw {
    pub data: Vec<Vec<Vec<SongleChar>>>,
}

#[derive(Debug, thiserror::Error)]
pub enum SongleError {
    #[error("invalid lyricsJsonUrl: {0}")]
    InvalidUrl(String),
    #[error("fetching songle lyrics: {0}")]
    Fetch(String),
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
}

pub async fn fetch_songle_lyrics(url: &str) -> Result<SongleLyricsRaw, SongleError> {
    let parsed = reqwest::Url::parse(url).map_err(|e| SongleError::InvalidUrl(e.to_string()))?;
    let host = parsed.host_str().unwrap_or("");

    if host != "songle.jp" && !host.ends_with(".songle.jp") {
        return Err(SongleError::InvalidUrl(
            "lyricsJsonUrl must be on songle.jp".to_string(),
        ));
    }

    let resp = reqwest::get(url).await.map_err(SongleError::Reqwest)?;

    if !resp.status().is_success() {
        return Err(SongleError::Fetch(format!("status {}", resp.status())));
    }

    let raw: SongleLyricsRaw = resp.json().await.map_err(SongleError::Reqwest)?;

    Ok(raw)
}
