use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SongData {
    Complete(CompleteSongData),
    Incomplete(IncompleteSongData),
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CompleteSongData {
    artist: String,
    duration_ms: f32,
    beats: Vec<Beat>,
    phrases: Vec<Phrase>,
    segments: Vec<Segment>,
    title: String,
}

impl CompleteSongData {
    pub fn new(
        artist: String,
        duration_ms: f32,
        beats: Vec<Beat>,
        phrases: Vec<Phrase>,
        segments: Vec<Segment>,
        title: String,
    ) -> Self {
        Self {
            artist,
            duration_ms,
            beats,
            phrases,
            segments,
            title,
        }
    }

    /// The song's beats (each one's start/end position in milliseconds).
    pub fn beats(&self) -> &Vec<Beat> {
        &self.beats
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct IncompleteSongData {
    duration_ms: f32,
    beats: Vec<Beat>,
    segments: Vec<Segment>,
}

impl IncompleteSongData {
    pub fn new(duration_ms: f32, beats: Vec<Beat>, segments: Vec<Segment>) -> Self {
        Self {
            duration_ms,
            beats,
            segments,
        }
    }

    pub fn duration_ms(&self) -> f32 {
        self.duration_ms
    }
    pub fn beats(&self) -> &Vec<Beat> {
        &self.beats
    }
    pub fn segments(&self) -> &Vec<Segment> {
        &self.segments
    }
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Beat {
    starts_at_ms: f32,
    ends_at_ms: f32,
}

impl Beat {
    /// The beat's start position in the song, in milliseconds.
    pub fn starts_at_ms(&self) -> f32 {
        self.starts_at_ms
    }
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Phrase {
    pub text: String,
    pub starts_at_ms: f32,
    pub ends_at_ms: f32,
}

impl Phrase {
    pub fn new(text: String, starts_at_ms: f32, ends_at_ms: f32) -> Self {
        Self {
            text,
            starts_at_ms,
            ends_at_ms,
        }
    }
}

#[derive(Clone, Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    is_chorus: bool,
    starts_at_ms: f32,
    ends_at_ms: f32,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum FetchedSongData {
    Complete(CompleteSongData),
    Incomplete(FetchedIncompleteSongData),
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct FetchedIncompleteSongData {
    pub duration_ms: f32,
    pub beats: Vec<Beat>,
    pub segments: Vec<Segment>,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateSongRequest {
    pub song_url: String,
    pub title: String,
    pub artist: String,
    pub lyrics: String,
    pub lyrics_json_url: String,
}

#[derive(Deserialize, Serialize)]
pub struct StoredSong {
    pub url: String,
    pub title: String,
    pub artist: String,
    pub phrases: Vec<Phrase>,
}
