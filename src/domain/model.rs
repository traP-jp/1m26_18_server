use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SongData {
    Complete(CompleteSongData),
    Incomplete(IncompleteSongData),
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CompleteSongData {
    artist: String,
    duration_ms: f32,
    beats: Vec<Beat>,
    segments: Vec<Segment>,
    title: String,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct IncompleteSongData {
    duration_ms: f32,
    beats: Vec<Beat>,
    segments: Vec<Segment>,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Beat {
    starts_at_ms: f32,
    ends_at_ms: f32,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Segment {
    is_chorus: bool,
    starts_at_ms: f32,
    ends_at_ms: f32,
}
