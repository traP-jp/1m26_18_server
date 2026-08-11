use serde::Deserialize;
use utoipa::ToSchema;

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase", tag = "type")]
pub enum SongData {
    Complete(CompleteSongData),
    Incomplete(IncompleteSongData),
}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CompleteSongData {
    artist: String,
    duration_ms: u32,
    title: String,
}

#[derive(Deserialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct IncompleteSongData {
    duration_ms: u32,
}
