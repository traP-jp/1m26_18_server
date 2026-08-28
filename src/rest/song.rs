use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::Deserialize;
use utoipa::IntoParams;

use crate::domain::model::{CompleteSongData, CreateSongRequest, SongData};
use crate::rest::AppState;
use crate::services::song::CreateSongError;

#[derive(Debug, Deserialize, IntoParams)]
pub struct GetSongByUrlQuery {
    /// [TextAlive](https://textalive.jp/songs)に登録されている曲のURL
    #[param(format = Uri)]
    url: String,
}

/// URLから曲の情報を取得します
#[utoipa::path(
    get,
    description = "TextAliveに登録されている曲のURLから曲の情報を取得します。楽曲が公開されていない場合は`/songs`で楽曲情報を登録しないと完全な情報が得られません。",
    params(GetSongByUrlQuery),
    path = "/songs",
    responses(
        (
            status = StatusCode::OK,
            body = SongData,
            description = "曲の情報を返します。楽曲が公開されていない、かつ登録もされていない場合は`IncompleteSongData`が返ります。"
        ),
        (status = StatusCode::INTERNAL_SERVER_ERROR),
    ),
    tag = "Song",
)]
pub async fn get_song_by_url(
    State(state): State<AppState>,
    Query(query): Query<GetSongByUrlQuery>,
) -> impl IntoResponse {
    match state.song_service.get_song(&query.url).await {
        Ok(data) => Json(data).into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to fetch song data: {}", e),
        )
            .into_response(),
    }
}

/// プライベートな楽曲のメタデータと歌詞を登録します
#[utoipa::path(
    post,
    path = "/songs",
    request_body = CreateSongRequest,
    responses(
        (status = StatusCode::CREATED, body = CompleteSongData, description = "登録に成功し、補完された曲情報を返します。"),
        (status = StatusCode::BAD_REQUEST, description = "歌詞の文字数が一致しない、または既に公開されている曲です。"),
        (status = StatusCode::CONFLICT, description = "既に登録済みの曲です。"),
        (status = StatusCode::INTERNAL_SERVER_ERROR),
    ),
    tag = "Song",
)]
pub async fn create_song(
    State(state): State<AppState>,
    Json(body): Json<CreateSongRequest>,
) -> impl IntoResponse {
    match state
        .song_service
        .create_song(
            &body.song_url,
            body.title,
            body.artist,
            body.lyrics,
            body.lyrics_json_url,
        )
        .await
    {
        Ok(data) => (StatusCode::CREATED, Json(data)).into_response(),
        Err(CreateSongError::AlreadyComplete) => (
            StatusCode::BAD_REQUEST,
            "song is already public".to_string(),
        )
            .into_response(),
        Err(CreateSongError::LyricsSplit(e)) => {
            (StatusCode::BAD_REQUEST, e.to_string()).into_response()
        }
        Err(CreateSongError::Songle(e)) => (StatusCode::BAD_REQUEST, e.to_string()).into_response(),
        Err(CreateSongError::Conflict) => {
            (StatusCode::CONFLICT, "song already exists".to_string()).into_response()
        }
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("failed to create song: {}", e),
        )
            .into_response(),
    }
}
