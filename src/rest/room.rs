use axum::{
    Json,
    extract::{Path, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde_json::json;

use crate::domain::room::{CreateRoomRequest, CreateRoomResponse, GetRoomResponse};
use crate::rest::AppState;
use crate::services::room::CreateRoomError;

/// 部屋を作成します
#[utoipa::path(
    post,
    path = "/rooms",
    request_body = CreateRoomRequest,
    responses(
        (status = StatusCode::CREATED, body = CreateRoomResponse, description = "部屋の作成に成功し、部屋IDを返します。"),
        (status = StatusCode::BAD_REQUEST, description = "曲データが不完全です。"),
        (status = StatusCode::INTERNAL_SERVER_ERROR),
    ),
    tag = "Room",
)]
pub async fn create_room(
    State(state): State<AppState>,
    Json(body): Json<CreateRoomRequest>,
) -> impl IntoResponse {
    match state.room_service.create_room(&body.song_url).await {
        Ok((room_id, host_token)) => (
            StatusCode::CREATED,
            Json(CreateRoomResponse {
                room_id,
                host_token,
            }),
        )
            .into_response(),
        Err(CreateRoomError::SongNotComplete) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "song data is incomplete"})),
        )
            .into_response(),
        Err(CreateRoomError::RoomIdExhausted) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": "failed to generate unique room id"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({"error": format!("failed to create room: {}", e)})),
        )
            .into_response(),
    }
}

/// 部屋の楽曲情報を取得します
#[utoipa::path(
    get,
    path = "/rooms/{room_id}",
    params(
        ("room_id" = String, Path, description = "4桁の部屋ID"),
    ),
    responses(
        (status = StatusCode::OK, body = GetRoomResponse, description = "部屋の楽曲情報を返します。"),
        (status = StatusCode::NOT_FOUND, description = "部屋が存在しません。"),
    ),
    tag = "Room",
)]
pub async fn get_room(
    State(state): State<AppState>,
    Path(room_id): Path<String>,
) -> impl IntoResponse {
    match state.room_service.get_room_song(&room_id) {
        Some(song) => (StatusCode::OK, Json(GetRoomResponse { song })).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "room not found"})),
        )
            .into_response(),
    }
}
