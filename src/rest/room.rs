use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde_json::json;

use crate::domain::room::{CreateRoomRequest, CreateRoomResponse};
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
