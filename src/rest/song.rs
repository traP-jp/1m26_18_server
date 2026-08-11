use axum::{Json, extract::Query, http::StatusCode, response::IntoResponse};
use serde::Deserialize;
use utoipa::IntoParams;

use crate::{domain::model::SongData, services::textalive};

#[derive(Debug, Deserialize, IntoParams)]
pub struct GetSongByPublicUrlQuery {
    /// [TextAlive](https://textalive.jp/songs)に登録されている曲のURL
    #[param(format = Uri)]
    url: String,
}

/// URLから公開された曲の情報を取得します
#[utoipa::path(
    get,
    params(GetSongByPublicUrlQuery),
    path = "/songs",
    responses(
        (
            status = StatusCode::OK,
            body = SongData,
            description = "曲の情報を返します。楽曲が公開されていない場合は完全な情報が得られません。",
        ),
        (status = StatusCode::INTERNAL_SERVER_ERROR),
    ),
    tag = "Song",
)]
pub async fn get_song_by_public_url(query: Query<GetSongByPublicUrlQuery>) -> impl IntoResponse {
    let song_data = match textalive::fetch_song_data(&query.url).await {
        Ok(data) => data,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("failed to fetch song data: {}", e),
            )
                .into_response();
        }
    };

    Json(song_data).into_response()
}
