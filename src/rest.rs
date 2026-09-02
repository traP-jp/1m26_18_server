pub mod room;
pub mod song;
pub mod webtransport;

use std::io::Result;

use axum::Router;
use tokio::net::TcpListener;
use tower_http::cors::CorsLayer;
use tower_http::trace::TraceLayer;
use utoipa::openapi::{ComponentsBuilder, Info, OpenApi, OpenApiBuilder, Server};
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

use crate::domain::model::{FetchedIncompleteSongData, FetchedSongData};
use crate::services::room::RoomService;
use crate::services::song::SongService;

#[derive(Clone)]
pub struct AppState {
    pub song_service: SongService,
    pub room_service: RoomService,
    /// SHA-256 digest (lowercase hex) of the self-signed WebTransport certificate.
    pub webtransport_cert_hash: String,
    /// UDP port the WebTransport server listens on.
    pub webtransport_port: u16,
}

const API_ROOT: &str = "/api/v1";

pub fn setup_openapi_routes() -> (Router<AppState>, OpenApi) {
    let openapi = OpenApiBuilder::new()
        .info(Info::new(
            "1-Monthon 2026 18班バックエンド",
            env!("CARGO_PKG_VERSION"),
        ))
        .servers(Some([Server::new(API_ROOT)]))
        .components(Some(
            ComponentsBuilder::new()
                .schema_from::<FetchedSongData>()
                .schema_from::<FetchedIncompleteSongData>()
                .build(),
        ))
        .build();

    OpenApiRouter::with_openapi(openapi)
        .routes(utoipa_axum::routes!(
            song::get_song_by_url,
            song::create_song
        ))
        .routes(utoipa_axum::routes!(room::create_room))
        .routes(utoipa_axum::routes!(webtransport::get_certificate_hash))
        .split_for_parts()
}

pub async fn serve(state: AppState) -> Result<()> {
    let (router, openapi) = setup_openapi_routes();
    let router = Router::new()
        .nest(API_ROOT, router)
        .merge(SwaggerUi::new("/docs/swagger-ui").url("/docs/openapi.json", openapi))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);
    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    axum::serve(listener, router).await?;

    Ok(())
}
