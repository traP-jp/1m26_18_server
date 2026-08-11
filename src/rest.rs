pub mod song;

use std::io::Result;

use axum::Router;
use tokio::net::TcpListener;
use tower_http::trace::TraceLayer;
use utoipa::openapi::{Info, OpenApi, OpenApiBuilder, Server};
use utoipa_axum::router::OpenApiRouter;
use utoipa_swagger_ui::SwaggerUi;

const API_ROOT: &str = "/api/v1";

pub fn setup_openapi_routes() -> (Router, OpenApi) {
    let openapi = OpenApiBuilder::new()
        .info(Info::new(
            "1-Monthon 2026 18班バックエンド",
            env!("CARGO_PKG_VERSION"),
        ))
        .servers(Some([Server::new(API_ROOT)]))
        .build();

    OpenApiRouter::with_openapi(openapi)
        .routes(utoipa_axum::routes!(song::get_song_by_public_url))
        .split_for_parts()
}

pub async fn serve() -> Result<()> {
    let (router, openapi) = setup_openapi_routes();
    let router = Router::new()
        .nest(API_ROOT, router)
        .merge(SwaggerUi::new("/docs/swagger-ui").url("/docs/openapi.json", openapi))
        .layer(TraceLayer::new_for_http());
    let listener = TcpListener::bind("0.0.0.0:8080").await?;

    axum::serve(listener, router).await?;

    Ok(())
}
