use std::{env, io};

use om26_18::{
    repository::{room::RoomRepository, song::SongRepository},
    rest,
    rest::AppState,
    services::{
        room::RoomService,
        song::SongService,
        webtransport::{WebTransportError, WebTransportServer, certificate_hash_hex},
    },
};
use sqlx::{migrate::MigrateError, mysql::MySqlPoolOptions};
use tracing_subscriber::EnvFilter;

#[derive(Debug, thiserror::Error)]
enum AppError {
    #[error("connecting to the database: {0}")]
    Connection(#[from] sqlx::Error),

    #[error(transparent)]
    Migration(#[from] MigrateError),

    #[error(transparent)]
    Io(#[from] io::Error),

    #[error(transparent)]
    WebTransport(#[from] WebTransportError),
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let database_url = env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@127.0.0.1:3306/database".to_string());
    let pool = MySqlPoolOptions::new().connect(&database_url).await?;

    sqlx::migrate!().run(&pool).await?;

    let repo = SongRepository::new(pool);
    let song_service = SongService::new(repo);
    let room_repo = RoomRepository::new();
    let room_service = RoomService::new(room_repo, song_service.clone());

    let wt_port: u16 = env::var("WEBTRANSPORT_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(4433);
    let (wt_server, cert_hash) = WebTransportServer::new(room_service.clone(), wt_port)?;
    let wt_local_port = wt_server.local_addr()?.port();

    let state = AppState {
        song_service,
        room_service,
        webtransport_cert_hash: certificate_hash_hex(&cert_hash),
        webtransport_port: wt_local_port,
    };

    tokio::spawn(async move {
        if let Err(e) = wt_server.serve().await {
            tracing::error!(error = %e, "WebTransport server error");
        }
    });

    rest::serve(state).await?;

    Ok(())
}
