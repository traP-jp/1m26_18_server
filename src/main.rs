use om26_18::{
    repository::song::SongRepository, rest, rest::AppState, services::song::SongService,
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
    Io(#[from] std::io::Error),
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let database_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "mysql://root:password@127.0.0.1:3306/database".to_string());
    let pool = MySqlPoolOptions::new().connect(&database_url).await?;

    sqlx::migrate!().run(&pool).await?;

    let repo = SongRepository::new(pool);
    let song_service = SongService::new(repo);
    let state = AppState { song_service };

    rest::serve(state).await?;

    Ok(())
}
