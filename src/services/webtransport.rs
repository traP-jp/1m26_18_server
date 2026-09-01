use std::{io, net::SocketAddr, sync::LazyLock, time::Duration};

use axum::extract::Query;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tracing::{Instrument, info, warn};
use uuid::Uuid;
use wtransport::{
    Endpoint, Identity, ServerConfig,
    endpoint::{IncomingSession, endpoint_side::Server},
    error::StreamWriteError,
    tls::{Sha256Digest, error::InvalidSan},
};

use crate::{
    domain::room::{ClientMessage, ServerMessage},
    repository::room::{InsertHostError, InsertParticipantError},
    services::room::RoomService,
};

static ROOM_ROUTER: LazyLock<matchit::Router<()>> = LazyLock::new(|| {
    let mut router = matchit::Router::new();
    // The route is statically valid; registration cannot fail.
    #[allow(clippy::expect_used)]
    router
        .insert("/rooms/{room_id}", ())
        .expect("valid route: /rooms/{room_id}");
    router
});

pub struct WebTransportServer {
    endpoint: Endpoint<Server>,
    room_service: RoomService,
}

impl WebTransportServer {
    /// Returns the SHA-256 digest of the self-signed certificate, needed by browsers as `serverCertificateHashes`.
    pub fn new(
        room_service: RoomService,
        port: u16,
    ) -> Result<(Self, Sha256Digest), WebTransportError> {
        let identity = Identity::self_signed(["localhost", "127.0.0.1"])?;

        let hash = identity.certificate_chain().as_slice()[0].hash();
        info!(port, hash = %hash, "WebTransport identity generated (use serverCertificateHashes for browser)");

        let config = ServerConfig::builder()
            .with_bind_default(port)
            .with_identity(identity)
            .keep_alive_interval(Some(Duration::from_secs(3)))
            .build();

        let endpoint = Endpoint::server(config)?;

        info!(
            port = endpoint.local_addr()?.port(),
            "WebTransport server listening"
        );

        Ok((
            Self {
                endpoint,
                room_service,
            },
            hash,
        ))
    }

    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        self.endpoint.local_addr()
    }

    pub async fn serve(self) -> io::Result<()> {
        info!("WebTransport serve loop started");
        let mut id = 0u64;
        loop {
            let incoming_session = self.endpoint.accept().await;
            let room_service = self.room_service.clone();
            let span_id = id;
            id = id.wrapping_add(1);
            tokio::spawn(
                Self::handle_incoming_session(incoming_session, room_service)
                    .instrument(tracing::info_span!("wt_session", span_id)),
            );
        }
    }

    async fn handle_incoming_session(incoming_session: IncomingSession, room_service: RoomService) {
        let session_request = match incoming_session.await {
            Ok(req) => req,
            Err(e) => {
                warn!(error = %e, "failed to receive session request");
                return;
            }
        };

        let path = session_request.path().to_string();
        let Some(route) = parse_room_path(&path) else {
            warn!(path = %path, "invalid WebTransport path, expected /rooms/:roomId");
            session_request.not_found().await;
            return;
        };
        let room_id = route.room_id;

        if let Some(token) = &route.host_token {
            if let Err(e) = room_service.validate_host_token(&room_id, token) {
                warn!(room_id = %room_id, error = %e, "host session rejected");
                match e {
                    InsertHostError::RoomNotFound => session_request.not_found().await,
                    InsertHostError::InvalidToken | InsertHostError::HostAlreadyJoined => {
                        session_request.forbidden().await
                    }
                }
                return;
            }
        } else if !room_service.exists(&room_id) {
            warn!(room_id = %room_id, "room not found");
            session_request.not_found().await;
            return;
        }

        let connection = match session_request.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                warn!(error = %e, "failed to accept session");
                return;
            }
        };

        if let Some(token) = route.host_token {
            let host_id = match room_service.join_room_as_host(&room_id, &token, connection.clone())
            {
                Ok(id) => id,
                Err(e) => {
                    warn!(room_id = %room_id, error = %e, "failed to register host after accept");
                    connection.close(
                        wtransport::VarInt::from_u32(403),
                        b"host registration failed",
                    );
                    return;
                }
            };

            info!(room_id = %room_id, host_id = %host_id, "host joined room");

            let _ = connection.closed().await;
            info!(room_id = %room_id, host_id = %host_id, "host disconnected, removing room");
            room_service.remove_room(&room_id);
            return;
        }

        info!(room_id = %room_id, "WebTransport session accepted, joining room");

        let participant_id = match room_service.join_room(&room_id, connection.clone()) {
            Ok(id) => id,
            Err(InsertParticipantError::RoomNotFound) => {
                warn!(room_id = %room_id, "room not found after accept, closing connection");
                connection.close(wtransport::VarInt::from_u32(404), b"room not found");
                return;
            }
        };

        info!(room_id = %room_id, participant_id = %participant_id, "participant joined");

        loop {
            let (send_stream, recv_stream) = match connection.accept_bi().await {
                Ok(streams) => streams,
                Err(e) => {
                    info!(room_id = %room_id, participant_id = %participant_id, error = %e, "connection closed");
                    break;
                }
            };

            tokio::spawn(async move {
                if let Err(e) = handle_bi_stream(send_stream, recv_stream, &participant_id).await {
                    warn!(error = %e, "failed to handle bi stream");
                }
            });
        }

        room_service.leave_room(&room_id, &participant_id);
    }
}

struct RoomRoute {
    room_id: String,
    host_token: Option<String>,
}

#[derive(Deserialize)]
struct RoomQuery {
    #[serde(rename = "hostToken")]
    host_token: Option<String>,
}

fn parse_room_path(path: &str) -> Option<RoomRoute> {
    // Strip the query string via http::Uri, then match the route with matchit.
    let uri = http::Uri::try_from(path).ok()?;
    let matched = ROOM_ROUTER.at(uri.path()).ok()?;
    let room_id = matched.params.get("room_id")?.to_string();
    let host_token = Query::<RoomQuery>::try_from_uri(&uri)
        .ok()
        .and_then(|query| query.0.host_token);
    Some(RoomRoute { room_id, host_token })
}

async fn handle_bi_stream(
    mut send_stream: wtransport::SendStream,
    mut recv_stream: wtransport::RecvStream,
    participant_id: &Uuid,
) -> Result<(), WebTransportError> {
    // Read the client request (capped at 8 KiB; excess is truncated).
    let mut buf = Vec::new();
    let mut limited = (&mut recv_stream).take(8192);
    limited
        .read_to_end(&mut buf)
        .await
        .map_err(|e| WebTransportError::Io(io::Error::other(e)))?;

    if buf.is_empty() {
        // Treat an empty request as a join (browser implementations differ).
        send_message(
            &mut send_stream,
            &ServerMessage::Joined {
                participant_id: *participant_id,
            },
        )
        .await?;
        return Ok(());
    }

    let msg: Result<ClientMessage, _> = serde_json::from_slice(&buf);
    let response = match msg {
        Ok(ClientMessage::Join) => ServerMessage::Joined {
            participant_id: *participant_id,
        },
        Err(e) => ServerMessage::Error {
            message: format!("invalid message: {e}"),
        },
    };

    let payload = send_message(&mut send_stream, &response).await?;

    info!(participant_id = %participant_id, response = %payload, "responded to participant");

    Ok(())
}

async fn send_message(
    send_stream: &mut wtransport::SendStream,
    msg: &ServerMessage,
) -> Result<String, WebTransportError> {
    let payload = serde_json::to_string(msg)?;
    send_stream.write_all(payload.as_bytes()).await?;
    send_stream.finish().await?;
    Ok(payload)
}

#[derive(Debug, thiserror::Error)]
pub enum WebTransportError {
    #[error("invalid SAN: {0}")]
    InvalidSan(#[from] InvalidSan),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("stream write error: {0}")]
    StreamWrite(#[from] StreamWriteError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        domain::model::CompleteSongData,
        domain::room::{Room, WaitingRoom},
        repository::{room::RoomRepository, song::SongRepository},
        services::{room::RoomService, song::SongService},
    };
    use tokio::io::AsyncReadExt;
    use tokio::time::sleep;
    use wtransport::{ClientConfig, Endpoint, endpoint::endpoint_side};

    const HOST_TOKEN: &str = "test-host-token";

    fn dummy_complete_song() -> CompleteSongData {
        serde_json::from_value(serde_json::json!({
            "artist": "artist",
            "durationMs": 1000.0,
            "beats": [{"startsAtMs": 0.0, "endsAtMs": 500.0}],
            "phrases": [],
            "segments": [{"isChorus": false, "startsAtMs": 0.0, "endsAtMs": 1000.0}],
            "title": "title"
        }))
        .expect("valid dummy song JSON")
    }

    fn setup_room_service(room_id: &str) -> (RoomRepository, RoomService) {
        let room_repo = RoomRepository::new();
        let pool =
            sqlx::MySqlPool::connect_lazy("mysql://root:password@127.0.0.1:3306/database")
                .expect("lazy pool");
        let song_repo = SongRepository::new(pool);
        let song_service = SongService::new(song_repo);
        let room_service = RoomService::new(room_repo.clone(), song_service);
        room_repo.insert(
            room_id.to_string(),
            Room::Waiting(WaitingRoom::new(
                dummy_complete_song(),
                HOST_TOKEN.to_string(),
            )),
        );
        (room_repo, room_service)
    }

    async fn start_server(room_service: RoomService) -> (u16, Sha256Digest) {
        let (server, cert_hash) =
            WebTransportServer::new(room_service, 0).expect("server creation");
        let server_port = server.local_addr().expect("local addr").port();
        tokio::spawn(async move {
            let _ = server.serve().await;
        });
        sleep(Duration::from_millis(200)).await;
        (server_port, cert_hash)
    }

    fn test_client(cert_hash: Sha256Digest) -> Endpoint<endpoint_side::Client> {
        Endpoint::client(
            ClientConfig::builder()
                .with_bind_default()
                .with_server_certificate_hashes([cert_hash])
                .build(),
        )
        .expect("client endpoint")
    }

    async fn join_as_participant(connection: &wtransport::Connection) -> Uuid {
        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .expect("open_bi")
            .await
            .expect("open_bi2");
        let join_msg =
            serde_json::to_vec(&ClientMessage::Join).expect("serialize join message");
        send.write_all(&join_msg).await.expect("write");
        send.finish().await.expect("finish");

        let mut buf = Vec::new();
        recv.read_to_end(&mut buf).await.expect("read_to_end");
        match serde_json::from_slice::<ServerMessage>(&buf).expect("valid server message") {
            ServerMessage::Joined { participant_id } => participant_id,
            ServerMessage::Error { message } => panic!("unexpected error: {message}"),
        }
    }

    #[tokio::test]
    async fn test_parse_room_path() {
        let route = parse_room_path("/rooms/1234").expect("valid route");
        assert_eq!(route.room_id, "1234");
        assert_eq!(route.host_token, None);
        let route = parse_room_path("/rooms/abcd").expect("valid route");
        assert_eq!(route.room_id, "abcd");
        assert_eq!(route.host_token, None);
        let route = parse_room_path("/rooms/1234?foo=bar").expect("valid route");
        assert_eq!(route.room_id, "1234");
        assert_eq!(route.host_token, None);
        let route = parse_room_path("/rooms/1234?hostToken=tok").expect("valid route");
        assert_eq!(route.room_id, "1234");
        assert_eq!(route.host_token.as_deref(), Some("tok"));
        let route = parse_room_path("/rooms/1234?foo=bar&hostToken=tok").expect("valid route");
        assert_eq!(route.host_token.as_deref(), Some("tok"));
        // percent-decoding via serde_urlencoded/form_urlencoded
        let route = parse_room_path("/rooms/1234?hostToken=a%20b").expect("valid route");
        assert_eq!(route.host_token.as_deref(), Some("a b"));
        let route = parse_room_path("/rooms/1234?hostToken=a%2Fb").expect("valid route");
        assert_eq!(route.host_token.as_deref(), Some("a/b"));
        assert!(parse_room_path("/rooms/").is_none());
        assert!(parse_room_path("/rooms").is_none());
        assert!(parse_room_path("/other/1234").is_none());
        assert!(parse_room_path("/rooms/1234/extra").is_none());
        assert!(parse_room_path("/rooms/1234/").is_none());
    }

    #[tokio::test]
    async fn test_webtransport_join_flow() {
        let room_id = "9999";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let connection = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect");

        let participant_id = join_as_participant(&connection).await;
        assert!(!participant_id.to_string().is_empty());

        sleep(Duration::from_millis(100)).await;
        assert_eq!(room_repo.participant_count(room_id), Some(1));
        connection.close(wtransport::VarInt::from_u32(0), b"done");
        sleep(Duration::from_millis(300)).await;
        assert_eq!(room_repo.participant_count(room_id), Some(0));
    }

    #[tokio::test]
    async fn test_webtransport_nonexistent_room_closed() {
        let room_repo = RoomRepository::new();
        let pool =
            sqlx::MySqlPool::connect_lazy("mysql://root:password@127.0.0.1:3306/database")
                .expect("lazy pool");
        let song_repo = SongRepository::new(pool);
        let song_service = SongService::new(song_repo);
        let room_service = RoomService::new(room_repo, song_service);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        // Connecting to a nonexistent room: the server rejects the session with not_found, so connect itself fails.
        let result = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/0000"))
            .await;
        assert!(
            result.is_err(),
            "expected connect to fail for nonexistent room, but it succeeded"
        );
    }

    #[tokio::test]
    async fn test_webtransport_host_join_flow() {
        let room_id = "8888";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let connection = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");

        sleep(Duration::from_millis(200)).await;
        assert!(room_repo.host_id(room_id).is_some());
        assert_eq!(room_repo.participant_count(room_id), Some(0));

        connection.close(wtransport::VarInt::from_u32(0), b"done");
        sleep(Duration::from_millis(300)).await;
        assert!(
            !room_repo.exists(room_id),
            "room should be removed after host disconnect"
        );
    }

    #[tokio::test]
    async fn test_webtransport_host_invalid_token_rejected() {
        let room_id = "7777";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let result = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken=wrong-token"
            ))
            .await;
        assert!(
            result.is_err(),
            "expected connect to fail with invalid host token"
        );
        assert!(room_repo.host_id(room_id).is_none());
        assert!(room_repo.exists(room_id));
    }

    #[tokio::test]
    async fn test_webtransport_second_host_rejected() {
        let room_id = "6666";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let first = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("first host connect");
        sleep(Duration::from_millis(200)).await;
        assert!(room_repo.host_id(room_id).is_some());

        let second = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await;
        assert!(second.is_err(), "expected second host connect to fail");
        assert!(room_repo.host_id(room_id).is_some());

        first.close(wtransport::VarInt::from_u32(0), b"done");
        sleep(Duration::from_millis(300)).await;
        assert!(!room_repo.exists(room_id));
    }

    #[tokio::test]
    async fn test_webtransport_participant_join_after_host() {
        let room_id = "5555";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let _host_connection = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;
        assert!(room_repo.host_id(room_id).is_some());

        let participant_connection = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let participant_id = join_as_participant(&participant_connection).await;
        assert!(!participant_id.to_string().is_empty());

        sleep(Duration::from_millis(100)).await;
        assert_eq!(room_repo.participant_count(room_id), Some(1));
        assert!(room_repo.host_id(room_id).is_some());

        participant_connection.close(wtransport::VarInt::from_u32(0), b"done");
        sleep(Duration::from_millis(300)).await;
        assert_eq!(room_repo.participant_count(room_id), Some(0));
        assert!(
            room_repo.exists(room_id),
            "room should survive a participant disconnect"
        );
    }
}
