use std::{io, net::SocketAddr, sync::LazyLock, time::Duration};

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
    repository::room::InsertParticipantError,
    services::room::RoomService,
};

static ROOM_ROUTER: LazyLock<matchit::Router<()>> = LazyLock::new(|| {
    let mut router = matchit::Router::new();
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
        let Some(room_id) = parse_room_path(&path) else {
            warn!(path = %path, "invalid WebTransport path, expected /rooms/:roomId");
            session_request.not_found().await;
            return;
        };

        if !room_service.exists(&room_id) {
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

fn parse_room_path(path: &str) -> Option<String> {
    // Strip the query string via http::Uri, then match the route with matchit.
    let uri = http::Uri::try_from(path).ok()?;
    let matched = ROOM_ROUTER.at(uri.path()).ok()?;
    Some(matched.params.get("room_id")?.to_string())
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

    send_message(&mut send_stream, &response).await?;

    info!(participant_id = %participant_id, response = %String::from_utf8_lossy(&serde_json::to_vec(&response).unwrap()), "responded to participant");

    Ok(())
}

async fn send_message(
    send_stream: &mut wtransport::SendStream,
    msg: &ServerMessage,
) -> Result<(), WebTransportError> {
    let payload = serde_json::to_vec(msg).unwrap();
    send_stream.write_all(&payload).await?;
    send_stream.finish().await?;
    Ok(())
}

#[derive(Debug, thiserror::Error)]
pub enum WebTransportError {
    #[error("invalid SAN: {0}")]
    InvalidSan(#[from] InvalidSan),
    #[error("IO error: {0}")]
    Io(#[from] io::Error),
    #[error("stream write error: {0}")]
    StreamWrite(#[from] StreamWriteError),
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
    use wtransport::{ClientConfig, Endpoint};

    fn dummy_complete_song() -> CompleteSongData {
        serde_json::from_value(serde_json::json!({
            "artist": "artist",
            "durationMs": 1000.0,
            "beats": [{"startsAtMs": 0.0, "endsAtMs": 500.0}],
            "phrases": [],
            "segments": [{"isChorus": false, "startsAtMs": 0.0, "endsAtMs": 1000.0}],
            "title": "title"
        }))
        .unwrap()
    }

    fn setup_room_service(room_id: &str) -> (RoomRepository, RoomService) {
        let room_repo = RoomRepository::new();
        let pool =
            sqlx::MySqlPool::connect_lazy("mysql://root:password@127.0.0.1:3306/database").unwrap();
        let song_repo = SongRepository::new(pool);
        let song_service = SongService::new(song_repo);
        let room_service = RoomService::new(room_repo.clone(), song_service);
        room_repo.insert(
            room_id.to_string(),
            Room::Waiting(WaitingRoom::new(dummy_complete_song())),
        );
        (room_repo, room_service)
    }

    #[tokio::test]
    async fn test_parse_room_path() {
        assert_eq!(parse_room_path("/rooms/1234"), Some("1234".to_string()));
        assert_eq!(parse_room_path("/rooms/abcd"), Some("abcd".to_string()));
        assert_eq!(
            parse_room_path("/rooms/1234?foo=bar"),
            Some("1234".to_string())
        );
        assert_eq!(parse_room_path("/rooms/"), None);
        assert_eq!(parse_room_path("/rooms"), None);
        assert_eq!(parse_room_path("/other/1234"), None);
        assert_eq!(parse_room_path("/rooms/1234/extra"), None);
        assert_eq!(parse_room_path("/rooms/1234/"), None);
    }

    #[tokio::test]
    async fn test_webtransport_join_flow() {
        let room_id = "9999";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server, cert_hash) =
            WebTransportServer::new(room_service.clone(), 0).expect("server creation");
        let server_port = server.local_addr().expect("local addr").port();

        tokio::spawn(async move {
            let _ = server.serve().await;
        });

        tokio::time::sleep(Duration::from_millis(200)).await;

        let client_config = ClientConfig::builder()
            .with_bind_default()
            .with_server_certificate_hashes([cert_hash])
            .build();
        let client_endpoint = Endpoint::client(client_config).expect("client endpoint");
        let connection = client_endpoint
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect");

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .expect("open_bi")
            .await
            .expect("open_bi2");
        let join_msg = serde_json::to_vec(&ClientMessage::Join).unwrap();
        send.write_all(&join_msg).await.expect("write");
        send.finish().await.expect("finish");

        let mut buf = Vec::new();
        recv.read_to_end(&mut buf).await.expect("read_to_end");
        let server_msg: ServerMessage = serde_json::from_slice(&buf).expect("valid server message");

        match server_msg {
            ServerMessage::Joined { participant_id } => {
                assert!(!participant_id.to_string().is_empty());
                tokio::time::sleep(Duration::from_millis(100)).await;
                assert_eq!(room_repo.participant_count(room_id), Some(1));
                connection.close(wtransport::VarInt::from_u32(0), b"done");
                tokio::time::sleep(Duration::from_millis(300)).await;
                assert_eq!(room_repo.participant_count(room_id), Some(0));
            }
            ServerMessage::Error { message } => panic!("unexpected error: {message}"),
        }
    }

    #[tokio::test]
    async fn test_webtransport_nonexistent_room_closed() {
        let room_repo = RoomRepository::new();
        let pool =
            sqlx::MySqlPool::connect_lazy("mysql://root:password@127.0.0.1:3306/database").unwrap();
        let song_repo = SongRepository::new(pool);
        let song_service = SongService::new(song_repo);
        let room_service = RoomService::new(room_repo, song_service);

        let (server, cert_hash) =
            WebTransportServer::new(room_service, 0).expect("server creation");
        let server_port = server.local_addr().unwrap().port();
        tokio::spawn(async move {
            let _ = server.serve().await;
        });
        tokio::time::sleep(Duration::from_millis(200)).await;

        let client_config = ClientConfig::builder()
            .with_bind_default()
            .with_server_certificate_hashes([cert_hash])
            .build();
        let client_endpoint = Endpoint::client(client_config).unwrap();
        // Connecting to a nonexistent room: the server rejects the session with not_found, so connect itself fails.
        let result = client_endpoint
            .connect(format!("https://127.0.0.1:{server_port}/rooms/0000"))
            .await;
        assert!(
            result.is_err(),
            "expected connect to fail for nonexistent room, but it succeeded"
        );
    }
}
