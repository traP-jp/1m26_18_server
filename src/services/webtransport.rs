use std::{
    io,
    net::SocketAddr,
    sync::LazyLock,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::extract::Query;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tracing::{Instrument, debug, info, warn};
use uuid::Uuid;
use wtransport::{
    Endpoint, Identity, ServerConfig,
    endpoint::{IncomingSession, endpoint_side::Server},
    error::{ConnectionError, StreamOpeningError, StreamWriteError},
    tls::{Sha256Digest, error::InvalidSan},
};

use crate::{
    domain::{
        room::{CALIBRATION_SOUND_COUNT, ClientMessage, ServerMessage},
        wire::{Encode, EncodeError, decode_exact},
    },
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

/// Formats a certificate SHA-256 digest as a lowercase hex string (64 chars),
/// ready to be decoded by clients into the raw 32 bytes required by the
/// browser's `serverCertificateHashes`.
pub fn certificate_hash_hex(digest: &Sha256Digest) -> String {
    digest
        .as_ref()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

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
        } else if !room_service.host_joined(&room_id) {
            warn!(room_id = %room_id, "participant session rejected: host has not joined");
            session_request.forbidden().await;
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

            handle_connection_streams(&room_service, &room_id, host_id, true, &connection).await;

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
            Err(InsertParticipantError::HostNotJoined) => {
                warn!(room_id = %room_id, "host has not joined after accept, closing connection");
                connection.close(wtransport::VarInt::from_u32(403), b"host has not joined");
                return;
            }
        };

        info!(room_id = %room_id, participant_id = %participant_id, "participant joined");

        // Fire-and-forget: the join flow must not block on (or fail with) the
        // host notification.
        tokio::spawn(notify_host(
            room_service.clone(),
            room_id.clone(),
            ServerMessage::ParticipantJoined { participant_id },
        ));

        handle_connection_streams(&room_service, &room_id, participant_id, false, &connection)
            .await;

        room_service.leave_room(&room_id, &participant_id);
    }
}

/// Pushes a server-initiated event to the room host on a bidirectional
/// stream.
///
/// Fire-and-forget: a missing host (e.g. the room was removed concurrently)
/// skips the notification, and any transport failure is only logged.
async fn notify_host(room_service: RoomService, room_id: String, message: ServerMessage) {
    let Some(connection) = room_service.host_connection(&room_id) else {
        debug!(room_id = %room_id, "host has not joined yet, skipping host notification");
        return;
    };

    let result = async {
        let (mut send_stream, _recv_stream) = connection.open_bi().await?.await?;
        send_message(&mut send_stream, &message).await
    }
    .await;

    match result {
        Ok(()) => {
            info!(room_id = %room_id, message = ?message, "notified host");
        }
        Err(e) => {
            warn!(room_id = %room_id, message = ?message, error = %e, "failed to notify host");
        }
    }
}

/// Accepts bidirectional streams until the connection closes, handling each
/// request in its own task. Shared by participants and the host.
async fn handle_connection_streams(
    room_service: &RoomService,
    room_id: &str,
    client_id: Uuid,
    is_host: bool,
    connection: &wtransport::Connection,
) {
    loop {
        let (send_stream, recv_stream) = match connection.accept_bi().await {
            Ok(streams) => streams,
            Err(e) => {
                info!(room_id = %room_id, client_id = %client_id, error = %e, "connection closed");
                break;
            }
        };

        // The spawned task needs 'static data, so pass owned clones.
        let room_service = room_service.clone();
        let room_id = room_id.to_string();
        tokio::spawn(async move {
            if let Err(e) = handle_bi_stream(
                &room_service,
                &room_id,
                client_id,
                is_host,
                send_stream,
                recv_stream,
            )
            .await
            {
                warn!(error = %e, "failed to handle bi stream");
            }
        });
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
    Some(RoomRoute {
        room_id,
        host_token,
    })
}

async fn handle_bi_stream(
    room_service: &RoomService,
    room_id: &str,
    client_id: Uuid,
    is_host: bool,
    mut send_stream: wtransport::SendStream,
    mut recv_stream: wtransport::RecvStream,
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
        let response = ServerMessage::Joined {
            participant_id: client_id,
        };
        send_message(&mut send_stream, &response).await?;
        return Ok(());
    }

    let response = match decode_exact::<ClientMessage>(&buf) {
        Ok(ClientMessage::Join) => Some(ServerMessage::Joined {
            participant_id: client_id,
        }),
        Ok(ClientMessage::TimeSyncRequest) => {
            // Stateless NTP-like exchange: t1 is taken as soon as the request
            // has been received and t2 right before the response is written.
            let t1 = unix_micros();
            let t2 = unix_micros();
            Some(ServerMessage::TimeSyncResponse { t1, t2 })
        }
        Ok(ClientMessage::CalibrationStart { times }) => {
            // Fire-and-forget: the server stores the announced sound times and
            // never responds.
            handle_calibration_start(room_service, room_id, client_id, is_host, times);
            None
        }
        Ok(ClientMessage::CalibrationDetect {
            sound_index,
            detected_at,
        }) => {
            // Fire-and-forget: the server matches the detection and stores the
            // per-participant lag; it is never reported back to the client.
            handle_calibration_detect(
                room_service,
                room_id,
                client_id,
                is_host,
                sound_index,
                detected_at,
            );
            None
        }
        Ok(ClientMessage::Ready) => {
            // Fire-and-forget: the server records the participant's readiness
            // and never responds; the host is notified on the first report.
            handle_ready(room_service, room_id, client_id, is_host);
            None
        }
        Ok(ClientMessage::Stamp { stamp_id }) => {
            // Fire-and-forget: the server relays the stamp to the host and
            // never responds.
            handle_stamp(room_service, room_id, client_id, is_host, stamp_id);
            None
        }
        Err(e) => {
            // Unknown event IDs and malformed messages are ignored silently
            // (no response is written) so that new client events can be
            // rolled out independently.
            warn!(participant_id = %client_id, error = %e, "ignoring undecodable client message");
            None
        }
    };

    match response {
        Some(response) => {
            send_message(&mut send_stream, &response).await?;
            info!(
                participant_id = %client_id,
                response = ?response,
                "responded to participant"
            );
        }
        None => send_stream.finish().await?,
    }

    Ok(())
}

fn handle_calibration_start(
    room_service: &RoomService,
    room_id: &str,
    client_id: Uuid,
    is_host: bool,
    times: [u64; CALIBRATION_SOUND_COUNT],
) {
    if !is_host {
        warn!(
            room_id = %room_id,
            participant_id = %client_id,
            "ignoring calibration start from a non-host client"
        );
        return;
    }
    if let Err(e) = room_service.start_calibration(room_id, times) {
        warn!(room_id = %room_id, error = %e, "failed to start calibration");
    }
}

/// Records a participant's ready report and, on the first transition,
/// notifies the host on a server-initiated bidirectional stream.
fn handle_ready(room_service: &RoomService, room_id: &str, participant_id: Uuid, is_host: bool) {
    if is_host {
        warn!(
            room_id = %room_id,
            host_id = %participant_id,
            "ignoring ready report from the host"
        );
        return;
    }
    match room_service.set_ready(room_id, &participant_id) {
        Ok(true) => {
            // Fire-and-forget: the report flow must not block on (or fail
            // with) the host notification.
            tokio::spawn(notify_host(
                room_service.clone(),
                room_id.to_string(),
                ServerMessage::ParticipantReady { participant_id },
            ));
        }
        Ok(false) => {}
        Err(e) => {
            warn!(
                room_id = %room_id,
                participant_id = %participant_id,
                error = %e,
                "failed to record participant ready"
            );
        }
    }
}

/// Relays a participant's stamp to the host on a server-initiated
/// bidirectional stream. The stamp id is passed through uninterpreted.
fn handle_stamp(
    room_service: &RoomService,
    room_id: &str,
    participant_id: Uuid,
    is_host: bool,
    stamp_id: u8,
) {
    if is_host {
        warn!(
            room_id = %room_id,
            host_id = %participant_id,
            "ignoring stamp from the host"
        );
        return;
    }
    tracing::debug!(
        room_id = %room_id,
        participant_id = %participant_id,
        stamp_id,
        "participant sent a stamp"
    );
    // Fire-and-forget: the relay flow must not block on (or fail with) the
    // host notification.
    tokio::spawn(notify_host(
        room_service.clone(),
        room_id.to_string(),
        ServerMessage::ParticipantStamp {
            participant_id,
            stamp_id,
        },
    ));
}

fn handle_calibration_detect(
    room_service: &RoomService,
    room_id: &str,
    participant_id: Uuid,
    is_host: bool,
    sound_index: usize,
    detected_at: u64,
) {
    if is_host {
        warn!(
            room_id = %room_id,
            host_id = %participant_id,
            "ignoring calibration sound detection from the host"
        );
        return;
    }
    if let Err(e) =
        room_service.record_detection(room_id, &participant_id, sound_index, detected_at)
    {
        warn!(
            room_id = %room_id,
            participant_id = %participant_id,
            error = %e,
            "failed to record calibration sound detection"
        );
    }
}

/// Current wall-clock time in microseconds since the Unix epoch.
fn unix_micros() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|elapsed| u64::try_from(elapsed.as_micros()).unwrap_or(u64::MAX))
        .unwrap_or(0)
}

async fn send_message(
    send_stream: &mut wtransport::SendStream,
    msg: &ServerMessage,
) -> Result<(), WebTransportError> {
    let mut payload = Vec::new();
    msg.encode(&mut payload)?;
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
    #[error("connection error: {0}")]
    Connection(#[from] ConnectionError),
    #[error("stream opening error: {0}")]
    StreamOpening(#[from] StreamOpeningError),
    #[error("stream write error: {0}")]
    StreamWrite(#[from] StreamWriteError),
    #[error("encoding error: {0}")]
    Encoding(#[from] EncodeError),
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
    use tokio::time::{sleep, timeout};
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
        let pool = sqlx::MySqlPool::connect_lazy("mysql://root:password@127.0.0.1:3306/database")
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
        let mut join_msg = Vec::new();
        ClientMessage::Join
            .encode(&mut join_msg)
            .expect("encode join message");
        send.write_all(&join_msg).await.expect("write");
        send.finish().await.expect("finish");

        let mut buf = Vec::new();
        recv.read_to_end(&mut buf).await.expect("read_to_end");
        match decode_exact::<ServerMessage>(&buf).expect("valid server message") {
            ServerMessage::Joined { participant_id } => participant_id,
            ServerMessage::TimeSyncResponse { .. } => panic!("unexpected time sync response"),
            ServerMessage::Error { message } => panic!("unexpected error: {message}"),
            ServerMessage::ParticipantJoined { .. } => {
                panic!("unexpected participant joined notification")
            }
            ServerMessage::ParticipantReady { .. } => {
                panic!("unexpected participant ready notification")
            }
            ServerMessage::ParticipantStamp { .. } => {
                panic!("unexpected participant stamp notification")
            }
        }
    }

    /// Sends one client message and returns the server's (possibly empty) response.
    async fn send_client_message(
        connection: &wtransport::Connection,
        message: &ClientMessage,
    ) -> Vec<u8> {
        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .expect("open_bi")
            .await
            .expect("open_bi2");
        let mut request = Vec::new();
        message.encode(&mut request).expect("encode message");
        send.write_all(&request).await.expect("write");
        send.finish().await.expect("finish");

        let mut response = Vec::new();
        recv.read_to_end(&mut response).await.expect("read_to_end");
        response
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
    async fn test_webtransport_participant_rejected_before_host_join() {
        let room_id = "9999";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        // The host has not joined yet: the participant session must be rejected.
        let result = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await;
        assert!(
            result.is_err(),
            "expected connect to fail while the host has not joined"
        );
        assert!(room_repo.exists(room_id));
        assert!(room_repo.host_id(room_id).is_none());
    }

    #[tokio::test]
    async fn test_webtransport_time_sync() {
        let room_id = "1111";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;

        let connection = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect");

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .expect("open_bi")
            .await
            .expect("open_bi2");
        let mut request = Vec::new();
        ClientMessage::TimeSyncRequest
            .encode(&mut request)
            .expect("encode time sync request");
        send.write_all(&request).await.expect("write");
        send.finish().await.expect("finish");

        let mut buf = Vec::new();
        recv.read_to_end(&mut buf).await.expect("read_to_end");
        let t3 = unix_micros();
        match decode_exact::<ServerMessage>(&buf).expect("valid server message") {
            ServerMessage::TimeSyncResponse { t1, t2 } => {
                assert!(t1 <= t2, "t1 must not exceed t2");
                assert!(
                    t2.saturating_sub(t1) < 5_000,
                    "server dwell between t1 and t2 must be negligible ({} µs)",
                    t2.saturating_sub(t1)
                );
                assert!(
                    t1.abs_diff(t3) < 10_000_000,
                    "t1 must be close to the local wall clock (diff = {} µs)",
                    t1.abs_diff(t3)
                );
            }
            other => panic!("unexpected message: {other:?}"),
        }

        connection.close(wtransport::VarInt::from_u32(0), b"done");
        sleep(Duration::from_millis(300)).await;
        assert_eq!(room_repo.participant_count(room_id), Some(0));
        host.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_calibration_flow() {
        let room_id = "3131";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;
        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let participant_id = join_as_participant(&participant).await;

        let host_times = [
            1_700_000_000_000_000,
            1_700_000_001_000_000,
            1_700_000_002_000_000,
        ];
        let response = send_client_message(
            &host,
            &ClientMessage::CalibrationStart { times: host_times },
        )
        .await;
        assert!(
            response.is_empty(),
            "calibration start must not get a response"
        );

        // Detections are reported in a shuffled order (clients distinguish the
        // sounds by frequency), each lagging its sound by a different amount;
        // the lag is the median difference (60_000 µs).
        let detections = [
            (2, 1_700_000_002_000_000 + 70_000),
            (0, 1_700_000_000_000_000 + 50_000),
            (1, 1_700_000_001_000_000 + 60_000),
        ];
        for (sound_index, detected_at) in detections {
            let response = send_client_message(
                &participant,
                &ClientMessage::CalibrationDetect {
                    sound_index,
                    detected_at,
                },
            )
            .await;
            assert!(
                response.is_empty(),
                "calibration detect must not get a response"
            );
        }

        assert_eq!(
            room_repo.participant_lag(room_id, &participant_id),
            Some(60_000)
        );

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_calibration_detect_before_start_ignored() {
        let room_id = "3232";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;
        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let participant_id = join_as_participant(&participant).await;

        // A detection with no calibration in progress is silently ignored.
        let response = send_client_message(
            &participant,
            &ClientMessage::CalibrationDetect {
                sound_index: 0,
                detected_at: 1_700_000_000_050_000,
            },
        )
        .await;
        assert!(response.is_empty());
        assert_eq!(room_repo.participant_lag(room_id, &participant_id), None);

        // The flow still works once the host has started a calibration round.
        let host_times = [
            1_700_000_000_000_000,
            1_700_000_001_000_000,
            1_700_000_002_000_000,
        ];
        send_client_message(
            &host,
            &ClientMessage::CalibrationStart { times: host_times },
        )
        .await;
        for (sound_index, lag) in [50_000u64, 60_000, 70_000].into_iter().enumerate() {
            send_client_message(
                &participant,
                &ClientMessage::CalibrationDetect {
                    sound_index,
                    detected_at: host_times[sound_index] + lag,
                },
            )
            .await;
        }

        assert_eq!(
            room_repo.participant_lag(room_id, &participant_id),
            Some(60_000)
        );

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_calibration_wrong_senders_ignored() {
        let room_id = "3434";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        let host_id = {
            sleep(Duration::from_millis(200)).await;
            room_repo.host_id(room_id).expect("host registered")
        };
        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let participant_id = join_as_participant(&participant).await;

        // A non-host client must not be able to start a calibration round.
        let host_times = [
            1_700_000_000_000_000,
            1_700_000_001_000_000,
            1_700_000_002_000_000,
        ];
        send_client_message(
            &participant,
            &ClientMessage::CalibrationStart { times: host_times },
        )
        .await;
        // The host must not report sound detections.
        send_client_message(
            &host,
            &ClientMessage::CalibrationDetect {
                sound_index: 0,
                detected_at: 1_700_000_000_050_000,
            },
        )
        .await;
        sleep(Duration::from_millis(100)).await;
        assert_eq!(room_repo.participant_lag(room_id, &participant_id), None);
        assert_eq!(room_repo.participant_lag(room_id, &host_id), None);

        // The connections remain usable: a proper host-initiated round works.
        send_client_message(
            &host,
            &ClientMessage::CalibrationStart { times: host_times },
        )
        .await;
        for (sound_index, sound) in host_times.into_iter().enumerate() {
            send_client_message(
                &participant,
                &ClientMessage::CalibrationDetect {
                    sound_index,
                    detected_at: sound + 60_000,
                },
            )
            .await;
        }

        assert_eq!(
            room_repo.participant_lag(room_id, &participant_id),
            Some(60_000)
        );

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_calibration_duplicate_and_invalid_index_ignored() {
        let room_id = "3535";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;
        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let participant_id = join_as_participant(&participant).await;

        let host_times = [
            1_700_000_000_000_000,
            1_700_000_001_000_000,
            1_700_000_002_000_000,
        ];
        send_client_message(
            &host,
            &ClientMessage::CalibrationStart { times: host_times },
        )
        .await;

        // Sound 0 is reported twice; the first report (lag 50_000 µs) must win.
        send_client_message(
            &participant,
            &ClientMessage::CalibrationDetect {
                sound_index: 0,
                detected_at: host_times[0] + 50_000,
            },
        )
        .await;
        send_client_message(
            &participant,
            &ClientMessage::CalibrationDetect {
                sound_index: 0,
                detected_at: host_times[0] + 550_000,
            },
        )
        .await;
        // An out-of-range index is ignored.
        send_client_message(
            &participant,
            &ClientMessage::CalibrationDetect {
                sound_index: CALIBRATION_SOUND_COUNT,
                detected_at: host_times[0] + 550_000,
            },
        )
        .await;

        for (sound_index, lag) in [60_000u64, 120_000].into_iter().enumerate() {
            send_client_message(
                &participant,
                &ClientMessage::CalibrationDetect {
                    sound_index: sound_index + 1,
                    detected_at: host_times[sound_index + 1] + lag,
                },
            )
            .await;
        }

        // First-wins for sound 0 gives diffs (50_000, 60_000, 120_000) whose
        // median is 60_000; an overwritten report would give 120_000.
        assert_eq!(
            room_repo.participant_lag(room_id, &participant_id),
            Some(60_000)
        );

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_host_time_sync() {
        let room_id = "1212";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let connection = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .expect("open_bi")
            .await
            .expect("open_bi2");
        let mut request = Vec::new();
        ClientMessage::TimeSyncRequest
            .encode(&mut request)
            .expect("encode time sync request");
        send.write_all(&request).await.expect("write");
        send.finish().await.expect("finish");

        let mut buf = Vec::new();
        recv.read_to_end(&mut buf).await.expect("read_to_end");
        match decode_exact::<ServerMessage>(&buf).expect("valid server message") {
            ServerMessage::TimeSyncResponse { t1, t2 } => {
                assert!(t1 <= t2);
                assert!(t2.saturating_sub(t1) < 5_000);
            }
            other => panic!("unexpected message: {other:?}"),
        }

        connection.close(wtransport::VarInt::from_u32(0), b"done");
        sleep(Duration::from_millis(300)).await;
        assert!(
            !room_repo.exists(room_id),
            "room should be removed after host disconnect"
        );
    }

    #[tokio::test]
    async fn test_webtransport_unknown_event_ignored() {
        let room_id = "4444";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;

        let connection = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect");

        let (mut send, mut recv) = connection
            .open_bi()
            .await
            .expect("open_bi")
            .await
            .expect("open_bi2");
        // 0x7F is an unassigned client -> server event ID.
        send.write_all(&[0x7F]).await.expect("write");
        send.finish().await.expect("finish");

        let mut buf = Vec::new();
        recv.read_to_end(&mut buf).await.expect("read_to_end");
        assert!(buf.is_empty(), "unknown event should get no response");

        // The connection must remain usable after an ignored message.
        let participant_id = join_as_participant(&connection).await;
        assert!(!participant_id.to_string().is_empty());

        sleep(Duration::from_millis(100)).await;
        assert_eq!(room_repo.participant_count(room_id), Some(1));
        connection.close(wtransport::VarInt::from_u32(0), b"done");
        host.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_nonexistent_room_closed() {
        let room_repo = RoomRepository::new();
        let pool = sqlx::MySqlPool::connect_lazy("mysql://root:password@127.0.0.1:3306/database")
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

    #[tokio::test]
    async fn test_webtransport_participant_ready_notifies_host() {
        let room_id = "9191";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;

        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let participant_id = join_as_participant(&participant).await;

        // Drain the participant-joined notification first.
        let (_send_stream, mut recv_stream) = host
            .accept_bi()
            .await
            .expect("host accepts server-initiated stream");
        let mut buf = Vec::new();
        recv_stream
            .read_to_end(&mut buf)
            .await
            .expect("read_to_end");
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf).expect("valid server message"),
            ServerMessage::ParticipantJoined { .. }
        ));

        assert_eq!(
            room_repo.participant_is_ready(room_id, &participant_id),
            Some(false),
            "participants start as not ready"
        );

        // The participant reports itself as ready.
        let response = send_client_message(&participant, &ClientMessage::Ready).await;
        assert!(response.is_empty(), "ready must not get a response");
        assert_eq!(
            room_repo.participant_is_ready(room_id, &participant_id),
            Some(true)
        );

        // The ready notification arrives on a server-initiated stream.
        let (_send_stream, mut recv_stream) = host
            .accept_bi()
            .await
            .expect("host accepts server-initiated stream");
        let mut buf = Vec::new();
        recv_stream
            .read_to_end(&mut buf)
            .await
            .expect("read_to_end");
        match decode_exact::<ServerMessage>(&buf).expect("valid server message") {
            ServerMessage::ParticipantReady {
                participant_id: notified,
            } => {
                assert_eq!(notified, participant_id);
            }
            other => panic!("unexpected message: {other:?}"),
        }

        // A repeated report does not retrigger the notification.
        let response = send_client_message(&participant, &ClientMessage::Ready).await;
        assert!(response.is_empty());
        let result = timeout(Duration::from_millis(300), host.accept_bi()).await;
        assert!(
            result.is_err(),
            "no second ready notification should be sent"
        );

        // A ready report from the host is ignored.
        let response = send_client_message(&host, &ClientMessage::Ready).await;
        assert!(response.is_empty());
        let result = timeout(Duration::from_millis(300), host.accept_bi()).await;
        assert!(result.is_err(), "host ready report must not notify anyone");

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_participant_stamp_notifies_host() {
        let room_id = "9292";
        let (_room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;

        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let participant_id = join_as_participant(&participant).await;

        // Drain the participant-joined notification first.
        let (_send_stream, mut recv_stream) = host
            .accept_bi()
            .await
            .expect("host accepts server-initiated stream");
        let mut buf = Vec::new();
        recv_stream
            .read_to_end(&mut buf)
            .await
            .expect("read_to_end");
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf).expect("valid server message"),
            ServerMessage::ParticipantJoined { .. }
        ));

        // The participant sends a stamp; the host is notified with the
        // sender's id and the (uninterpreted) stamp id.
        let response =
            send_client_message(&participant, &ClientMessage::Stamp { stamp_id: 42 }).await;
        assert!(response.is_empty(), "stamp must not get a response");

        let (_send_stream, mut recv_stream) = host
            .accept_bi()
            .await
            .expect("host accepts server-initiated stream");
        let mut buf = Vec::new();
        recv_stream
            .read_to_end(&mut buf)
            .await
            .expect("read_to_end");
        match decode_exact::<ServerMessage>(&buf).expect("valid server message") {
            ServerMessage::ParticipantStamp {
                participant_id: notified,
                stamp_id,
            } => {
                assert_eq!(notified, participant_id);
                assert_eq!(stamp_id, 42);
            }
            other => panic!("unexpected message: {other:?}"),
        }

        // Every stamp report is relayed; stamp ids are opaque to the server.
        for stamp_id in [0u8, 255] {
            send_client_message(&participant, &ClientMessage::Stamp { stamp_id }).await;
            let (_send_stream, mut recv_stream) = host
                .accept_bi()
                .await
                .expect("host accepts server-initiated stream");
            let mut buf = Vec::new();
            recv_stream
                .read_to_end(&mut buf)
                .await
                .expect("read_to_end");
            match decode_exact::<ServerMessage>(&buf).expect("valid server message") {
                ServerMessage::ParticipantStamp {
                    participant_id: notified,
                    stamp_id: notified_stamp,
                } => {
                    assert_eq!(notified, participant_id);
                    assert_eq!(notified_stamp, stamp_id);
                }
                other => panic!("unexpected message: {other:?}"),
            }
        }

        // A stamp report from the host is ignored.
        let response = send_client_message(&host, &ClientMessage::Stamp { stamp_id: 1 }).await;
        assert!(response.is_empty());
        let result = timeout(Duration::from_millis(300), host.accept_bi()).await;
        assert!(result.is_err(), "host stamp report must not notify anyone");

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_notify_host_on_participant_join() {
        let room_id = "9090";
        let (_room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;

        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let participant_id = join_as_participant(&participant).await;

        // The notification arrives on a server-initiated bidirectional stream.
        let (_send_stream, mut recv_stream) = host
            .accept_bi()
            .await
            .expect("host accepts server-initiated stream");

        let mut buf = Vec::new();
        recv_stream
            .read_to_end(&mut buf)
            .await
            .expect("read_to_end");
        match decode_exact::<ServerMessage>(&buf).expect("valid server message") {
            ServerMessage::ParticipantJoined {
                participant_id: notified,
            } => {
                assert_eq!(notified, participant_id);
            }
            other => panic!("unexpected message: {other:?}"),
        }

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[test]
    fn test_certificate_hash_hex() {
        let bytes = [
            0x00, 0x0f, 0xa5, 0xff, 0x12, 0x34, 0x56, 0x78, 0x9a, 0xbc, 0xde, 0xf0, 0x01, 0x23,
            0x45, 0x67, 0x89, 0xab, 0xcd, 0xef, 0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
            0xca, 0xfe, 0xba, 0xbe,
        ];
        let digest = Sha256Digest::new(bytes);
        let hex = certificate_hash_hex(&digest);
        assert_eq!(hex.len(), 64);
        assert_eq!(
            hex,
            "000fa5ff123456789abcdef00123456789abcdeffedcba9876543210cafebabe"
        );
        assert!(
            hex.chars()
                .all(|c| c.is_ascii_digit() || ('a'..='f').contains(&c))
        );
    }
}
