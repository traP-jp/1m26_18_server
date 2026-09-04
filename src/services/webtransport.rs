use std::{
    io,
    net::SocketAddr,
    sync::{Arc, LazyLock},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;

use axum::extract::Query;
use serde::Deserialize;
use tokio::io::AsyncReadExt;
use tokio::time::sleep;
use tokio_util::sync::CancellationToken;
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
        room::{ClientMessage, HOST_GRACE_PERIOD, SYNC_REPORT_DELAY_US, ServerMessage},
        wire::{Encode, EncodeError, decode_exact},
    },
    repository::room::{InsertHostError, InsertParticipantError},
    services::room::RoomService,
};

/// Time without any client bidirectional-stream message after which the
/// server closes the connection as dead.
///
/// Clients should send [`ClientMessage::Heartbeat`] (or any other client
/// message) about every 5 seconds. Transport-level QUIC keep-alives are not
/// sufficient: browsers may keep ACKing them after the tab was closed.
pub(crate) const HEARTBEAT_TIMEOUT: Duration = Duration::from_secs(10);

/// Polling interval of the heartbeat watchdog.
const WATCHDOG_TICK: Duration = Duration::from_secs(1);

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
    heartbeat_timeout: Duration,
    host_grace_period: Duration,
}

impl WebTransportServer {
    /// Returns the SHA-256 digest of the self-signed certificate, needed by browsers as `serverCertificateHashes`.
    pub fn new(
        room_service: RoomService,
        port: u16,
    ) -> Result<(Self, Sha256Digest), WebTransportError> {
        Self::with_heartbeat_timeout(room_service, port, HEARTBEAT_TIMEOUT)
    }

    /// Same as [`WebTransportServer::new`] with an injectable heartbeat
    /// timeout (tests use a short timeout instead of [`HEARTBEAT_TIMEOUT`]).
    pub fn with_heartbeat_timeout(
        room_service: RoomService,
        port: u16,
        heartbeat_timeout: Duration,
    ) -> Result<(Self, Sha256Digest), WebTransportError> {
        Self::with_timeouts(room_service, port, heartbeat_timeout, HOST_GRACE_PERIOD)
    }

    /// Same as [`WebTransportServer::new`] with injectable heartbeat timeout
    /// and host grace period (tests use short values for both).
    pub fn with_timeouts(
        room_service: RoomService,
        port: u16,
        heartbeat_timeout: Duration,
        host_grace_period: Duration,
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
                heartbeat_timeout,
                host_grace_period,
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
            let heartbeat_timeout = self.heartbeat_timeout;
            let host_grace_period = self.host_grace_period;
            let span_id = id;
            id = id.wrapping_add(1);
            tokio::spawn(
                Self::handle_incoming_session(
                    incoming_session,
                    room_service,
                    heartbeat_timeout,
                    host_grace_period,
                )
                .instrument(tracing::info_span!("wt_session", span_id)),
            );
        }
    }

    async fn handle_incoming_session(
        incoming_session: IncomingSession,
        room_service: RoomService,
        heartbeat_timeout: Duration,
        host_grace_period: Duration,
    ) {
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

            run_connection_handlers_with_timeout(
                &room_service,
                &room_id,
                host_id,
                true,
                &connection,
                heartbeat_timeout,
            )
            .await;

            info!(room_id = %room_id, host_id = %host_id, "host disconnected, starting grace period");
            room_service.disconnect_host(&room_id, &host_id, host_grace_period);
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
            Err(InsertParticipantError::LiveStarted) => {
                warn!(room_id = %room_id, "live has already started after accept, closing connection");
                connection.close(wtransport::VarInt::from_u32(403), b"live already started");
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

        run_connection_handlers_with_timeout(
            &room_service,
            &room_id,
            participant_id,
            false,
            &connection,
            heartbeat_timeout,
        )
        .await;

        room_service.leave_room(&room_id, &participant_id);

        // Fire-and-forget: the leave flow must not block on (or fail with)
        // the host notification.
        tokio::spawn(notify_host(
            room_service.clone(),
            room_id.clone(),
            ServerMessage::ParticipantLeft { participant_id },
        ));
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

/// Pushes a server-initiated event to every participant of the room, each on
/// its own server-initiated bidirectional stream.
///
/// Fire-and-forget: a missing room (e.g. removed concurrently) skips the
/// broadcast, and any transport failure is only logged.
async fn notify_participants(room_service: RoomService, room_id: String, message: ServerMessage) {
    let Some(participants) = room_service.participant_connections(&room_id) else {
        debug!(room_id = %room_id, "room is not available, skipping participant notification");
        return;
    };

    for (participant_id, connection) in participants {
        // Each participant is notified on its own task so that a stalled
        // connection does not delay the others.
        let room_id = room_id.clone();
        let message = message.clone();
        tokio::spawn(async move {
            let result = async {
                let (mut send_stream, _recv_stream) = connection.open_bi().await?.await?;
                send_message(&mut send_stream, &message).await
            }
            .await;

            match result {
                Ok(()) => {
                    info!(
                        room_id = %room_id,
                        participant_id = %participant_id,
                        message = ?message,
                        "notified participant"
                    );
                }
                Err(e) => {
                    warn!(
                        room_id = %room_id,
                        participant_id = %participant_id,
                        message = ?message,
                        error = %e,
                        "failed to notify participant"
                    );
                }
            }
        });
    }
}

/// Accepts bidirectional streams until the connection closes, handling each
/// request in its own task. Shared by participants and the host.
///
/// Every successfully decoded client message refreshes `last_seen`, so a
/// client sending any message (including [`ClientMessage::Heartbeat`]) about
/// every 5 seconds stays alive.
async fn handle_connection_streams(
    room_service: &RoomService,
    room_id: &str,
    client_id: Uuid,
    is_host: bool,
    connection: &wtransport::Connection,
    last_seen: Arc<Mutex<Instant>>,
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
        let last_seen = Arc::clone(&last_seen);
        tokio::spawn(async move {
            if let Err(e) = handle_bi_stream(
                &room_service,
                &room_id,
                client_id,
                is_host,
                send_stream,
                recv_stream,
                &last_seen,
            )
            .await
            {
                warn!(error = %e, "failed to handle bi stream");
            }
        });
    }
}

/// Serves a connection until it closes: bidirectional streams are handled
/// inline, datagrams and the heartbeat watchdog run in dedicated tasks.
/// Shared by participants and the host.
///
/// `heartbeat_timeout` is [`HEARTBEAT_TIMEOUT`] in production; tests inject a
/// short timeout.
async fn run_connection_handlers_with_timeout(
    room_service: &RoomService,
    room_id: &str,
    client_id: Uuid,
    is_host: bool,
    connection: &wtransport::Connection,
    heartbeat_timeout: Duration,
) {
    // Per-connection liveness timestamp, refreshed on every decoded client
    // message. The watchdog closes connections silent for `heartbeat_timeout`,
    // which drives the existing cleanup (`leave_room` / `remove_room`).
    let last_seen: Arc<Mutex<Instant>> = Arc::new(Mutex::new(Instant::now()));

    // Unreliable events travel as datagrams; they are served concurrently
    // with the streams. The task ends on its own when the connection closes.
    let datagram_handle = tokio::spawn(handle_datagrams(
        room_service.clone(),
        room_id.to_string(),
        client_id,
        is_host,
        connection.clone(),
        Arc::clone(&last_seen),
    ));

    let watchdog_handle = tokio::spawn(run_heartbeat_watchdog(
        room_id.to_string(),
        client_id,
        connection.clone(),
        Arc::clone(&last_seen),
        heartbeat_timeout,
    ));

    handle_connection_streams(
        room_service,
        room_id,
        client_id,
        is_host,
        connection,
        last_seen,
    )
    .await;

    // The connection is closed (cleanly or by the watchdog): stop the
    // background tasks. The datagram task usually already ended on its own.
    watchdog_handle.abort();
    datagram_handle.abort();
}

/// Closes connections that stopped sending application messages.
///
/// QUIC keep-alives are ACKed by the transport even after the browser tab was
/// closed, so only application traffic proves the client is alive. On timeout
/// the connection is closed, which unblocks `accept_bi` / `receive_datagram`
/// and runs the normal disconnect cleanup.
async fn run_heartbeat_watchdog(
    room_id: String,
    client_id: Uuid,
    connection: wtransport::Connection,
    last_seen: Arc<Mutex<Instant>>,
    timeout: Duration,
) {
    loop {
        sleep(WATCHDOG_TICK).await;
        let elapsed = last_seen.lock().elapsed();
        if elapsed >= timeout {
            warn!(
                room_id = %room_id,
                client_id = %client_id,
                elapsed_secs = elapsed.as_secs(),
                "heartbeat timeout, closing connection"
            );
            connection.close(wtransport::VarInt::from_u32(408), b"heartbeat timeout");
            break;
        }
    }
}

/// Receives datagrams until the connection closes. Only `Shake` reports are
/// expected (and handled) on the unreliable channel; any other or malformed
/// message is ignored.
async fn handle_datagrams(
    room_service: RoomService,
    room_id: String,
    client_id: Uuid,
    is_host: bool,
    connection: wtransport::Connection,
    last_seen: Arc<Mutex<Instant>>,
) {
    loop {
        let datagram = match connection.receive_datagram().await {
            Ok(datagram) => datagram,
            Err(e) => {
                debug!(
                    room_id = %room_id,
                    client_id = %client_id,
                    error = %e,
                    "datagram receive loop ended"
                );
                return;
            }
        };

        match decode_exact::<ClientMessage>(&datagram.payload()) {
            Ok(ClientMessage::Shake { detected_at }) => {
                *last_seen.lock() = Instant::now();
                handle_shake(&room_service, &room_id, client_id, is_host, detected_at);
            }
            Ok(message) => {
                debug!(
                    room_id = %room_id,
                    client_id = %client_id,
                    message = ?message,
                    "ignoring non-shake message on the datagram channel"
                );
            }
            Err(e) => {
                // Unknown event IDs and malformed messages are ignored
                // silently (no response is possible on the unreliable
                // channel) so that new client events can be rolled out
                // independently.
                warn!(client_id = %client_id, error = %e, "ignoring undecodable datagram");
            }
        }
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
    last_seen: &Mutex<Instant>,
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
        *last_seen.lock() = Instant::now();
        let response = ServerMessage::Joined {
            participant_id: client_id,
        };
        send_message(&mut send_stream, &response).await?;
        return Ok(());
    }

    let message = match decode_exact::<ClientMessage>(&buf) {
        Ok(message) => {
            // Any decodable client message proves the application is alive.
            // Unknown IDs and malformed messages must not refresh liveness.
            *last_seen.lock() = Instant::now();
            message
        }
        Err(e) => {
            // Unknown event IDs and malformed messages are ignored silently
            // (no response is written) so that new client events can be
            // rolled out independently.
            warn!(participant_id = %client_id, error = %e, "ignoring undecodable client message");
            return send_stream.finish().await.map_err(WebTransportError::from);
        }
    };

    let response = match message {
        ClientMessage::Join => Some(ServerMessage::Joined {
            participant_id: client_id,
        }),
        ClientMessage::TimeSyncRequest => {
            // Stateless NTP-like exchange: t1 is taken as soon as the request
            // has been received and t2 right before the response is written.
            let t1 = unix_micros();
            let t2 = unix_micros();
            Some(ServerMessage::TimeSyncResponse { t1, t2 })
        }
        ClientMessage::Heartbeat => {
            // Fire-and-forget liveness heartbeat; no response is written.
            debug!(participant_id = %client_id, "heartbeat received");
            None
        }
        ClientMessage::Ready => {
            // Fire-and-forget: the server records the participant's readiness
            // and never responds; the host is notified on the first report.
            handle_ready(room_service, room_id, client_id, is_host);
            None
        }
        ClientMessage::Stamp { stamp_id } => {
            // Fire-and-forget: the server relays the stamp to the host and
            // never responds.
            handle_stamp(room_service, room_id, client_id, is_host, stamp_id);
            None
        }
        ClientMessage::ColorChange { color_id } => {
            // Fire-and-forget: the server relays the color change to the host
            // and never responds.
            handle_color_change(room_service, room_id, client_id, is_host, color_id);
            None
        }
        ClientMessage::LiveStart { start_time } => {
            // Fire-and-forget: the server transitions the room to live and
            // broadcasts the start time to every participant; it never
            // responds.
            handle_live_start(room_service, room_id, client_id, is_host, start_time);
            None
        }
        ClientMessage::Shake { detected_at } => {
            // Shakes are expected on the unreliable datagram channel; for
            // robustness a shake reported on a stream is recorded the same
            // way. Fire-and-forget: no response is written.
            handle_shake(room_service, room_id, client_id, is_host, detected_at);
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

/// Relays a participant's color change to the host on a server-initiated
/// bidirectional stream. The color id is passed through uninterpreted.
fn handle_color_change(
    room_service: &RoomService,
    room_id: &str,
    participant_id: Uuid,
    is_host: bool,
    color_id: u8,
) {
    if is_host {
        warn!(
            room_id = %room_id,
            host_id = %participant_id,
            "ignoring color change from the host"
        );
        return;
    }
    tracing::debug!(
        room_id = %room_id,
        participant_id = %participant_id,
        color_id,
        "participant sent a color change"
    );
    // Fire-and-forget: the relay flow must not block on (or fail with) the
    // host notification.
    tokio::spawn(notify_host(
        room_service.clone(),
        room_id.to_string(),
        ServerMessage::ParticipantColorChange {
            participant_id,
            color_id,
        },
    ));
}

/// Transitions the room to live with the announced start time and broadcasts
/// the start time to every participant on a server-initiated bidirectional
/// stream.
fn handle_live_start(
    room_service: &RoomService,
    room_id: &str,
    client_id: Uuid,
    is_host: bool,
    start_time: u64,
) {
    if !is_host {
        warn!(
            room_id = %room_id,
            participant_id = %client_id,
            "ignoring live start from a non-host client"
        );
        return;
    }
    if let Err(e) = room_service.start_live(room_id, start_time) {
        warn!(room_id = %room_id, error = %e, "failed to start live");
        return;
    }
    // Fire-and-forget: the start flow must not block on (or fail with) the
    // broadcast.
    tokio::spawn(notify_participants(
        room_service.clone(),
        room_id.to_string(),
        ServerMessage::LiveStarted { start_time },
    ));
    // Fire-and-forget: per-beat sync-rate reports are unreliable and the
    // loop ends after the song's last beat, or when the room is removed
    // (the token is cancelled in `remove_room`).
    let sync_cancel = CancellationToken::new();
    room_service.set_sync_cancel(room_id.to_string(), sync_cancel.clone());
    tokio::spawn(run_sync_rate_updates(
        room_service.clone(),
        room_id.to_string(),
        sync_cancel,
    ));
}

/// Records a participant's device-shake report (sent unreliably as a
/// datagram) for per-beat sync-rate calculation.
fn handle_shake(
    room_service: &RoomService,
    room_id: &str,
    participant_id: Uuid,
    is_host: bool,
    detected_at: u64,
) {
    if is_host {
        warn!(
            room_id = %room_id,
            host_id = %participant_id,
            "ignoring device shake from the host"
        );
        return;
    }
    if let Err(e) = room_service.record_shake(room_id, &participant_id, detected_at) {
        warn!(
            room_id = %room_id,
            participant_id = %participant_id,
            error = %e,
            "failed to record device shake"
        );
    }
}

/// Reports the room's sync rate to the host after each beat of the song, as
/// an unreliable datagram. Beats without any valid shake are skipped; the
/// loop ends after the song's last beat or when the room is removed (via
/// `cancellation`).
async fn run_sync_rate_updates(
    room_service: RoomService,
    room_id: String,
    cancellation: CancellationToken,
) {
    let Some(beat_times) = room_service.beat_schedule(&room_id) else {
        debug!(
            room_id = %room_id,
            "room is not live, skipping sync-rate updates"
        );
        room_service.remove_sync_cancel_if_same(&room_id, &cancellation);
        return;
    };

    if cancellation.is_cancelled() {
        debug!(
            room_id = %room_id,
            "sync-rate updates cancelled before start"
        );
        room_service.remove_sync_cancel_if_same(&room_id, &cancellation);
        return;
    }

    for beat_at in beat_times {
        if cancellation.is_cancelled() {
            debug!(
                room_id = %room_id,
                beat_at,
                "sync-rate updates cancelled, stopping"
            );
            break;
        }
        if !room_service.exists(&room_id) {
            debug!(
                room_id = %room_id,
                beat_at,
                "room was removed, stopping sync-rate updates"
            );
            break;
        }

        // Wait until the beat's tolerance window has closed so that all
        // shakes attributed to the beat (including late-arriving reports)
        // are accounted for. The wait is cancelled as soon as the room is
        // removed so the task does not linger through the song.
        let report_at = beat_at.saturating_add(SYNC_REPORT_DELAY_US);
        let now = unix_micros();
        if report_at > now {
            tokio::select! {
                () = sleep(Duration::from_micros(report_at - now)) => {},
                () = cancellation.cancelled() => {
                    debug!(
                        room_id = %room_id,
                        beat_at,
                        "sync-rate updates cancelled while waiting, stopping"
                    );
                    break;
                }
            }
        }

        if cancellation.is_cancelled() {
            debug!(
                room_id = %room_id,
                beat_at,
                "sync-rate updates cancelled, stopping"
            );
            break;
        }
        if !room_service.exists(&room_id) {
            debug!(
                room_id = %room_id,
                beat_at,
                "room was removed, stopping sync-rate updates"
            );
            break;
        }

        let Some(rate) = room_service.sync_rate(&room_id, beat_at) else {
            // `None` means either "no valid shakes" or "room is gone";
            // only the latter stops the loop.
            if cancellation.is_cancelled() || !room_service.exists(&room_id) {
                debug!(
                    room_id = %room_id,
                    beat_at,
                    "room was removed, stopping sync-rate updates"
                );
                break;
            }
            debug!(
                room_id = %room_id,
                beat_at,
                "no valid shakes for beat, skipping sync-rate report"
            );
            continue;
        };

        send_datagram_to_host(&room_service, &room_id, &ServerMessage::SyncRate { rate });
    }

    room_service.remove_sync_cancel_if_same(&room_id, &cancellation);
    debug!(room_id = %room_id, "sync-rate updates stopped");
}

/// Sends a server-initiated event to the room host as an unreliable
/// datagram. Fire-and-forget: a missing host (e.g. the room was removed
/// concurrently) or a transport failure (dropped datagrams are expected on
/// the unreliable channel) is only logged.
fn send_datagram_to_host(room_service: &RoomService, room_id: &str, message: &ServerMessage) {
    let Some(connection) = room_service.host_connection(room_id) else {
        debug!(
            room_id = %room_id,
            "host is not available, skipping datagram notification"
        );
        return;
    };

    let mut payload = Vec::new();
    if let Err(e) = message.encode(&mut payload) {
        warn!(
            room_id = %room_id,
            message = ?message,
            error = %e,
            "failed to encode datagram notification"
        );
        return;
    }

    match connection.send_datagram(payload) {
        Ok(()) => {
            info!(room_id = %room_id, message = ?message, "notified host via datagram");
        }
        Err(e) => {
            warn!(
                room_id = %room_id,
                message = ?message,
                error = %e,
                "failed to send datagram to host"
            );
        }
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

    fn song_with_beats(starts_at_ms: &[f32]) -> CompleteSongData {
        serde_json::from_value(serde_json::json!({
            "artist": "artist",
            "durationMs": 10_000.0,
            "beats": starts_at_ms
                .iter()
                .map(|&starts_at_ms| {
                    serde_json::json!({"startsAtMs": starts_at_ms, "endsAtMs": starts_at_ms + 400.0})
                })
                .collect::<Vec<_>>(),
            "phrases": [],
            "segments": [{"isChorus": false, "startsAtMs": 0.0, "endsAtMs": 10_000.0}],
            "title": "title"
        }))
        .expect("valid song JSON")
    }

    fn setup_room_service(room_id: &str) -> (RoomRepository, RoomService) {
        setup_room_service_with_song(room_id, dummy_complete_song())
    }

    fn setup_room_service_with_song(
        room_id: &str,
        song: CompleteSongData,
    ) -> (RoomRepository, RoomService) {
        let room_repo = RoomRepository::new();
        let pool = sqlx::MySqlPool::connect_lazy("mysql://root:password@127.0.0.1:3306/database")
            .expect("lazy pool");
        let song_repo = SongRepository::new(pool);
        let song_service = SongService::new(song_repo);
        let room_service = RoomService::new(room_repo.clone(), song_service);
        room_repo.insert(
            room_id.to_string(),
            Room::Waiting(WaitingRoom::new(song, HOST_TOKEN.to_string())),
        );
        (room_repo, room_service)
    }

    /// Short grace for tests so host-disconnect removal assertions stay fast.
    const TEST_GRACE: Duration = Duration::from_millis(100);

    async fn start_server(room_service: RoomService) -> (u16, Sha256Digest) {
        start_server_with_timeout(room_service, HEARTBEAT_TIMEOUT).await
    }

    async fn start_server_with_timeout(
        room_service: RoomService,
        heartbeat_timeout: Duration,
    ) -> (u16, Sha256Digest) {
        start_server_with_timeouts(room_service, heartbeat_timeout, TEST_GRACE).await
    }

    async fn start_server_with_timeouts(
        room_service: RoomService,
        heartbeat_timeout: Duration,
        host_grace_period: Duration,
    ) -> (u16, Sha256Digest) {
        let (server, cert_hash) = WebTransportServer::with_timeouts(
            room_service,
            0,
            heartbeat_timeout,
            host_grace_period,
        )
        .expect("server creation");
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
            ServerMessage::ParticipantLeft { .. } => {
                panic!("unexpected participant left notification")
            }
            ServerMessage::ParticipantReady { .. } => {
                panic!("unexpected participant ready notification")
            }
            ServerMessage::ParticipantStamp { .. } => {
                panic!("unexpected participant stamp notification")
            }
            ServerMessage::ParticipantColorChange { .. } => {
                panic!("unexpected participant color change notification")
            }
            ServerMessage::LiveStarted { .. } => {
                panic!("unexpected live started notification")
            }
            ServerMessage::SyncRate { .. } => panic!("unexpected sync rate notification"),
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

    /// Receives one server-initiated message from the given connection.
    async fn recv_server_message(connection: &wtransport::Connection) -> ServerMessage {
        let (_send_stream, mut recv_stream) = connection
            .accept_bi()
            .await
            .expect("server-initiated stream accepted");
        let mut buf = Vec::new();
        recv_stream
            .read_to_end(&mut buf)
            .await
            .expect("read_to_end");
        decode_exact::<ServerMessage>(&buf).expect("valid server message")
    }

    /// Sends one client message as an unreliable datagram.
    fn send_datagram(connection: &wtransport::Connection, message: &ClientMessage) {
        let mut payload = Vec::new();
        message
            .encode(&mut payload)
            .expect("encode datagram message");
        connection.send_datagram(payload).expect("send datagram");
    }

    /// Receives one server-initiated datagram message.
    async fn recv_datagram(connection: &wtransport::Connection) -> ServerMessage {
        let datagram = connection
            .receive_datagram()
            .await
            .expect("receive datagram");
        decode_exact::<ServerMessage>(&datagram.payload()).expect("valid server datagram")
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
    async fn test_webtransport_participant_color_change_notifies_host() {
        let room_id = "9495";
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

        // The participant sends a color change; the host is notified with the
        // sender's id and the (uninterpreted) color id.
        let response =
            send_client_message(&participant, &ClientMessage::ColorChange { color_id: 42 }).await;
        assert!(response.is_empty(), "color change must not get a response");

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
            ServerMessage::ParticipantColorChange {
                participant_id: notified,
                color_id,
            } => {
                assert_eq!(notified, participant_id);
                assert_eq!(color_id, 42);
            }
            other => panic!("unexpected message: {other:?}"),
        }

        // Every color change report is relayed; color ids are opaque to the
        // server.
        for color_id in [0u8, 255] {
            send_client_message(&participant, &ClientMessage::ColorChange { color_id }).await;
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
                ServerMessage::ParticipantColorChange {
                    participant_id: notified,
                    color_id: notified_color,
                } => {
                    assert_eq!(notified, participant_id);
                    assert_eq!(notified_color, color_id);
                }
                other => panic!("unexpected message: {other:?}"),
            }
        }

        // A color change report from the host is ignored.
        let response =
            send_client_message(&host, &ClientMessage::ColorChange { color_id: 1 }).await;
        assert!(response.is_empty());
        let result = timeout(Duration::from_millis(300), host.accept_bi()).await;
        assert!(
            result.is_err(),
            "host color change report must not notify anyone"
        );

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

    #[tokio::test]
    async fn test_webtransport_notify_host_on_participant_leave() {
        let room_id = "9091";
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
        assert!(matches!(
            timeout(Duration::from_secs(5), recv_server_message(&host))
                .await
                .expect("participant-joined notification"),
            ServerMessage::ParticipantJoined { .. }
        ));
        assert_eq!(room_repo.participant_count(room_id), Some(1));

        // Closing the participant's connection notifies the host.
        participant.close(wtransport::VarInt::from_u32(0), b"done");
        match timeout(Duration::from_secs(5), recv_server_message(&host))
            .await
            .expect("participant-left notification")
        {
            ServerMessage::ParticipantLeft {
                participant_id: notified,
            } => {
                assert_eq!(notified, participant_id);
            }
            other => panic!("unexpected message: {other:?}"),
        }
        sleep(Duration::from_millis(200)).await;
        assert_eq!(room_repo.participant_count(room_id), Some(0));

        host.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_live_start_broadcasts_to_participants() {
        let room_id = "9393";
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

        let first = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as first participant");
        join_as_participant(&first).await;
        let second = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as second participant");
        join_as_participant(&second).await;

        // Drain the participant-joined notifications first.
        for _ in 0..2 {
            assert!(matches!(
                recv_server_message(&host).await,
                ServerMessage::ParticipantJoined { .. }
            ));
        }

        // The host announces the live start time.
        let start_time = 1_700_000_000_000_000;
        let response = send_client_message(&host, &ClientMessage::LiveStart { start_time }).await;
        assert!(response.is_empty(), "live start must not get a response");
        assert_eq!(room_repo.start_time(room_id), Some(start_time));

        // Every participant is notified on a server-initiated stream.
        for participant in [&first, &second] {
            match recv_server_message(participant).await {
                ServerMessage::LiveStarted {
                    start_time: notified,
                } => assert_eq!(notified, start_time),
                other => panic!("unexpected message: {other:?}"),
            }
        }

        // The host itself is not notified.
        let result = timeout(Duration::from_millis(300), host.accept_bi()).await;
        assert!(
            result.is_err(),
            "the host must not be notified of the live start"
        );

        host.close(wtransport::VarInt::from_u32(0), b"done");
        first.close(wtransport::VarInt::from_u32(0), b"done");
        second.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_live_start_from_non_host_ignored() {
        let room_id = "9494";
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
        join_as_participant(&participant).await;

        // Drain the participant-joined notification first.
        assert!(matches!(
            recv_server_message(&host).await,
            ServerMessage::ParticipantJoined { .. }
        ));

        // A live start from a participant is ignored.
        let response = send_client_message(
            &participant,
            &ClientMessage::LiveStart {
                start_time: 1_700_000_000_000_000,
            },
        )
        .await;
        assert!(response.is_empty());
        let result = timeout(Duration::from_millis(300), participant.accept_bi()).await;
        assert!(result.is_err(), "non-host live start must not broadcast");

        // The room stays in the host-joined state.
        assert_eq!(room_repo.start_time(room_id), None);
        assert!(room_repo.host_id(room_id).is_some());

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_live_start_duplicate_ignored() {
        let room_id = "9595";
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

        let first = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as first participant");
        join_as_participant(&first).await;
        let second = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as second participant");
        join_as_participant(&second).await;

        // Drain the participant-joined notifications first.
        for _ in 0..2 {
            assert!(matches!(
                recv_server_message(&host).await,
                ServerMessage::ParticipantJoined { .. }
            ));
        }

        let first_start_time = 1_700_000_000_000_000;
        let response = send_client_message(
            &host,
            &ClientMessage::LiveStart {
                start_time: first_start_time,
            },
        )
        .await;
        assert!(response.is_empty());

        for participant in [&first, &second] {
            match recv_server_message(participant).await {
                ServerMessage::LiveStarted {
                    start_time: notified,
                } => assert_eq!(notified, first_start_time),
                other => panic!("unexpected message: {other:?}"),
            }
        }

        // A repeated announcement does not retrigger the broadcast.
        let response = send_client_message(
            &host,
            &ClientMessage::LiveStart {
                start_time: 1_700_000_001_000_000,
            },
        )
        .await;
        assert!(response.is_empty());
        for participant in [&first, &second] {
            let result = timeout(Duration::from_millis(300), participant.accept_bi()).await;
            assert!(result.is_err(), "duplicate live start must not broadcast");
        }
        assert_eq!(room_repo.start_time(room_id), Some(first_start_time));

        host.close(wtransport::VarInt::from_u32(0), b"done");
        first.close(wtransport::VarInt::from_u32(0), b"done");
        second.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_participant_rejected_after_live_start() {
        let room_id = "9696";
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

        // The host announces the live start time.
        let start_time = 1_700_000_000_000_000;
        let response = send_client_message(&host, &ClientMessage::LiveStart { start_time }).await;
        assert!(response.is_empty());
        assert_eq!(room_repo.start_time(room_id), Some(start_time));

        // Participants may not join once the live has started.
        let result = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await;
        assert!(
            result.is_err(),
            "expected connect to fail after the live started"
        );

        host.close(wtransport::VarInt::from_u32(0), b"done");
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

    #[tokio::test]
    async fn test_webtransport_shake_reports_sync_rate_to_host() {
        let room_id = "9797";
        let (room_repo, room_service) =
            setup_room_service_with_song(room_id, song_with_beats(&[0.0, 500.0]));

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

        // The host announces the live start; the participant shakes exactly
        // on beat 0.
        let start_time = unix_micros() + 1_000_000;
        let response = send_client_message(&host, &ClientMessage::LiveStart { start_time }).await;
        assert!(response.is_empty());
        send_datagram(
            &participant,
            &ClientMessage::Shake {
                detected_at: start_time,
            },
        );

        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            room_repo.participant_shake_count(room_id, &participant_id),
            Some(1)
        );

        // The beat-0 report arrives on the datagram channel once the beat's
        // tolerance window has closed.
        let message = timeout(Duration::from_secs(5), recv_datagram(&host))
            .await
            .expect("sync-rate report for beat 0");
        assert!(
            matches!(message, ServerMessage::SyncRate { rate: 100 }),
            "a shake exactly on the beat must score 100, got {message:?}"
        );

        // Beat 1 (at start + 500 ms) has no shakes: no report is sent for it.
        let result = timeout(Duration::from_millis(800), recv_datagram(&host)).await;
        assert!(result.is_err(), "beats without shakes must not be reported");

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_host_shake_ignored() {
        let room_id = "9898";
        let (room_repo, room_service) =
            setup_room_service_with_song(room_id, song_with_beats(&[0.0, 500.0]));

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

        let start_time = unix_micros() + 1_000_000;
        let response = send_client_message(&host, &ClientMessage::LiveStart { start_time }).await;
        assert!(response.is_empty());

        // The participant shakes exactly on beat 0.
        send_datagram(
            &participant,
            &ClientMessage::Shake {
                detected_at: start_time,
            },
        );
        sleep(Duration::from_millis(100)).await;
        assert_eq!(
            room_repo.participant_shake_count(room_id, &participant_id),
            Some(1)
        );

        // A shake sent by the host is ignored.
        send_datagram(
            &host,
            &ClientMessage::Shake {
                detected_at: start_time,
            },
        );

        // The beat-0 report arrives; beat 1 has no shakes and is skipped.
        let message = timeout(Duration::from_secs(5), recv_datagram(&host))
            .await
            .expect("sync-rate report for beat 0");
        assert!(
            matches!(message, ServerMessage::SyncRate { rate: 100 }),
            "a shake exactly on the beat must score 100, got {message:?}"
        );
        let result = timeout(Duration::from_millis(800), recv_datagram(&host)).await;
        assert!(result.is_err(), "beats without shakes must not be reported");

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    /// Sends heartbeats until the task is aborted (keeps a test connection alive).
    async fn heartbeat_loop(connection: wtransport::Connection) {
        loop {
            send_client_message(&connection, &ClientMessage::Heartbeat).await;
            sleep(Duration::from_millis(200)).await;
        }
    }

    #[tokio::test]
    async fn test_webtransport_heartbeat_gets_no_response() {
        let room_id = "3131";
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
        join_as_participant(&participant).await;

        let response = send_client_message(&participant, &ClientMessage::Heartbeat).await;
        assert!(response.is_empty(), "heartbeat must not get a response");

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_idle_participant_reaped() {
        let room_id = "3232";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) =
            start_server_with_timeout(room_service, Duration::from_millis(600)).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;
        // The host stays alive with heartbeats while the participant idles.
        let host_heartbeats = tokio::spawn(heartbeat_loop(host.clone()));

        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        join_as_participant(&participant).await;
        sleep(Duration::from_millis(100)).await;
        assert_eq!(room_repo.participant_count(room_id), Some(1));

        // Drain the join notification so the leave notification can be read next.
        assert!(matches!(
            recv_server_message(&host).await,
            ServerMessage::ParticipantJoined { .. }
        ));

        // No heartbeats from the participant: the watchdog must close it.
        sleep(Duration::from_millis(2500)).await;
        assert_eq!(
            room_repo.participant_count(room_id),
            Some(0),
            "idle participant must be reaped"
        );
        assert!(
            room_repo.exists(room_id),
            "room must survive a participant timeout"
        );
        assert!(matches!(
            timeout(Duration::from_secs(2), recv_server_message(&host))
                .await
                .expect("leave notification"),
            ServerMessage::ParticipantLeft { .. }
        ));

        host_heartbeats.abort();
        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_heartbeat_keeps_participant_alive() {
        let room_id = "3333";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) =
            start_server_with_timeout(room_service, Duration::from_millis(600)).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;
        let host_heartbeats = tokio::spawn(heartbeat_loop(host.clone()));

        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        join_as_participant(&participant).await;
        let participant_heartbeats = tokio::spawn(heartbeat_loop(participant.clone()));

        sleep(Duration::from_millis(2500)).await;
        assert_eq!(
            room_repo.participant_count(room_id),
            Some(1),
            "heartbeat sender must not be reaped"
        );

        host_heartbeats.abort();
        participant_heartbeats.abort();
        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_idle_host_reaped() {
        let room_id = "3434";
        let (room_repo, room_service) = setup_room_service(room_id);

        let (server_port, cert_hash) =
            start_server_with_timeout(room_service, Duration::from_millis(600)).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;
        assert!(room_repo.host_id(room_id).is_some());

        // No heartbeats from the host: the watchdog must close it and the
        // room must be removed like on a clean host disconnect.
        sleep(Duration::from_millis(2500)).await;
        assert!(
            !room_repo.exists(room_id),
            "room must be removed after host heartbeat timeout"
        );

        host.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_shake_before_live_not_recorded() {
        let room_id = "9998";
        let (_room_repo, room_service) =
            setup_room_service_with_song(room_id, song_with_beats(&[0.0]));

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
        join_as_participant(&participant).await;

        // A shake reported before the live starts is not recorded; it is
        // 80 ms off the beat (a score of 20 if it were counted).
        let start_time = unix_micros() + 1_000_000;
        send_datagram(
            &participant,
            &ClientMessage::Shake {
                detected_at: start_time + 80_000,
            },
        );
        sleep(Duration::from_millis(200)).await;

        // After the live starts, a shake exactly on beat 0 must be the only
        // one counted: the rate is 100, not the average with the pre-live
        // shake.
        let response = send_client_message(&host, &ClientMessage::LiveStart { start_time }).await;
        assert!(response.is_empty());
        send_datagram(
            &participant,
            &ClientMessage::Shake {
                detected_at: start_time,
            },
        );

        let message = timeout(Duration::from_secs(5), recv_datagram(&host))
            .await
            .expect("sync-rate report for beat 0");
        assert!(
            matches!(message, ServerMessage::SyncRate { rate: 100 }),
            "the pre-live shake must not be averaged into the beat's rate, got {message:?}"
        );

        host.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_sync_rate_updates_stop_on_host_disconnect() {
        let room_id = "9899";
        // Beats far in the future: without cancellation the task would sleep
        // for seconds after the room is removed.
        let (room_repo, room_service) =
            setup_room_service_with_song(room_id, song_with_beats(&[5000.0, 6000.0]));

        let (server_port, cert_hash) = start_server(room_service).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;

        let start_time = unix_micros() + 1_000_000;
        let response = send_client_message(&host, &ClientMessage::LiveStart { start_time }).await;
        assert!(response.is_empty());

        // Wait for the spawned sync-rate task to register its token.
        let token = timeout(Duration::from_secs(5), async {
            loop {
                if let Some(token) = room_repo.sync_cancel_token(room_id) {
                    return token;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("sync-rate token registered");
        assert!(!token.is_cancelled());

        host.close(wtransport::VarInt::from_u32(0), b"done");

        // The server notices the close, removes the room and cancels the token.
        timeout(Duration::from_secs(5), async {
            loop {
                if !room_repo.exists(room_id) {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("room removed after host disconnect");
        assert!(
            token.is_cancelled(),
            "remove_room must cancel the sync-rate task"
        );
        assert!(
            !room_repo.has_sync_cancel(room_id),
            "cancelled task must not leave a stale token"
        );
    }

    #[tokio::test]
    async fn test_run_sync_rate_updates_exits_promptly_on_cancel() {
        let room_id = "9900";
        let (room_repo, room_service) =
            setup_room_service_with_song(room_id, song_with_beats(&[5000.0, 6000.0]));

        let (server_port, cert_hash) = start_server(room_service.clone()).await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        sleep(Duration::from_millis(200)).await;

        let start_time = unix_micros() + 1_000_000;
        let response = send_client_message(&host, &ClientMessage::LiveStart { start_time }).await;
        assert!(response.is_empty());
        sleep(Duration::from_millis(200)).await;

        // Spawn an extra waiter on far-future beats and cancel it: it must
        // return well before the first beat instead of sleeping through it.
        let extra = CancellationToken::new();
        let handle = tokio::spawn(run_sync_rate_updates(
            room_service.clone(),
            room_id.to_string(),
            extra.clone(),
        ));
        sleep(Duration::from_millis(100)).await;
        extra.cancel();
        timeout(Duration::from_secs(2), handle)
            .await
            .expect("cancelled sync-rate task must finish promptly")
            .expect("task must not panic");

        // The live's own task is still registered; clean up via disconnect.
        assert!(room_repo.has_sync_cancel(room_id));
        host.close(wtransport::VarInt::from_u32(0), b"done");
    }

    async fn wait_until_room_gone(room_repo: &RoomRepository, room_id: &str) {
        timeout(Duration::from_secs(5), async {
            loop {
                if !room_repo.exists(room_id) {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("room should be removed");
    }

    async fn wait_until_host_disconnected(room_repo: &RoomRepository, room_id: &str) {
        timeout(Duration::from_secs(5), async {
            loop {
                if room_repo.exists(room_id) && room_repo.host_id(room_id).is_none() {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("host should enter grace period (room exists, host None)");
    }

    async fn wait_until_host_connected(room_repo: &RoomRepository, room_id: &str) {
        timeout(Duration::from_secs(5), async {
            loop {
                if room_repo.host_id(room_id).is_some() {
                    return;
                }
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("host should be connected");
    }

    #[tokio::test]
    async fn test_webtransport_host_reconnect_within_grace_restores_room() {
        let room_id = "3132";
        let (room_repo, room_service) = setup_room_service(room_id);
        let (server_port, cert_hash) =
            start_server_with_timeouts(room_service, HEARTBEAT_TIMEOUT, Duration::from_secs(2))
                .await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        wait_until_host_connected(&room_repo, room_id).await;
        let first_host_id = room_repo.host_id(room_id).expect("host id");

        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let participant_id = join_as_participant(&participant).await;
        // Mark ready so we can verify state survives the reconnect.
        let response = send_client_message(&participant, &ClientMessage::Ready).await;
        assert!(response.is_empty());
        sleep(Duration::from_millis(200)).await;
        assert_eq!(
            room_repo.participant_is_ready(room_id, &participant_id),
            Some(true)
        );
        // Drain join + ready notifications.
        let _ = timeout(Duration::from_secs(2), recv_server_message(&host)).await;
        let _ = timeout(Duration::from_secs(2), recv_server_message(&host)).await;

        host.close(wtransport::VarInt::from_u32(0), b"done");
        wait_until_host_disconnected(&room_repo, room_id).await;
        assert_eq!(room_repo.participant_count(room_id), Some(1));

        // Same token reconnects with a fresh host id; state is preserved.
        let host2 = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("reconnect as host within grace");
        wait_until_host_connected(&room_repo, room_id).await;
        let second_host_id = room_repo.host_id(room_id).expect("host id");
        assert_ne!(first_host_id, second_host_id);
        assert_eq!(room_repo.participant_count(room_id), Some(1));
        assert_eq!(
            room_repo.participant_is_ready(room_id, &participant_id),
            Some(true)
        );
        assert!(room_repo.exists(room_id));

        // The room accepts new participants again after the reconnect.
        let newcomer = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("new participant after reconnect");
        let _ = join_as_participant(&newcomer).await;
        sleep(Duration::from_millis(200)).await;
        assert_eq!(room_repo.participant_count(room_id), Some(2));

        host2.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
        newcomer.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_new_participant_blocked_during_grace() {
        let room_id = "3133";
        let (room_repo, room_service) = setup_room_service(room_id);
        let (server_port, cert_hash) =
            start_server_with_timeouts(room_service, HEARTBEAT_TIMEOUT, Duration::from_secs(2))
                .await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        wait_until_host_connected(&room_repo, room_id).await;

        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let _ = join_as_participant(&participant).await;
        assert_eq!(room_repo.participant_count(room_id), Some(1));

        host.close(wtransport::VarInt::from_u32(0), b"done");
        wait_until_host_disconnected(&room_repo, room_id).await;

        // New joins are blocked while the host is away (it could not be notified).
        let result = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await;
        assert!(
            result.is_err(),
            "new participant must be rejected during host grace period"
        );
        assert_eq!(room_repo.participant_count(room_id), Some(1));

        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_host_grace_expiry_removes_room() {
        let room_id = "3134";
        let (room_repo, room_service) = setup_room_service(room_id);
        let (server_port, cert_hash) =
            start_server_with_timeouts(room_service, HEARTBEAT_TIMEOUT, Duration::from_millis(300))
                .await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        wait_until_host_connected(&room_repo, room_id).await;

        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let _ = join_as_participant(&participant).await;
        assert_eq!(room_repo.participant_count(room_id), Some(1));

        host.close(wtransport::VarInt::from_u32(0), b"done");
        // The room survives the disconnect briefly...
        wait_until_host_disconnected(&room_repo, room_id).await;
        assert_eq!(room_repo.participant_count(room_id), Some(1));
        // ...then is removed once the grace period expires.
        wait_until_room_gone(&room_repo, room_id).await;

        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }

    #[tokio::test]
    async fn test_webtransport_host_reconnect_preserves_live_state() {
        let room_id = "3135";
        let (room_repo, room_service) = setup_room_service(room_id);
        let (server_port, cert_hash) =
            start_server_with_timeouts(room_service, HEARTBEAT_TIMEOUT, Duration::from_secs(2))
                .await;
        let client = test_client(cert_hash);

        let host = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("connect as host");
        wait_until_host_connected(&room_repo, room_id).await;

        let participant = client
            .connect(format!("https://127.0.0.1:{server_port}/rooms/{room_id}"))
            .await
            .expect("connect as participant");
        let _ = join_as_participant(&participant).await;
        // Drain the join notification.
        let _ = timeout(Duration::from_secs(2), recv_server_message(&host)).await;

        let start_time = 1_700_000_000_000_000;
        let response = send_client_message(&host, &ClientMessage::LiveStart { start_time }).await;
        assert!(response.is_empty());
        sleep(Duration::from_millis(200)).await;
        assert_eq!(room_repo.start_time(room_id), Some(start_time));

        host.close(wtransport::VarInt::from_u32(0), b"done");
        wait_until_host_disconnected(&room_repo, room_id).await;
        // Live state is kept during the grace period.
        assert_eq!(room_repo.start_time(room_id), Some(start_time));

        let host2 = client
            .connect(format!(
                "https://127.0.0.1:{server_port}/rooms/{room_id}?hostToken={HOST_TOKEN}"
            ))
            .await
            .expect("reconnect as host within grace");
        wait_until_host_connected(&room_repo, room_id).await;
        assert_eq!(room_repo.start_time(room_id), Some(start_time));
        assert_eq!(room_repo.participant_count(room_id), Some(1));

        host2.close(wtransport::VarInt::from_u32(0), b"done");
        participant.close(wtransport::VarInt::from_u32(0), b"done");
    }
}
