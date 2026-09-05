use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::model::CompleteSongData;

/// Grace period after the host disconnects before the room is removed.
///
/// If a host with the same token reconnects within this period, the room
/// (participants, readiness and live state) is restored. Otherwise the
/// room is removed and remaining participant connections are closed.
pub const HOST_GRACE_PERIOD: Duration = Duration::from_secs(20);

pub enum Room {
    Waiting(WaitingRoom),
    HostJoined(Box<HostJoinedRoom>),
    Live(Box<LiveRoom>),
}

impl Room {
    /// Returns the room's song, regardless of the room state.
    pub fn song(&self) -> &CompleteSongData {
        match self {
            Room::Waiting(waiting) => waiting.song(),
            Room::HostJoined(joined) => joined.song(),
            Room::Live(live) => live.song(),
        }
    }

    /// Returns the room's participants; `None` while the room is waiting for
    /// its host, as participants may join only after the host has joined.
    pub fn participants(&self) -> Option<&HashMap<Uuid, Participant>> {
        match self {
            Room::Waiting(_) => None,
            Room::HostJoined(joined) => Some(&joined.participants),
            Room::Live(live) => Some(&live.participants),
        }
    }

    pub(crate) fn participants_mut(&mut self) -> Option<&mut HashMap<Uuid, Participant>> {
        match self {
            Room::Waiting(_) => None,
            Room::HostJoined(joined) => Some(&mut joined.participants),
            Room::Live(live) => Some(&mut live.participants),
        }
    }

    pub(crate) fn host_joined_mut(&mut self) -> Option<&mut HostJoinedRoom> {
        match self {
            Room::Waiting(_) | Room::Live(_) => None,
            Room::HostJoined(joined) => Some(joined.as_mut()),
        }
    }

    /// Returns the room's host token, regardless of the room state.
    pub fn host_token(&self) -> Option<&str> {
        match self {
            Room::Waiting(waiting) => Some(waiting.host_token()),
            Room::HostJoined(joined) => Some(joined.host_token()),
            Room::Live(live) => Some(live.host_token()),
        }
    }

    /// Returns the room's connected host, if any. `None` while waiting for
    /// the host or during the post-disconnect grace period.
    pub fn host(&self) -> Option<&Host> {
        match self {
            Room::Waiting(_) => None,
            Room::HostJoined(joined) => joined.host(),
            Room::Live(live) => live.host(),
        }
    }

    /// Whether the host is currently connected. New participants may join
    /// only while this is `true`; during the grace period (`false`) joins
    /// are blocked because the host cannot be notified.
    pub fn is_host_connected(&self) -> bool {
        match self {
            Room::Waiting(_) => false,
            Room::HostJoined(joined) => joined.host().is_some(),
            Room::Live(live) => live.host().is_some(),
        }
    }

    /// Marks the host as disconnected (grace period start). Returns `true`
    /// only if `host_id` matches the currently connected host.
    pub(crate) fn disconnect_host(&mut self, host_id: &Uuid) -> bool {
        match self {
            Room::Waiting(_) => false,
            Room::HostJoined(joined) => joined.disconnect_host(host_id),
            Room::Live(live) => live.disconnect_host(host_id),
        }
    }

    /// Replaces the host connection, whether or not a host is currently
    /// connected. Returns the previous host, if any.
    ///
    /// Used when a new connection arrives with a valid host token while an
    /// old host is still connected: the new connection takes over and the
    /// caller is responsible for closing the old one. Participants, live
    /// state and shakes are preserved.
    pub(crate) fn replace_host(&mut self, host: Host) -> Option<Host> {
        match self {
            Room::Waiting(_) => None,
            Room::HostJoined(joined) => joined.replace_host(host),
            Room::Live(live) => live.replace_host(host),
        }
    }
}

pub struct Host {
    id: Uuid,
    connection: wtransport::Connection,
}

impl Host {
    pub fn new(id: Uuid, connection: wtransport::Connection) -> Self {
        Self { id, connection }
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn connection(&self) -> &wtransport::Connection {
        &self.connection
    }
}

pub struct WaitingRoom {
    host_token: String,
    song: CompleteSongData,
}

impl WaitingRoom {
    pub fn new(song: CompleteSongData, host_token: String) -> Self {
        Self { host_token, song }
    }

    pub fn song(&self) -> &CompleteSongData {
        &self.song
    }

    pub fn host_token(&self) -> &str {
        &self.host_token
    }

    pub fn join_host(self, host: Host) -> HostJoinedRoom {
        HostJoinedRoom {
            host: Some(host),
            host_token: self.host_token,
            song: self.song,
            participants: HashMap::new(),
        }
    }
}

/// A participant of a room, along with its readiness state.
pub struct Participant {
    connection: wtransport::Connection,
    /// Whether the participant has reported itself as ready. Starts as
    /// `false`; a participant may only become ready (never un-ready).
    is_ready: bool,
}

impl Participant {
    pub fn new(connection: wtransport::Connection) -> Self {
        Self {
            connection,
            is_ready: false,
        }
    }

    pub fn connection(&self) -> &wtransport::Connection {
        &self.connection
    }

    pub fn is_ready(&self) -> bool {
        self.is_ready
    }
}

pub struct HostJoinedRoom {
    host: Option<Host>,
    host_token: String,
    song: CompleteSongData,
    participants: HashMap<Uuid, Participant>,
}

impl HostJoinedRoom {
    pub fn host(&self) -> Option<&Host> {
        self.host.as_ref()
    }

    pub fn host_token(&self) -> &str {
        &self.host_token
    }

    pub fn song(&self) -> &CompleteSongData {
        &self.song
    }

    pub(crate) fn disconnect_host(&mut self, host_id: &Uuid) -> bool {
        match self.host.as_ref() {
            Some(host) if host.id() == *host_id => {
                self.host = None;
                true
            }
            _ => false,
        }
    }

    pub(crate) fn replace_host(&mut self, host: Host) -> Option<Host> {
        self.host.replace(host)
    }

    /// Marks a participant as ready. Returns whether this call caused the
    /// transition (i.e. the participant was not ready before); repeated
    /// reports are idempotent and return `false`.
    pub(crate) fn set_ready(&mut self, participant_id: &Uuid) -> Option<bool> {
        let participant = self.participants.get_mut(participant_id)?;
        let newly_ready = !participant.is_ready;
        participant.is_ready = true;
        Some(newly_ready)
    }

    /// Transitions the room to live with the given start time (unix
    /// microseconds) announced by the host. Participants and
    /// the host connection are carried over.
    pub fn start_live(self, start_time: u64) -> LiveRoom {
        LiveRoom {
            host: self.host,
            host_token: self.host_token,
            song: self.song,
            participants: self.participants,
            start_time,
        }
    }
}

/// A room whose live has started, carrying the start time announced by the
/// host.
pub struct LiveRoom {
    host: Option<Host>,
    host_token: String,
    song: CompleteSongData,
    participants: HashMap<Uuid, Participant>,
    /// Start time of the live (unix microseconds), announced by the host.
    start_time: u64,
}

impl LiveRoom {
    pub fn host(&self) -> Option<&Host> {
        self.host.as_ref()
    }

    pub fn host_token(&self) -> &str {
        &self.host_token
    }

    pub fn song(&self) -> &CompleteSongData {
        &self.song
    }

    pub(crate) fn disconnect_host(&mut self, host_id: &Uuid) -> bool {
        match self.host.as_ref() {
            Some(host) if host.id() == *host_id => {
                self.host = None;
                true
            }
            _ => false,
        }
    }

    pub(crate) fn replace_host(&mut self, host: Host) -> Option<Host> {
        self.host.replace(host)
    }

    /// The live start time (unix microseconds) announced by the host.
    pub fn start_time(&self) -> u64 {
        self.start_time
    }
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateRoomRequest {
    pub song_url: String,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct CreateRoomResponse {
    pub room_id: String,
    pub host_token: Uuid,
}

#[derive(Deserialize, Serialize, ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GetRoomResponse {
    pub song: CompleteSongData,
}

/// Message sent from the client to the server over WebTransport.
///
/// The wire encoding is defined in [`crate::domain::wire`].
#[derive(Debug)]
pub enum ClientMessage {
    Join,
    TimeSyncRequest,
    /// Liveness heartbeat. Clients should send this on a new bidirectional
    /// stream about every 5 seconds for the whole session; the server closes
    /// connections silent for 10 seconds. Fire-and-forget: no response.
    Heartbeat,
    /// Participant: reports itself as ready to start. Idempotent; a repeated
    /// report does not change the state.
    Ready,
    /// Participant: sends a stamp to the host. The server does not interpret
    /// the stamp id; the meaning of each id is a client-side concern. Sent
    /// per stamp, with no server-side state.
    Stamp {
        stamp_id: u8,
    },
    /// Participant: reports a color change to the host. The server does not
    /// interpret the color id; the meaning of each id is a client-side
    /// concern. Sent per change, with no server-side state.
    ColorChange {
        color_id: u8,
    },
    /// Host: announces the start time of the live (unix microseconds) and
    /// transitions the room to live. The server broadcasts the start time to
    /// every participant. Idempotent: a repeated announcement does not
    /// retrigger the broadcast.
    LiveStart {
        start_time: u64,
    },
    /// Participant: reports that its device was shaken. Sent unreliably as a
    /// WebTransport datagram; the server relays it to the host as-is without
    /// aggregation. The host uses the receipt time for scoring.
    Shake,
}

/// Message sent from the server to the client over WebTransport.
///
/// The wire encoding is defined in [`crate::domain::wire`].
#[derive(Debug, Clone)]
pub enum ServerMessage {
    Joined {
        participant_id: Uuid,
    },
    TimeSyncResponse {
        t1: u64,
        t2: u64,
    },
    Error {
        message: String,
    },
    /// Host only: a participant joined the room. Sent on a server-initiated
    /// bidirectional stream.
    ParticipantJoined {
        participant_id: Uuid,
    },
    /// Host only: a participant disconnected (its WebTransport connection
    /// closed). Sent on a server-initiated bidirectional stream.
    ParticipantLeft {
        participant_id: Uuid,
    },
    /// Host only: a participant reported itself as ready to start. Sent on a
    /// server-initiated bidirectional stream, once per participant (a repeated
    /// report does not retrigger the notification).
    ParticipantReady {
        participant_id: Uuid,
    },
    /// Host only: a participant sent a stamp. Relayed as-is; the server does
    /// not interpret the stamp id. Sent on a server-initiated bidirectional
    /// stream, once per stamp report.
    ParticipantStamp {
        participant_id: Uuid,
        stamp_id: u8,
    },
    /// Host only: a participant reported a color change. Relayed as-is; the
    /// server does not interpret the color id. Sent on a server-initiated
    /// bidirectional stream, once per color change report.
    ParticipantColorChange {
        participant_id: Uuid,
        color_id: u8,
    },
    /// Participants: the live has started. Carries the start time (unix
    /// microseconds) announced by the host. Sent on a server-initiated
    /// bidirectional stream, once per room (a repeated announcement does not
    /// retrigger the broadcast).
    LiveStarted {
        start_time: u64,
    },
    /// Host only: a participant shook its device. Relayed as-is; the server
    /// does not aggregate shakes. Sent unreliably as a WebTransport datagram,
    /// once per shake report.
    ParticipantShake {
        participant_id: Uuid,
    },
}

#[cfg(test)]
mod tests {}
