use std::{collections::HashMap, time::Duration};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::model::CompleteSongData;

/// Tolerance for beat-sync scoring: a device shake exactly on the beat
/// scores 100, one at (or beyond) this distance scores zero,
/// and the score decays linearly in between.
pub const SYNC_TOLERANCE_US: i64 = 100_000;

/// Delay from a beat's start time until the beat's sync-rate report is sent
/// to the host, so that shakes within the beat's tolerance window (including
/// late-arriving reports) have time to arrive.
pub const SYNC_REPORT_DELAY_US: u64 = 200_000;

/// Grace period after the host disconnects before the room is removed.
///
/// If a host with the same token reconnects within this period, the room
/// (participants, readiness, live state, shakes) is restored. Otherwise the
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

    pub(crate) fn live_mut(&mut self) -> Option<&mut LiveRoom> {
        match self {
            Room::Waiting(_) | Room::HostJoined(_) => None,
            Room::Live(live) => Some(live.as_mut()),
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

    /// Restores the host connection during the grace period. Returns `true`
    /// only if the room is currently disconnected (grace period active).
    pub(crate) fn reconnect_host(&mut self, host: Host) -> bool {
        match self {
            Room::Waiting(_) => false,
            Room::HostJoined(joined) => {
                if joined.host().is_some() {
                    return false;
                }
                joined.reconnect_host(host);
                true
            }
            Room::Live(live) => {
                if live.host().is_some() {
                    return false;
                }
                live.reconnect_host(host);
                true
            }
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

    pub(crate) fn reconnect_host(&mut self, host: Host) {
        self.host = Some(host);
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
            shakes: HashMap::new(),
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
    /// Reported device-shake times (unix microseconds), per participant.
    shakes: HashMap<Uuid, Vec<u64>>,
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

    pub(crate) fn reconnect_host(&mut self, host: Host) {
        self.host = Some(host);
    }

    /// The live start time (unix microseconds) announced by the host.
    pub fn start_time(&self) -> u64 {
        self.start_time
    }

    /// Records a device-shake report.
    pub(crate) fn record_shake(&mut self, participant_id: Uuid, detected_at: u64) -> ShakeOutcome {
        if !self.participants.contains_key(&participant_id) {
            return ShakeOutcome::UnknownParticipant;
        }
        self.shakes
            .entry(participant_id)
            .or_default()
            .push(detected_at);
        ShakeOutcome::Recorded
    }

    /// The overall sync rate (0-100) of the device shakes attributed to the
    /// beat starting at `beat_at` (unix microseconds), or `None` if no valid
    /// shake falls within the beat's tolerance window.
    pub(crate) fn sync_rate(&self, beat_at: u64) -> Option<u8> {
        beat_sync_rate(self.participants.keys(), &self.shakes, beat_at)
    }

    /// Absolute start times (unix microseconds) of the song's beats, as seen
    /// from this live's start time; used to schedule per-beat sync-rate
    /// reports.
    pub(crate) fn beat_schedule(&self) -> Vec<u64> {
        self.song
            .beats()
            .iter()
            .map(|beat| beat_start_time(self.start_time, beat.starts_at_ms()))
            .collect()
    }

    #[cfg(test)]
    pub(crate) fn shake_count(&self, participant_id: &Uuid) -> Option<usize> {
        self.shakes.get(participant_id).map(Vec::len)
    }
}

/// Result of recording a participant device shake.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShakeOutcome {
    /// The report was recorded and will be considered in sync calculations.
    Recorded,
    /// The participant is not in the room (e.g. it has disconnected).
    UnknownParticipant,
}

/// Saturating conversion of a unix-microseconds timestamp to `i64`.
///
/// Absolute times fit `i64` until the year ~294247, so this never saturates in practice.
fn timestamp_to_i64(us: u64) -> i64 {
    i64::try_from(us).unwrap_or(i64::MAX)
}

/// Computes the overall sync rate (0-100) of the device shakes attributed to
/// the beat starting at `beat_at` (unix microseconds).
///
/// Only shakes of the given participants are considered; shakes of
/// disconnected participants (not listed) are excluded. Each shake time
/// is scored by its distance to the beat time: exactly on the beat scores
/// 100, at (or beyond) [`SYNC_TOLERANCE_US`] scores zero, decaying linearly
/// in between. Returns `None` when no valid shake falls within the beat's
/// tolerance window.
fn beat_sync_rate<'a>(
    participants: impl IntoIterator<Item = &'a Uuid>,
    shakes: &HashMap<Uuid, Vec<u64>>,
    beat_at: u64,
) -> Option<u8> {
    let mut total = 0.0;
    let mut count = 0usize;
    for participant_id in participants {
        let Some(times) = shakes.get(participant_id) else {
            continue;
        };
        for &detected_at in times {
            let deviation = (timestamp_to_i64(detected_at) - timestamp_to_i64(beat_at)).abs();
            if deviation > SYNC_TOLERANCE_US {
                continue;
            }
            total += 100.0 * (1.0 - deviation as f64 / SYNC_TOLERANCE_US as f64);
            count += 1;
        }
    }
    if count == 0 {
        return None;
    }
    Some((total / count as f64).round().clamp(0.0, 100.0) as u8)
}

/// Absolute start time (unix microseconds) of the beat at `starts_at_ms` into
/// the song, as seen from a live that started at `start_time`. Negative
/// offsets (malformed data) clamp to the live start.
fn beat_start_time(start_time: u64, starts_at_ms: f32) -> u64 {
    start_time.saturating_add((starts_at_ms.max(0.0) * 1000.0) as u64)
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
    /// Participant: reports the absolute time (unix microseconds) at which
    /// its device was shaken. Sent unreliably as a WebTransport datagram;
    /// the server uses the report to compute the room's per-beat sync rate.
    Shake {
        detected_at: u64,
    },
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
    /// Host only: the overall sync rate (0-100) of the device shakes
    /// attributed to one beat of the song. Sent unreliably as a WebTransport
    /// datagram, once per beat; beats without any valid shake are skipped.
    SyncRate {
        rate: u8,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn beat_sync_rate_scores_distance_to_beat() {
        let participant = Uuid::now_v7();
        let beat_at = 1_000_000_000_000_000;

        // Exactly on the beat scores 100.
        let shakes = HashMap::from([(participant, vec![beat_at])]);
        assert_eq!(beat_sync_rate([&participant], &shakes, beat_at), Some(100));

        // Half the tolerance away scores 50.
        let shakes = HashMap::from([(participant, vec![beat_at + 50_000])]);
        assert_eq!(beat_sync_rate([&participant], &shakes, beat_at), Some(50));

        // Beyond the tolerance the shake is not attributed to the beat at all.
        let shakes = HashMap::from([(participant, vec![beat_at + 150_000])]);
        assert_eq!(beat_sync_rate([&participant], &shakes, beat_at), None);
    }

    #[test]
    fn beat_sync_rate_averages_shake_scores() {
        let participant = Uuid::now_v7();
        let beat_at = 1_000_000_000_000_000;

        // Scores 100 (on the beat) and 40 (60 ms away) average to 70.
        let shakes = HashMap::from([(participant, vec![beat_at, beat_at + 60_000])]);
        assert_eq!(beat_sync_rate([&participant], &shakes, beat_at), Some(70));
    }

    #[test]
    fn beat_sync_rate_excludes_absent_participants() {
        let present = Uuid::now_v7();
        let absent = Uuid::now_v7();
        let missing = Uuid::now_v7();
        let beat_at = 1_000_000_000_000_000;
        let shakes = HashMap::from([(present, vec![beat_at]), (absent, vec![beat_at + 60_000])]);

        // Only listed participants are considered; the unlisted shake (which
        // would score 40) does not drag the average down from 100.
        assert_eq!(beat_sync_rate([&present], &shakes, beat_at), Some(100));
        // A listed participant without shakes yields no rate.
        assert_eq!(beat_sync_rate([&missing], &shakes, beat_at), None);
    }

    #[test]
    fn beat_start_time_is_live_start_plus_song_offset() {
        let start_time = 1_000_000_000_000_000;
        assert_eq!(beat_start_time(start_time, 0.0), start_time);
        assert_eq!(beat_start_time(start_time, 500.0), start_time + 500_000);
        // Negative offsets (malformed data) clamp to the live start.
        assert_eq!(beat_start_time(start_time, -1.0), start_time);
    }
}
