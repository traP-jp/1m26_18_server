use std::collections::{HashMap, HashSet};

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;
use uuid::Uuid;

use crate::domain::model::CompleteSongData;

/// Number of sound pulses the host plays during latency calibration.
///
/// The length is fixed (for now); change this constant to update the protocol.
pub const CALIBRATION_SOUND_COUNT: usize = 3;

pub enum Room {
    Waiting(WaitingRoom),
    HostJoined(Box<HostJoinedRoom>),
    Live(Box<LiveRoom>),
}

impl Room {
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
            host,
            song: self.song,
            participants: HashMap::new(),
            calibration: None,
            lags: HashMap::new(),
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
    host: Host,
    song: CompleteSongData,
    participants: HashMap<Uuid, Participant>,
    /// In-progress latency calibration, started by the host.
    calibration: Option<Calibration>,
    /// Determined per-participant latency in microseconds
    /// (detected sound time minus host sound time).
    lags: HashMap<Uuid, i64>,
}

impl HostJoinedRoom {
    pub fn host(&self) -> &Host {
        &self.host
    }

    pub fn song(&self) -> &CompleteSongData {
        &self.song
    }

    pub(crate) fn calibration_mut(&mut self) -> Option<&mut Calibration> {
        self.calibration.as_mut()
    }

    pub(crate) fn start_calibration(&mut self, host_times: [u64; CALIBRATION_SOUND_COUNT]) {
        self.calibration = Some(Calibration::new(host_times));
    }

    pub(crate) fn insert_lag(&mut self, participant_id: Uuid, lag: i64) {
        self.lags.insert(participant_id, lag);
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
    /// microseconds) announced by the host. Participants, determined lags and
    /// the host connection are carried over; any in-progress calibration
    /// round is discarded.
    pub fn start_live(self, start_time: u64) -> LiveRoom {
        LiveRoom {
            host: self.host,
            song: self.song,
            participants: self.participants,
            lags: self.lags,
            start_time,
        }
    }

    #[cfg(test)]
    pub fn lag(&self, participant_id: &Uuid) -> Option<i64> {
        self.lags.get(participant_id).copied()
    }
}

/// A room whose live has started, carrying the start time announced by the
/// host.
pub struct LiveRoom {
    host: Host,
    song: CompleteSongData,
    participants: HashMap<Uuid, Participant>,
    /// Determined per-participant latency in microseconds
    /// (detected sound time minus host sound time).
    // Carried over from the host-joined state; nothing consumes it yet.
    #[allow(dead_code)]
    lags: HashMap<Uuid, i64>,
    /// Start time of the live (unix microseconds), announced by the host.
    start_time: u64,
}

impl LiveRoom {
    pub fn host(&self) -> &Host {
        &self.host
    }

    pub fn song(&self) -> &CompleteSongData {
        &self.song
    }

    /// The live start time (unix microseconds) announced by the host.
    pub fn start_time(&self) -> u64 {
        self.start_time
    }

    #[cfg(test)]
    pub fn lag(&self, participant_id: &Uuid) -> Option<i64> {
        self.lags.get(participant_id).copied()
    }
}

/// State of a latency calibration round initiated by the host.
///
/// The host announces the absolute times (unix microseconds) at which it will
/// play `CALIBRATION_SOUND_COUNT` sounds. Each participant reports the absolute
/// time at which it detected each sound along with the sound's index (clients
/// distinguish the sounds by their frequency); the server matches each
/// detection to the host time at that index and, once every sound has been
/// reported, stores the median difference as the participant's lag.
pub struct Calibration {
    host_times: [u64; CALIBRATION_SOUND_COUNT],
    /// Reported sound index and detected-minus-host diff, per participant.
    matched: HashMap<Uuid, Vec<(usize, i64)>>,
    /// Participants whose lag has already been determined for this round.
    completed: HashSet<Uuid>,
}

impl Calibration {
    pub fn new(host_times: [u64; CALIBRATION_SOUND_COUNT]) -> Self {
        Self {
            host_times,
            matched: HashMap::new(),
            completed: HashSet::new(),
        }
    }

    pub fn host_times(&self) -> &[u64; CALIBRATION_SOUND_COUNT] {
        &self.host_times
    }

    /// Records one sound detection, matched against the host time at the
    /// reported index. The first report for an index wins; duplicates and
    /// out-of-range indices are rejected.
    pub fn record_detection(
        &mut self,
        participant_id: Uuid,
        sound_index: usize,
        detected_at: u64,
    ) -> Result<DetectionOutcome, DetectionError> {
        if self.completed.contains(&participant_id) {
            return Ok(DetectionOutcome::AlreadyCompleted);
        }
        if sound_index >= CALIBRATION_SOUND_COUNT {
            return Err(DetectionError::InvalidSoundIndex(
                sound_index,
                CALIBRATION_SOUND_COUNT,
            ));
        }

        let matched = self.matched.entry(participant_id).or_default();
        if matched.iter().any(|&(index, _)| index == sound_index) {
            return Err(DetectionError::DuplicateSoundIndex(sound_index));
        }

        let diff = timestamp_to_i64(detected_at) - timestamp_to_i64(self.host_times[sound_index]);
        matched.push((sound_index, diff));

        if matched.len() < CALIBRATION_SOUND_COUNT {
            return Ok(DetectionOutcome::Recorded);
        }
        let diffs: Vec<i64> = matched.iter().map(|&(_, diff)| diff).collect();
        self.matched.remove(&participant_id);
        self.completed.insert(participant_id);
        Ok(DetectionOutcome::Completed {
            lag: median_i64(&diffs),
        })
    }
}

/// Result of recording one participant sound detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DetectionOutcome {
    /// Detection matched to a host time; more detections are needed.
    Recorded,
    /// All sounds matched; the participant's lag has been determined.
    Completed { lag: i64 },
    /// The participant's lag was already determined for this round.
    AlreadyCompleted,
}

/// Errors reported while recording a calibration sound detection.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DetectionError {
    #[error("sound index {0} is out of range (sound count: {1})")]
    InvalidSoundIndex(usize, usize),
    #[error("sound index {0} was already reported")]
    DuplicateSoundIndex(usize),
}

/// Saturating conversion of a unix-microseconds timestamp to `i64`.
///
/// Absolute times fit `i64` until the year ~294247, so this never saturates in practice.
fn timestamp_to_i64(us: u64) -> i64 {
    i64::try_from(us).unwrap_or(i64::MAX)
}

/// Median of the given values (rounded mean of the two middle values for even lengths).
fn median_i64(values: &[i64]) -> i64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    let mid = sorted.len() / 2;
    match sorted.len() % 2 {
        0 => i64::try_from((i128::from(sorted[mid - 1]) + i128::from(sorted[mid])) / 2)
            .unwrap_or(i64::MAX),
        _ => sorted[mid],
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

/// Message sent from the client to the server over WebTransport.
///
/// The wire encoding is defined in [`crate::domain::wire`].
#[derive(Debug)]
pub enum ClientMessage {
    Join,
    TimeSyncRequest,
    /// Host: announces the absolute times (unix microseconds) at which the
    /// host will play each calibration sound. Starts a calibration round.
    CalibrationStart {
        times: [u64; CALIBRATION_SOUND_COUNT],
    },
    /// Participant: reports the absolute time (unix microseconds) at which a
    /// calibration sound was detected, along with the 0-based index of the
    /// sound. Clients distinguish the sounds by their frequency. Sent once per
    /// detected sound.
    CalibrationDetect {
        sound_index: usize,
        detected_at: u64,
    },
    /// Participant: reports itself as ready to start. Idempotent; a repeated
    /// report does not change the state.
    Ready,
    /// Participant: sends a stamp to the host. The server does not interpret
    /// the stamp id; the meaning of each id is a client-side concern. Sent
    /// per stamp, with no server-side state.
    Stamp {
        stamp_id: u8,
    },
    /// Host: announces the start time of the live (unix microseconds) and
    /// transitions the room to live. The server broadcasts the start time to
    /// every participant. Idempotent: a repeated announcement does not
    /// retrigger the broadcast.
    LiveStart {
        start_time: u64,
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
    /// Participants: the live has started. Carries the start time (unix
    /// microseconds) announced by the host. Sent on a server-initiated
    /// bidirectional stream, once per room (a repeated announcement does not
    /// retrigger the broadcast).
    LiveStarted {
        start_time: u64,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOST_TIMES: [u64; CALIBRATION_SOUND_COUNT] = [1_000_000, 2_000_000, 3_000_000];

    fn new_calibration() -> Calibration {
        Calibration::new(HOST_TIMES)
    }

    #[test]
    fn calibration_accepts_detections_in_any_order() {
        let participant = Uuid::now_v7();
        let mut calibration = new_calibration();

        // Detections carry the index of the detected sound, so they may be
        // reported in any order.
        assert_eq!(
            calibration.record_detection(participant, 2, 3_070_000),
            Ok(DetectionOutcome::Recorded)
        );
        assert_eq!(
            calibration.record_detection(participant, 0, 1_050_000),
            Ok(DetectionOutcome::Recorded)
        );
        assert_eq!(
            calibration.record_detection(participant, 1, 2_060_000),
            Ok(DetectionOutcome::Completed { lag: 60_000 })
        );
    }

    #[test]
    fn calibration_participants_are_independent() {
        let first = Uuid::now_v7();
        let second = Uuid::now_v7();
        let mut calibration = new_calibration();

        assert_eq!(
            calibration.record_detection(first, 0, 1_050_000),
            Ok(DetectionOutcome::Recorded)
        );
        // Another participant's detections must not consume the first one's matches.
        assert_eq!(
            calibration.record_detection(second, 0, 1_050_000),
            Ok(DetectionOutcome::Recorded)
        );
        assert_eq!(
            calibration.record_detection(first, 1, 2_060_000),
            Ok(DetectionOutcome::Recorded)
        );
        assert_eq!(
            calibration.record_detection(second, 1, 2_060_000),
            Ok(DetectionOutcome::Recorded)
        );
        assert_eq!(
            calibration.record_detection(first, 2, 3_070_000),
            Ok(DetectionOutcome::Completed { lag: 60_000 })
        );
        assert_eq!(
            calibration.record_detection(second, 2, 3_070_000),
            Ok(DetectionOutcome::Completed { lag: 60_000 })
        );
    }

    #[test]
    fn calibration_ignores_detections_after_completion() {
        let participant = Uuid::now_v7();
        let mut calibration = new_calibration();

        for (index, detected_at) in [(0, 1_050_000), (1, 2_060_000)] {
            assert_eq!(
                calibration.record_detection(participant, index, detected_at),
                Ok(DetectionOutcome::Recorded)
            );
        }
        assert_eq!(
            calibration.record_detection(participant, 2, 3_070_000),
            Ok(DetectionOutcome::Completed { lag: 60_000 })
        );
        assert_eq!(
            calibration.record_detection(participant, 2, 3_070_000),
            Ok(DetectionOutcome::AlreadyCompleted)
        );
    }

    #[test]
    fn calibration_rejects_out_of_range_index() {
        let mut calibration = new_calibration();

        assert_eq!(
            calibration.record_detection(Uuid::now_v7(), CALIBRATION_SOUND_COUNT, 3_070_000),
            Err(DetectionError::InvalidSoundIndex(
                CALIBRATION_SOUND_COUNT,
                CALIBRATION_SOUND_COUNT
            ))
        );
        // The rejected detection is not recorded for any participant.
        assert!(calibration.matched.is_empty());
    }

    #[test]
    fn calibration_rejects_duplicate_index() {
        let participant = Uuid::now_v7();
        let mut calibration = new_calibration();

        assert_eq!(
            calibration.record_detection(participant, 0, 1_050_000),
            Ok(DetectionOutcome::Recorded)
        );
        assert_eq!(
            calibration.record_detection(participant, 0, 1_060_000),
            Err(DetectionError::DuplicateSoundIndex(0))
        );
        // The first report wins; the round can still complete afterwards.
        assert_eq!(
            calibration.record_detection(participant, 1, 2_060_000),
            Ok(DetectionOutcome::Recorded)
        );
        assert_eq!(
            calibration.record_detection(participant, 2, 3_070_000),
            Ok(DetectionOutcome::Completed { lag: 60_000 })
        );
    }

    #[test]
    fn median_of_odd_count_is_middle_value() {
        assert_eq!(median_i64(&[70_000, 50_000, 60_000]), 60_000);
        assert_eq!(median_i64(&[5]), 5);
    }

    #[test]
    fn median_of_even_count_is_middle_mean() {
        assert_eq!(median_i64(&[1, 2, 4, 7]), 3);
        assert_eq!(median_i64(&[-10, 20]), 5);
    }
}
