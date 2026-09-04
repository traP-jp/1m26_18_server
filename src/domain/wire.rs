//! Binary wire format for events exchanged over WebTransport.
//!
//! Every message is a single byte sequence:
//!
//! ```text
//! +--------------+----------------------------+
//! | event_id u8  | payload (per-event layout) |
//! +--------------+----------------------------+
//! ```
//!
//! Event IDs are allocated in direction-specific ranges; unassigned IDs are
//! reserved for future events:
//!
//! | Range         | Direction       |
//! |---------------|-----------------|
//! | 0x01..=0x7F   | client -> server|
//! | 0x81..=0xFF   | server -> client|
//!
//! Primitive encoding rules, shared by all events:
//!
//! - multi-byte integers are big-endian
//! - UUIDs are raw 16 bytes
//! - strings are a `u16` byte length followed by UTF-8 bytes
//!
//! Currently defined events:
//!
//! | ID   | Direction | Event              | Payload                            |
//! |------|-----------|--------------------|------------------------------------|
//! | 0x01 | C -> S    | Join               | empty                              |
//! | 0x02 | C -> S    | TimeSyncRequest    | empty                              |
//! | 0x03 | C -> S    | Heartbeat          | empty                              |
//! | 0x05 | C -> S    | Ready              | empty                              |
//! | 0x06 | C -> S    | Stamp              | u8 stamp id                        |
//! | 0x07 | C -> S    | LiveStart          | u64 (unix µs, live start time)     |
//! | 0x08 | C -> S    | ColorChange        | u8 color id                        |
//! | 0x09 | C -> S    | Shake              | u64 (unix µs, device shake time)   |
//! | 0x81 | S -> C    | Joined             | 16-byte UUID (participant id)      |
//! | 0x83 | S -> C    | TimeSyncResponse   | u64 t1 + u64 t2 (unix µs)          |
//! | 0x82 | S -> C    | Error              | u16 length + UTF-8 message         |
//! | 0x84 | S -> C    | ParticipantJoined  | 16-byte UUID (participant id)      |
//! | 0x85 | S -> C    | ParticipantReady   | 16-byte UUID (participant id)      |
//! | 0x86 | S -> C    | ParticipantStamp   | 16-byte UUID (participant id) + u8 stamp id |
//! | 0x87 | S -> C    | LiveStarted        | u64 (unix µs, live start time)     |
//! | 0x88 | S -> C    | ParticipantColorChange | 16-byte UUID (participant id) + u8 color id |
//! | 0x89 | S -> C    | SyncRate           | u8 (sync rate 0-100)               |
//! | 0x8A | S -> C    | ParticipantLeft  | 16-byte UUID (participant id)      |
//!
//! `Joined` and `TimeSyncResponse` are responses written on the
//! client-initiated stream that carried the request; `Heartbeat`, `Ready`,
//! `Stamp`, `ColorChange`, `LiveStart` and `Shake` sent on streams are
//! fire-and-forget (the server only finishes the stream). `ParticipantJoined`,
//! `ParticipantLeft`, `ParticipantReady`, `ParticipantStamp`, `LiveStarted`
//! and `ParticipantColorChange` are pushed by the server on a server-initiated
//! bidirectional stream (clients must accept incoming streams). `Shake` is
//! sent by clients as an unreliable WebTransport datagram and `SyncRate` is
//! pushed by the server as a datagram; each datagram carries exactly one
//! message.
//!
//! Liveness: clients should send `Heartbeat` (or any other client message)
//! on a new bidirectional stream about every 5 seconds; the server closes
//! connections silent for 10 seconds. This detects clients whose QUIC
//! connection still ACKs keep-alives after the tab was closed.

use std::str::from_utf8;

use uuid::Uuid;

use crate::domain::room::{ClientMessage, ServerMessage};

/// Event ID of [`ClientMessage::Join`].
pub const EVENT_JOIN: u8 = 0x01;
/// Event ID of [`ClientMessage::TimeSyncRequest`].
pub const EVENT_TIME_SYNC_REQUEST: u8 = 0x02;
/// Event ID of [`ClientMessage::Heartbeat`].
pub const EVENT_HEARTBEAT: u8 = 0x03;
/// Event ID of [`ClientMessage::Ready`].
pub const EVENT_READY: u8 = 0x05;
/// Event ID of [`ClientMessage::Stamp`].
pub const EVENT_STAMP: u8 = 0x06;
/// Event ID of [`ClientMessage::LiveStart`].
pub const EVENT_LIVE_START: u8 = 0x07;
/// Event ID of [`ClientMessage::ColorChange`].
pub const EVENT_COLOR_CHANGE: u8 = 0x08;
/// Event ID of [`ClientMessage::Shake`].
pub const EVENT_SHAKE: u8 = 0x09;
/// Event ID of [`ServerMessage::Joined`].
pub const EVENT_JOINED: u8 = 0x81;
/// Event ID of [`ServerMessage::TimeSyncResponse`].
pub const EVENT_TIME_SYNC_RESPONSE: u8 = 0x83;
/// Event ID of [`ServerMessage::Error`].
pub const EVENT_ERROR: u8 = 0x82;
/// Event ID of [`ServerMessage::ParticipantJoined`].
pub const EVENT_PARTICIPANT_JOINED: u8 = 0x84;
/// Event ID of [`ServerMessage::ParticipantReady`].
pub const EVENT_PARTICIPANT_READY: u8 = 0x85;
/// Event ID of [`ServerMessage::ParticipantStamp`].
pub const EVENT_PARTICIPANT_STAMP: u8 = 0x86;
/// Event ID of [`ServerMessage::LiveStarted`].
pub const EVENT_LIVE_STARTED: u8 = 0x87;
/// Event ID of [`ServerMessage::ParticipantColorChange`].
pub const EVENT_PARTICIPANT_COLOR_CHANGE: u8 = 0x88;
/// Event ID of [`ServerMessage::SyncRate`].
pub const EVENT_SYNC_RATE: u8 = 0x89;
/// Event ID of [`ServerMessage::ParticipantLeft`].
pub const EVENT_PARTICIPANT_LEFT: u8 = 0x8A;

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("string length {0} exceeds the u16 length prefix limit")]
    StringTooLong(usize),
}

#[derive(Debug, thiserror::Error)]
pub enum DecodeError {
    #[error("unexpected end of input")]
    UnexpectedEof,
    #[error("payload is not valid UTF-8")]
    InvalidUtf8,
    #[error("unknown event id: 0x{0:02X}")]
    UnknownEventId(u8),
    #[error("trailing bytes after message")]
    TrailingBytes,
}

pub trait Encode {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError>;
}

pub trait Decode: Sized {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError>;
}

/// Decodes exactly one message, requiring the whole buffer to be consumed.
pub fn decode_exact<T: Decode>(mut buf: &[u8]) -> Result<T, DecodeError> {
    let value = T::decode(&mut buf)?;
    if !buf.is_empty() {
        return Err(DecodeError::TrailingBytes);
    }
    Ok(value)
}

fn take<'a>(buf: &mut &'a [u8], len: usize) -> Result<&'a [u8], DecodeError> {
    if buf.len() < len {
        return Err(DecodeError::UnexpectedEof);
    }
    let (head, rest) = buf.split_at(len);
    *buf = rest;
    Ok(head)
}

impl Encode for u8 {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
        buf.push(*self);
        Ok(())
    }
}

impl Decode for u8 {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let Some((first, rest)) = buf.split_first() else {
            return Err(DecodeError::UnexpectedEof);
        };
        *buf = rest;
        Ok(*first)
    }
}

impl Encode for u16 {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
        buf.extend_from_slice(&self.to_be_bytes());
        Ok(())
    }
}

impl Decode for u16 {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let bytes = take(buf, 2)?;
        Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
    }
}

impl Encode for u64 {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
        buf.extend_from_slice(&self.to_be_bytes());
        Ok(())
    }
}

impl Decode for u64 {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let bytes = take(buf, 8)?;
        let mut array = [0u8; 8];
        array.copy_from_slice(bytes);
        Ok(u64::from_be_bytes(array))
    }
}

impl Encode for str {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
        let len = self.len();
        u16::try_from(len)
            .map_err(|_| EncodeError::StringTooLong(len))?
            .encode(buf)?;
        buf.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

impl Encode for String {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
        self.as_str().encode(buf)
    }
}

impl Decode for String {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let len = u16::decode(buf)?;
        let bytes = take(buf, usize::from(len))?;
        from_utf8(bytes)
            .map(str::to_owned)
            .map_err(|_| DecodeError::InvalidUtf8)
    }
}

impl Encode for Uuid {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
        buf.extend_from_slice(self.as_bytes());
        Ok(())
    }
}

impl Decode for Uuid {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(take(buf, 16)?);
        Ok(Uuid::from_bytes(bytes))
    }
}

impl Encode for ClientMessage {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            ClientMessage::Join => EVENT_JOIN.encode(buf),
            ClientMessage::TimeSyncRequest => EVENT_TIME_SYNC_REQUEST.encode(buf),
            ClientMessage::Heartbeat => EVENT_HEARTBEAT.encode(buf),
            ClientMessage::Ready => EVENT_READY.encode(buf),
            ClientMessage::Stamp { stamp_id } => {
                EVENT_STAMP.encode(buf)?;
                stamp_id.encode(buf)
            }
            ClientMessage::ColorChange { color_id } => {
                EVENT_COLOR_CHANGE.encode(buf)?;
                color_id.encode(buf)
            }
            ClientMessage::LiveStart { start_time } => {
                EVENT_LIVE_START.encode(buf)?;
                start_time.encode(buf)
            }
            ClientMessage::Shake { detected_at } => {
                EVENT_SHAKE.encode(buf)?;
                detected_at.encode(buf)
            }
        }
    }
}

impl Decode for ClientMessage {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        match u8::decode(buf)? {
            EVENT_JOIN => Ok(ClientMessage::Join),
            EVENT_TIME_SYNC_REQUEST => Ok(ClientMessage::TimeSyncRequest),
            EVENT_HEARTBEAT => Ok(ClientMessage::Heartbeat),
            EVENT_READY => Ok(ClientMessage::Ready),
            EVENT_STAMP => Ok(ClientMessage::Stamp {
                stamp_id: u8::decode(buf)?,
            }),
            EVENT_COLOR_CHANGE => Ok(ClientMessage::ColorChange {
                color_id: u8::decode(buf)?,
            }),
            EVENT_LIVE_START => Ok(ClientMessage::LiveStart {
                start_time: u64::decode(buf)?,
            }),
            EVENT_SHAKE => Ok(ClientMessage::Shake {
                detected_at: u64::decode(buf)?,
            }),
            id => Err(DecodeError::UnknownEventId(id)),
        }
    }
}

impl Encode for ServerMessage {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
        match self {
            ServerMessage::Joined { participant_id } => {
                EVENT_JOINED.encode(buf)?;
                participant_id.encode(buf)
            }
            ServerMessage::TimeSyncResponse { t1, t2 } => {
                EVENT_TIME_SYNC_RESPONSE.encode(buf)?;
                t1.encode(buf)?;
                t2.encode(buf)
            }
            ServerMessage::Error { message } => {
                EVENT_ERROR.encode(buf)?;
                message.encode(buf)
            }
            ServerMessage::ParticipantJoined { participant_id } => {
                EVENT_PARTICIPANT_JOINED.encode(buf)?;
                participant_id.encode(buf)
            }
            ServerMessage::ParticipantLeft { participant_id } => {
                EVENT_PARTICIPANT_LEFT.encode(buf)?;
                participant_id.encode(buf)
            }
            ServerMessage::ParticipantReady { participant_id } => {
                EVENT_PARTICIPANT_READY.encode(buf)?;
                participant_id.encode(buf)
            }
            ServerMessage::ParticipantStamp {
                participant_id,
                stamp_id,
            } => {
                EVENT_PARTICIPANT_STAMP.encode(buf)?;
                participant_id.encode(buf)?;
                stamp_id.encode(buf)
            }
            ServerMessage::ParticipantColorChange {
                participant_id,
                color_id,
            } => {
                EVENT_PARTICIPANT_COLOR_CHANGE.encode(buf)?;
                participant_id.encode(buf)?;
                color_id.encode(buf)
            }
            ServerMessage::LiveStarted { start_time } => {
                EVENT_LIVE_STARTED.encode(buf)?;
                start_time.encode(buf)
            }
            ServerMessage::SyncRate { rate } => {
                EVENT_SYNC_RATE.encode(buf)?;
                rate.encode(buf)
            }
        }
    }
}

impl Decode for ServerMessage {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        match u8::decode(buf)? {
            EVENT_JOINED => Ok(ServerMessage::Joined {
                participant_id: Uuid::decode(buf)?,
            }),
            EVENT_TIME_SYNC_RESPONSE => Ok(ServerMessage::TimeSyncResponse {
                t1: u64::decode(buf)?,
                t2: u64::decode(buf)?,
            }),
            EVENT_ERROR => Ok(ServerMessage::Error {
                message: String::decode(buf)?,
            }),
            EVENT_PARTICIPANT_JOINED => Ok(ServerMessage::ParticipantJoined {
                participant_id: Uuid::decode(buf)?,
            }),
            EVENT_PARTICIPANT_LEFT => Ok(ServerMessage::ParticipantLeft {
                participant_id: Uuid::decode(buf)?,
            }),
            EVENT_PARTICIPANT_READY => Ok(ServerMessage::ParticipantReady {
                participant_id: Uuid::decode(buf)?,
            }),
            EVENT_PARTICIPANT_STAMP => Ok(ServerMessage::ParticipantStamp {
                participant_id: Uuid::decode(buf)?,
                stamp_id: u8::decode(buf)?,
            }),
            EVENT_PARTICIPANT_COLOR_CHANGE => Ok(ServerMessage::ParticipantColorChange {
                participant_id: Uuid::decode(buf)?,
                color_id: u8::decode(buf)?,
            }),
            EVENT_LIVE_STARTED => Ok(ServerMessage::LiveStarted {
                start_time: u64::decode(buf)?,
            }),
            EVENT_SYNC_RATE => Ok(ServerMessage::SyncRate {
                rate: u8::decode(buf)?,
            }),
            id => Err(DecodeError::UnknownEventId(id)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_roundtrip() {
        let mut buf = Vec::new();
        ClientMessage::Join.encode(&mut buf).expect("encode join");
        assert_eq!(buf, [EVENT_JOIN]);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf).expect("decode join"),
            ClientMessage::Join
        ));
    }

    #[test]
    fn joined_roundtrip() {
        let id = Uuid::now_v7();
        let mut buf = Vec::new();
        ServerMessage::Joined { participant_id: id }
            .encode(&mut buf)
            .expect("encode joined");
        assert_eq!(buf[0], EVENT_JOINED);
        assert_eq!(buf.len(), 17);
        match decode_exact::<ServerMessage>(&buf).expect("decode joined") {
            ServerMessage::Joined { participant_id } => assert_eq!(participant_id, id),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn participant_joined_roundtrip() {
        let id = Uuid::now_v7();
        let mut buf = Vec::new();
        ServerMessage::ParticipantJoined { participant_id: id }
            .encode(&mut buf)
            .expect("encode participant joined");
        assert_eq!(buf[0], EVENT_PARTICIPANT_JOINED);
        assert_eq!(buf.len(), 17);
        match decode_exact::<ServerMessage>(&buf).expect("decode participant joined") {
            ServerMessage::ParticipantJoined { participant_id } => assert_eq!(participant_id, id),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn participant_left_roundtrip() {
        let id = Uuid::now_v7();
        let mut buf = Vec::new();
        ServerMessage::ParticipantLeft { participant_id: id }
            .encode(&mut buf)
            .expect("encode participant left");
        assert_eq!(buf[0], EVENT_PARTICIPANT_LEFT);
        assert_eq!(buf.len(), 17);
        match decode_exact::<ServerMessage>(&buf).expect("decode participant left") {
            ServerMessage::ParticipantLeft { participant_id } => assert_eq!(participant_id, id),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn ready_roundtrip() {
        let mut buf = Vec::new();
        ClientMessage::Ready.encode(&mut buf).expect("encode ready");
        assert_eq!(buf, [EVENT_READY]);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf).expect("decode ready"),
            ClientMessage::Ready
        ));
    }

    #[test]
    fn participant_ready_roundtrip() {
        let id = Uuid::now_v7();
        let mut buf = Vec::new();
        ServerMessage::ParticipantReady { participant_id: id }
            .encode(&mut buf)
            .expect("encode participant ready");
        assert_eq!(buf[0], EVENT_PARTICIPANT_READY);
        assert_eq!(buf.len(), 17);
        match decode_exact::<ServerMessage>(&buf).expect("decode participant ready") {
            ServerMessage::ParticipantReady { participant_id } => assert_eq!(participant_id, id),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn stamp_roundtrip() {
        let mut buf = Vec::new();
        ClientMessage::Stamp { stamp_id: 42 }
            .encode(&mut buf)
            .expect("encode stamp");
        assert_eq!(buf, [EVENT_STAMP, 42]);
        match decode_exact::<ClientMessage>(&buf).expect("decode stamp") {
            ClientMessage::Stamp { stamp_id } => assert_eq!(stamp_id, 42),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn participant_stamp_roundtrip() {
        let id = Uuid::now_v7();
        let mut buf = Vec::new();
        ServerMessage::ParticipantStamp {
            participant_id: id,
            stamp_id: 7,
        }
        .encode(&mut buf)
        .expect("encode participant stamp");
        assert_eq!(buf[0], EVENT_PARTICIPANT_STAMP);
        assert_eq!(buf.len(), 18);
        match decode_exact::<ServerMessage>(&buf).expect("decode participant stamp") {
            ServerMessage::ParticipantStamp {
                participant_id,
                stamp_id,
            } => assert_eq!((participant_id, stamp_id), (id, 7)),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn color_change_roundtrip() {
        let mut buf = Vec::new();
        ClientMessage::ColorChange { color_id: 42 }
            .encode(&mut buf)
            .expect("encode color change");
        assert_eq!(buf, [EVENT_COLOR_CHANGE, 42]);
        match decode_exact::<ClientMessage>(&buf).expect("decode color change") {
            ClientMessage::ColorChange { color_id } => assert_eq!(color_id, 42),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn participant_color_change_roundtrip() {
        let id = Uuid::now_v7();
        let mut buf = Vec::new();
        ServerMessage::ParticipantColorChange {
            participant_id: id,
            color_id: 7,
        }
        .encode(&mut buf)
        .expect("encode participant color change");
        assert_eq!(buf[0], EVENT_PARTICIPANT_COLOR_CHANGE);
        assert_eq!(buf.len(), 18);
        match decode_exact::<ServerMessage>(&buf).expect("decode participant color change") {
            ServerMessage::ParticipantColorChange {
                participant_id,
                color_id,
            } => assert_eq!((participant_id, color_id), (id, 7)),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn live_start_roundtrip() {
        let mut buf = Vec::new();
        ClientMessage::LiveStart {
            start_time: 1_700_000_000_000_000,
        }
        .encode(&mut buf)
        .expect("encode live start");
        assert_eq!(buf[0], EVENT_LIVE_START);
        assert_eq!(buf.len(), 9);
        match decode_exact::<ClientMessage>(&buf).expect("decode live start") {
            ClientMessage::LiveStart { start_time } => {
                assert_eq!(start_time, 1_700_000_000_000_000);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn live_started_roundtrip() {
        let mut buf = Vec::new();
        ServerMessage::LiveStarted {
            start_time: 1_700_000_000_000_000,
        }
        .encode(&mut buf)
        .expect("encode live started");
        assert_eq!(buf[0], EVENT_LIVE_STARTED);
        assert_eq!(buf.len(), 9);
        match decode_exact::<ServerMessage>(&buf).expect("decode live started") {
            ServerMessage::LiveStarted { start_time } => {
                assert_eq!(start_time, 1_700_000_000_000_000);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn shake_roundtrip() {
        let mut buf = Vec::new();
        ClientMessage::Shake {
            detected_at: 1_700_000_000_012_345,
        }
        .encode(&mut buf)
        .expect("encode shake");
        assert_eq!(buf[0], EVENT_SHAKE);
        assert_eq!(buf.len(), 9);
        match decode_exact::<ClientMessage>(&buf).expect("decode shake") {
            ClientMessage::Shake { detected_at } => {
                assert_eq!(detected_at, 1_700_000_000_012_345);
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn sync_rate_roundtrip() {
        let mut buf = Vec::new();
        ServerMessage::SyncRate { rate: 87 }
            .encode(&mut buf)
            .expect("encode sync rate");
        assert_eq!(buf, [EVENT_SYNC_RATE, 87]);
        match decode_exact::<ServerMessage>(&buf).expect("decode sync rate") {
            ServerMessage::SyncRate { rate } => assert_eq!(rate, 87),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn error_roundtrip() {
        let mut buf = Vec::new();
        ServerMessage::Error {
            message: "エラー".to_owned(),
        }
        .encode(&mut buf)
        .expect("encode error");
        assert_eq!(buf[0], EVENT_ERROR);
        match decode_exact::<ServerMessage>(&buf).expect("decode error") {
            ServerMessage::Error { message } => assert_eq!(message, "エラー"),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn time_sync_request_roundtrip() {
        let mut buf = Vec::new();
        ClientMessage::TimeSyncRequest
            .encode(&mut buf)
            .expect("encode time sync request");
        assert_eq!(buf, [EVENT_TIME_SYNC_REQUEST]);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf).expect("decode time sync request"),
            ClientMessage::TimeSyncRequest
        ));
    }

    #[test]
    fn heartbeat_roundtrip() {
        let mut buf = Vec::new();
        ClientMessage::Heartbeat
            .encode(&mut buf)
            .expect("encode heartbeat");
        assert_eq!(buf, [EVENT_HEARTBEAT]);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf).expect("decode heartbeat"),
            ClientMessage::Heartbeat
        ));
    }

    #[test]
    fn time_sync_response_roundtrip() {
        let mut buf = Vec::new();
        ServerMessage::TimeSyncResponse { t1: 1000, t2: 2000 }
            .encode(&mut buf)
            .expect("encode time sync response");
        assert_eq!(buf[0], EVENT_TIME_SYNC_RESPONSE);
        assert_eq!(buf.len(), 17);
        match decode_exact::<ServerMessage>(&buf).expect("decode time sync response") {
            ServerMessage::TimeSyncResponse { t1, t2 } => {
                assert_eq!((t1, t2), (1000, 2000));
            }
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn shake_truncated_payload() {
        // A shake is an 8-byte timestamp.
        assert!(matches!(
            decode_exact::<ClientMessage>(&[EVENT_SHAKE]),
            Err(DecodeError::UnexpectedEof)
        ));
        let mut buf = vec![EVENT_SHAKE];
        buf.extend_from_slice(&[0u8; 7]);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    #[test]
    fn shake_trailing_bytes() {
        let mut buf = Vec::new();
        ClientMessage::Shake {
            detected_at: 1_700_000_000_000_000,
        }
        .encode(&mut buf)
        .expect("encode shake");
        buf.push(0x00);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf),
            Err(DecodeError::TrailingBytes)
        ));
    }

    #[test]
    fn unknown_event_id() {
        assert!(matches!(
            decode_exact::<ClientMessage>(&[0x7F]),
            Err(DecodeError::UnknownEventId(0x7F))
        ));
        assert!(matches!(
            decode_exact::<ServerMessage>(&[0x80]),
            Err(DecodeError::UnknownEventId(0x80))
        ));
    }

    #[test]
    fn truncated_payload() {
        assert!(matches!(
            decode_exact::<ServerMessage>(&[EVENT_JOINED, 0x00, 0x01]),
            Err(DecodeError::UnexpectedEof)
        ));
        assert!(matches!(
            decode_exact::<ServerMessage>(&[EVENT_ERROR, 0x00]),
            Err(DecodeError::UnexpectedEof)
        ));
        assert!(matches!(
            decode_exact::<ClientMessage>(&[]),
            Err(DecodeError::UnexpectedEof)
        ));
        assert!(matches!(
            decode_exact::<ClientMessage>(&[EVENT_STAMP]),
            Err(DecodeError::UnexpectedEof)
        ));
        assert!(matches!(
            decode_exact::<ClientMessage>(&[EVENT_COLOR_CHANGE]),
            Err(DecodeError::UnexpectedEof)
        ));
        assert!(matches!(
            decode_exact::<ServerMessage>(&[EVENT_PARTICIPANT_STAMP]),
            Err(DecodeError::UnexpectedEof)
        ));
        assert!(matches!(
            decode_exact::<ServerMessage>(&[EVENT_PARTICIPANT_COLOR_CHANGE]),
            Err(DecodeError::UnexpectedEof)
        ));
        let mut buf = vec![EVENT_PARTICIPANT_STAMP];
        buf.extend_from_slice(&[0u8; 15]);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
        let mut buf = vec![EVENT_PARTICIPANT_COLOR_CHANGE];
        buf.extend_from_slice(&[0u8; 15]);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
        assert!(matches!(
            decode_exact::<ServerMessage>(&[EVENT_SYNC_RATE]),
            Err(DecodeError::UnexpectedEof)
        ));
        assert!(matches!(
            decode_exact::<ServerMessage>(&[EVENT_PARTICIPANT_LEFT]),
            Err(DecodeError::UnexpectedEof)
        ));
        let mut buf = vec![EVENT_PARTICIPANT_LEFT];
        buf.extend_from_slice(&[0u8; 15]);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    #[test]
    fn invalid_utf8() {
        let mut buf = vec![EVENT_ERROR];
        3u16.encode(&mut buf).expect("encode length");
        buf.extend_from_slice(&[0xFF, 0xFE, 0xFF]);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::InvalidUtf8)
        ));
    }

    #[test]
    fn trailing_bytes() {
        let mut buf = vec![EVENT_JOIN, 0x00];
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf),
            Err(DecodeError::TrailingBytes)
        ));
        buf = vec![EVENT_HEARTBEAT, 0x00];
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf),
            Err(DecodeError::TrailingBytes)
        ));
        buf = vec![EVENT_JOINED];
        buf.extend_from_slice(&[0u8; 15]);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
        buf = vec![EVENT_PARTICIPANT_JOINED];
        buf.extend_from_slice(&[0u8; 15]);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
        buf = vec![EVENT_PARTICIPANT_READY];
        buf.extend_from_slice(&[0u8; 15]);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
        buf = vec![EVENT_PARTICIPANT_LEFT];
        buf.extend_from_slice(&[0u8; 15]);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
        buf = vec![EVENT_PARTICIPANT_LEFT];
        buf.extend_from_slice(&[0u8; 16]);
        buf.push(0x00);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::TrailingBytes)
        ));
        buf = vec![EVENT_STAMP];
        buf.extend_from_slice(&[0u8; 2]);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf),
            Err(DecodeError::TrailingBytes)
        ));
        buf = vec![EVENT_COLOR_CHANGE];
        buf.extend_from_slice(&[0u8; 2]);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf),
            Err(DecodeError::TrailingBytes)
        ));
        buf = vec![EVENT_PARTICIPANT_STAMP];
        buf.extend_from_slice(&[0u8; 17]);
        buf.push(0x00);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::TrailingBytes)
        ));
        buf = vec![EVENT_PARTICIPANT_COLOR_CHANGE];
        buf.extend_from_slice(&[0u8; 17]);
        buf.push(0x00);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::TrailingBytes)
        ));
        buf = vec![
            EVENT_TIME_SYNC_RESPONSE,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x00,
            0x01,
        ];
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    #[test]
    fn string_too_long() {
        let mut buf = Vec::new();
        let long = "x".repeat(u16::MAX as usize + 1);
        assert!(matches!(
            long.as_str().encode(&mut buf),
            Err(EncodeError::StringTooLong(_))
        ));
        assert!(buf.is_empty());
    }

    #[test]
    fn string_max_length_roundtrip() {
        let mut buf = Vec::new();
        let long = "あ".repeat(u16::MAX as usize / 3);
        long.encode(&mut buf).expect("encode long string");
        assert_eq!(String::decode(&mut buf.as_slice()).expect("decode"), long);
    }
}
