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
//! | 0x03 | C -> S    | CalibrationStart   | 3 × u64 (unix µs, host sound times)|
//! | 0x04 | C -> S    | CalibrationDetect  | u8 index + u64 (unix µs, detection)|
//! | 0x81 | S -> C    | Joined             | 16-byte UUID (participant id)      |
//! | 0x83 | S -> C    | TimeSyncResponse   | u64 t1 + u64 t2 (unix µs)          |
//! | 0x82 | S -> C    | Error              | u16 length + UTF-8 message         |

use std::str::from_utf8;

use uuid::Uuid;

use crate::domain::room::{CALIBRATION_SOUND_COUNT, ClientMessage, ServerMessage};

/// Event ID of [`ClientMessage::Join`].
pub const EVENT_JOIN: u8 = 0x01;
/// Event ID of [`ClientMessage::TimeSyncRequest`].
pub const EVENT_TIME_SYNC_REQUEST: u8 = 0x02;
/// Event ID of [`ClientMessage::CalibrationStart`].
pub const EVENT_CALIBRATION_START: u8 = 0x03;
/// Event ID of [`ClientMessage::CalibrationDetect`].
pub const EVENT_CALIBRATION_DETECT: u8 = 0x04;
/// Event ID of [`ServerMessage::Joined`].
pub const EVENT_JOINED: u8 = 0x81;
/// Event ID of [`ServerMessage::TimeSyncResponse`].
pub const EVENT_TIME_SYNC_RESPONSE: u8 = 0x83;
/// Event ID of [`ServerMessage::Error`].
pub const EVENT_ERROR: u8 = 0x82;

#[derive(Debug, thiserror::Error)]
pub enum EncodeError {
    #[error("string length {0} exceeds the u16 length prefix limit")]
    StringTooLong(usize),
    #[error("sound index {0} does not fit in a u8")]
    IndexOutOfRange(usize),
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

impl<const N: usize> Encode for [u64; N] {
    fn encode(&self, buf: &mut Vec<u8>) -> Result<(), EncodeError> {
        for value in self {
            value.encode(buf)?;
        }
        Ok(())
    }
}

impl<const N: usize> Decode for [u64; N] {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        let mut array = [0u64; N];
        for value in &mut array {
            *value = u64::decode(buf)?;
        }
        Ok(array)
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
            ClientMessage::CalibrationStart { times } => {
                EVENT_CALIBRATION_START.encode(buf)?;
                times.encode(buf)
            }
            ClientMessage::CalibrationDetect {
                sound_index,
                detected_at,
            } => {
                // Validate before writing so a failed encode leaves the buffer untouched.
                let sound_index = u8::try_from(*sound_index)
                    .map_err(|_| EncodeError::IndexOutOfRange(*sound_index))?;
                EVENT_CALIBRATION_DETECT.encode(buf)?;
                sound_index.encode(buf)?;
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
            EVENT_CALIBRATION_START => Ok(ClientMessage::CalibrationStart {
                times: <[u64; CALIBRATION_SOUND_COUNT]>::decode(buf)?,
            }),
            EVENT_CALIBRATION_DETECT => Ok(ClientMessage::CalibrationDetect {
                sound_index: usize::from(u8::decode(buf)?),
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
    fn calibration_start_roundtrip() {
        let times = [
            1_700_000_000_000_000,
            1_700_000_001_000_000,
            1_700_000_002_000_000,
        ];
        let mut buf = Vec::new();
        ClientMessage::CalibrationStart { times }
            .encode(&mut buf)
            .expect("encode calibration start");
        assert_eq!(buf[0], EVENT_CALIBRATION_START);
        assert_eq!(buf.len(), 1 + CALIBRATION_SOUND_COUNT * 8);
        match decode_exact::<ClientMessage>(&buf).expect("decode calibration start") {
            ClientMessage::CalibrationStart { times: decoded } => assert_eq!(decoded, times),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn calibration_detect_roundtrip() {
        let mut buf = Vec::new();
        ClientMessage::CalibrationDetect {
            sound_index: 2,
            detected_at: 1_700_000_000_050_000,
        }
        .encode(&mut buf)
        .expect("encode calibration detect");
        assert_eq!(buf[0], EVENT_CALIBRATION_DETECT);
        assert_eq!(buf.len(), 10);
        match decode_exact::<ClientMessage>(&buf).expect("decode calibration detect") {
            ClientMessage::CalibrationDetect {
                sound_index,
                detected_at,
            } => assert_eq!((sound_index, detected_at), (2, 1_700_000_000_050_000)),
            other => panic!("unexpected message: {other:?}"),
        }
    }

    #[test]
    fn calibration_detect_index_too_large() {
        let mut buf = Vec::new();
        assert!(matches!(
            ClientMessage::CalibrationDetect {
                sound_index: usize::from(u8::MAX) + 1,
                detected_at: 0,
            }
            .encode(&mut buf),
            Err(EncodeError::IndexOutOfRange(_))
        ));
        assert!(buf.is_empty());
    }

    #[test]
    fn calibration_truncated_payload() {
        // One host time is 8 bytes; truncate the start payload mid-way.
        let mut buf = vec![EVENT_CALIBRATION_START];
        buf.extend_from_slice(&[0u8; 8]);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
        // A detection is one index byte plus an 8-byte timestamp.
        let mut buf = vec![EVENT_CALIBRATION_DETECT, 0x01];
        buf.extend_from_slice(&[0u8; 7]);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
        ));
        assert!(matches!(
            decode_exact::<ClientMessage>(&[EVENT_CALIBRATION_DETECT]),
            Err(DecodeError::UnexpectedEof)
        ));
    }

    #[test]
    fn calibration_trailing_bytes() {
        let mut buf = Vec::new();
        ClientMessage::CalibrationStart {
            times: [0; CALIBRATION_SOUND_COUNT],
        }
        .encode(&mut buf)
        .expect("encode calibration start");
        buf.push(0x00);
        assert!(matches!(
            decode_exact::<ClientMessage>(&buf),
            Err(DecodeError::TrailingBytes)
        ));

        let mut buf = Vec::new();
        ClientMessage::CalibrationDetect {
            sound_index: 0,
            detected_at: 0,
        }
        .encode(&mut buf)
        .expect("encode calibration detect");
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
        buf = vec![EVENT_JOINED];
        buf.extend_from_slice(&[0u8; 15]);
        assert!(matches!(
            decode_exact::<ServerMessage>(&buf),
            Err(DecodeError::UnexpectedEof)
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
