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
//! | ID   | Direction | Event   | Payload                       |
//! |------|-----------|---------|-------------------------------|
//! | 0x01 | C -> S    | Join    | empty                         |
//! | 0x81 | S -> C    | Joined  | 16-byte UUID (participant id) |
//! | 0x82 | S -> C    | Error   | u16 length + UTF-8 message    |

use std::str::from_utf8;

use uuid::Uuid;

use crate::domain::room::{ClientMessage, ServerMessage};

/// Event ID of [`ClientMessage::Join`].
pub const EVENT_JOIN: u8 = 0x01;
/// Event ID of [`ServerMessage::Joined`].
pub const EVENT_JOINED: u8 = 0x81;
/// Event ID of [`ServerMessage::Error`].
pub const EVENT_ERROR: u8 = 0x82;

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
        }
    }
}

impl Decode for ClientMessage {
    fn decode(buf: &mut &[u8]) -> Result<Self, DecodeError> {
        match u8::decode(buf)? {
            EVENT_JOIN => Ok(ClientMessage::Join),
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
