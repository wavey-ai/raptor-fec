//! Datagram-transport wrappers for `raptor-udp-fec`.
//!
//! WebTransport datagrams and WebRTC data-channel messages both provide an
//! unordered datagram-like surface. This crate keeps the FEC layer independent
//! of any one runtime by wrapping a small async `DatagramSender` trait.

use async_trait::async_trait;
use bytes::Bytes;
use raptor_udp_fec::{UdpFecDecoder, UdpFecEncoder, UdpFecError};
use std::fmt;

pub const STREAM_ID_PREFIX_LEN: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FecTransportKind {
    WebTransport,
    WebRtc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamIdMode {
    None,
    Prefix64Be(u64),
}

impl StreamIdMode {
    fn prefix(self) -> Option<[u8; STREAM_ID_PREFIX_LEN]> {
        match self {
            Self::None => None,
            Self::Prefix64Be(stream_id) => Some(encode_stream_id_prefix(stream_id)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FecTransportConfig {
    pub kind: FecTransportKind,
    pub stream_id_mode: StreamIdMode,
}

impl FecTransportConfig {
    pub fn webtransport(stream_id: u64) -> Self {
        Self {
            kind: FecTransportKind::WebTransport,
            stream_id_mode: StreamIdMode::Prefix64Be(stream_id),
        }
    }

    pub fn webrtc() -> Self {
        Self {
            kind: FecTransportKind::WebRtc,
            stream_id_mode: StreamIdMode::None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct FecDatagramEncoder {
    inner: UdpFecEncoder,
    stream_id_mode: StreamIdMode,
}

impl FecDatagramEncoder {
    pub fn new(config: FecTransportConfig) -> Self {
        Self {
            inner: UdpFecEncoder::new(),
            stream_id_mode: config.stream_id_mode,
        }
    }

    pub fn webtransport(stream_id: u64) -> Self {
        Self::new(FecTransportConfig::webtransport(stream_id))
    }

    pub fn webrtc() -> Self {
        Self::new(FecTransportConfig::webrtc())
    }

    pub fn fec_encoder(&self) -> &UdpFecEncoder {
        &self.inner
    }

    pub fn fec_encoder_mut(&mut self) -> &mut UdpFecEncoder {
        &mut self.inner
    }

    pub fn encode_payload(&mut self, payload: &[u8]) -> Result<Vec<Bytes>, UdpFecError> {
        let prefix = self.stream_id_mode.prefix();
        self.inner
            .encode_payload(payload)?
            .into_iter()
            .map(|datagram| Ok(Bytes::from(add_prefix(prefix, datagram))))
            .collect()
    }
}

#[derive(Debug)]
pub struct FecDatagramDecoder {
    inner: UdpFecDecoder,
    stream_id_mode: StreamIdMode,
}

impl FecDatagramDecoder {
    pub fn new(config: FecTransportConfig) -> Self {
        Self {
            inner: UdpFecDecoder::new(),
            stream_id_mode: config.stream_id_mode,
        }
    }

    pub fn webtransport(stream_id: u64) -> Self {
        Self::new(FecTransportConfig::webtransport(stream_id))
    }

    pub fn webrtc() -> Self {
        Self::new(FecTransportConfig::webrtc())
    }

    pub fn push_datagram(&mut self, datagram: &[u8]) -> Result<Option<Vec<u8>>, FecTransportError> {
        let payload = match self.stream_id_mode {
            StreamIdMode::None => datagram,
            StreamIdMode::Prefix64Be(expected) => {
                let (stream_id, payload) = split_stream_id_prefix(datagram)
                    .ok_or(FecTransportError::MissingStreamIdPrefix)?;
                if stream_id != expected {
                    return Err(FecTransportError::UnexpectedStreamId {
                        expected,
                        actual: stream_id,
                    });
                }
                payload
            }
        };

        self.inner
            .push_datagram(payload)
            .map_err(FecTransportError::Fec)
    }
}

#[async_trait]
pub trait DatagramSender {
    type Error: Send + Sync + 'static;

    async fn send_datagram(&mut self, datagram: Bytes) -> Result<(), Self::Error>;
}

#[derive(Debug)]
pub struct FecDatagramSender<T> {
    inner: T,
    encoder: FecDatagramEncoder,
}

impl<T> FecDatagramSender<T> {
    pub fn new(inner: T, config: FecTransportConfig) -> Self {
        Self {
            inner,
            encoder: FecDatagramEncoder::new(config),
        }
    }

    pub fn inner(&self) -> &T {
        &self.inner
    }

    pub fn inner_mut(&mut self) -> &mut T {
        &mut self.inner
    }

    pub fn encoder(&self) -> &FecDatagramEncoder {
        &self.encoder
    }

    pub fn encoder_mut(&mut self) -> &mut FecDatagramEncoder {
        &mut self.encoder
    }
}

impl<T> FecDatagramSender<T>
where
    T: DatagramSender + Send,
{
    pub async fn send_fec(&mut self, payload: &[u8]) -> Result<usize, FecSendError<T::Error>> {
        let datagrams = self
            .encoder
            .encode_payload(payload)
            .map_err(FecSendError::Fec)?;
        let count = datagrams.len();
        for datagram in datagrams {
            self.inner
                .send_datagram(datagram)
                .await
                .map_err(FecSendError::Transport)?;
        }
        Ok(count)
    }
}

pub type WebTransportFecSender<T> = FecDatagramSender<T>;
pub type WebRtcFecSender<T> = FecDatagramSender<T>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FecTransportError {
    MissingStreamIdPrefix,
    UnexpectedStreamId { expected: u64, actual: u64 },
    Fec(UdpFecError),
}

impl fmt::Display for FecTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MissingStreamIdPrefix => write!(formatter, "missing stream id prefix"),
            Self::UnexpectedStreamId { expected, actual } => write!(
                formatter,
                "unexpected stream id prefix: expected {expected}, got {actual}"
            ),
            Self::Fec(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for FecTransportError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FecSendError<T> {
    Fec(UdpFecError),
    Transport(T),
}

impl<T> fmt::Display for FecSendError<T>
where
    T: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fec(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "{error}"),
        }
    }
}

impl<T> std::error::Error for FecSendError<T> where T: fmt::Debug + fmt::Display {}

pub fn encode_stream_id_prefix(stream_id: u64) -> [u8; STREAM_ID_PREFIX_LEN] {
    stream_id.to_be_bytes()
}

pub fn webtransport_subscription_datagram(stream_id: u64) -> Bytes {
    Bytes::copy_from_slice(&encode_stream_id_prefix(stream_id))
}

pub fn split_stream_id_prefix(datagram: &[u8]) -> Option<(u64, &[u8])> {
    if datagram.len() < STREAM_ID_PREFIX_LEN {
        return None;
    }

    let mut prefix = [0; STREAM_ID_PREFIX_LEN];
    prefix.copy_from_slice(&datagram[..STREAM_ID_PREFIX_LEN]);
    Some((
        u64::from_be_bytes(prefix),
        &datagram[STREAM_ID_PREFIX_LEN..],
    ))
}

fn add_prefix(prefix: Option<[u8; STREAM_ID_PREFIX_LEN]>, datagram: Vec<u8>) -> Vec<u8> {
    let Some(prefix) = prefix else {
        return datagram;
    };

    let mut prefixed = Vec::with_capacity(STREAM_ID_PREFIX_LEN + datagram.len());
    prefixed.extend_from_slice(&prefix);
    prefixed.extend_from_slice(&datagram);
    prefixed
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_id_prefix_is_big_endian() {
        let prefix = encode_stream_id_prefix(0x0102_0304_0506_0708);
        assert_eq!(prefix, [1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn webtransport_codec_roundtrips_with_prefixed_datagrams() {
        let payload = b"fec over webtransport".repeat(32);
        let mut encoder = FecDatagramEncoder::webtransport(42);
        encoder.fec_encoder_mut().set_source_symbols(32);
        encoder.fec_encoder_mut().set_symbol_size(64);
        encoder.fec_encoder_mut().set_repair_symbols(2);

        let datagrams = encoder.encode_payload(&payload).expect("encode");
        let mut decoder = FecDatagramDecoder::webtransport(42);
        let mut decoded = None;
        for (index, datagram) in datagrams.iter().enumerate() {
            if index == 1 {
                continue;
            }
            decoded = decoder.push_datagram(datagram).expect("decode");
            if decoded.is_some() {
                break;
            }
        }

        assert_eq!(decoded, Some(payload));
    }

    #[test]
    fn webrtc_codec_uses_unprefixed_datagrams() {
        let payload = b"fec over webrtc";
        let mut encoder = FecDatagramEncoder::webrtc();
        let datagrams = encoder.encode_payload(payload).expect("encode");
        assert_ne!(datagrams[0].len(), STREAM_ID_PREFIX_LEN);

        let mut decoder = FecDatagramDecoder::webrtc();
        let decoded = decoder
            .push_datagram(&datagrams[0])
            .expect("decode")
            .expect("decoded payload");
        assert_eq!(decoded, payload);
    }
}
