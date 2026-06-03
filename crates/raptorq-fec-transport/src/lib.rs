//! Datagram-transport wrappers for `raptorq-datagram-fec`.
//!
//! WebTransport datagrams and WebRTC data-channel messages both provide an
//! unordered datagram-like surface. This crate keeps the FEC layer independent
//! of any one runtime by wrapping a small async `DatagramSender` trait.

use async_trait::async_trait;
use bytes::Bytes;
use raptorq_datagram_fec::{
    DatagramFecDecoder, DatagramFecEncoder, DatagramFecError, DecodedMediaFrame, EncodedMediaBlock,
    FecDecision, MediaFecDecoder, MediaFecEncoder, MediaFecError, MediaFrame, SequenceStats,
};
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
    pub fn webtransport() -> Self {
        Self {
            kind: FecTransportKind::WebTransport,
            stream_id_mode: StreamIdMode::None,
        }
    }

    pub fn webtransport_with_stream_prefix(stream_id: u64) -> Self {
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
    inner: DatagramFecEncoder,
    stream_id_mode: StreamIdMode,
}

impl FecDatagramEncoder {
    pub fn new(config: FecTransportConfig) -> Self {
        Self {
            inner: DatagramFecEncoder::new(),
            stream_id_mode: config.stream_id_mode,
        }
    }

    pub fn webtransport() -> Self {
        Self::new(FecTransportConfig::webtransport())
    }

    pub fn webtransport_with_stream_prefix(stream_id: u64) -> Self {
        Self::new(FecTransportConfig::webtransport_with_stream_prefix(
            stream_id,
        ))
    }

    pub fn webrtc() -> Self {
        Self::new(FecTransportConfig::webrtc())
    }

    pub fn fec_encoder(&self) -> &DatagramFecEncoder {
        &self.inner
    }

    pub fn fec_encoder_mut(&mut self) -> &mut DatagramFecEncoder {
        &mut self.inner
    }

    pub fn encode_payload(&mut self, payload: &[u8]) -> Result<Vec<Bytes>, DatagramFecError> {
        let prefix = self.stream_id_mode.prefix();
        self.inner
            .encode_payload(payload)?
            .into_iter()
            .map(|datagram| Ok(Bytes::from(add_prefix(prefix, datagram))))
            .collect()
    }

    pub fn encode_media_frame(
        &self,
        media: &mut MediaFecEncoder,
        frame: MediaFrame<'_>,
    ) -> Result<EncodedTransportMediaFrame, MediaFecError> {
        let prefix = self.stream_id_mode.prefix();
        let encoded = media.encode_frame(frame)?;
        Ok(EncodedTransportMediaFrame {
            sequence: encoded.sequence,
            fragment_count: encoded.fragment_count,
            decision: encoded.decision,
            blocks: encoded.blocks,
            datagrams: encoded
                .datagrams
                .into_iter()
                .map(|datagram| Bytes::from(add_prefix(prefix, datagram)))
                .collect(),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct EncodedTransportMediaFrame {
    pub sequence: u64,
    pub fragment_count: u16,
    pub decision: FecDecision,
    pub blocks: Vec<EncodedMediaBlock>,
    pub datagrams: Vec<Bytes>,
}

#[derive(Debug)]
pub struct FecDatagramDecoder {
    inner: DatagramFecDecoder,
    stream_id_mode: StreamIdMode,
}

impl FecDatagramDecoder {
    pub fn new(config: FecTransportConfig) -> Self {
        Self {
            inner: DatagramFecDecoder::new(),
            stream_id_mode: config.stream_id_mode,
        }
    }

    pub fn webtransport() -> Self {
        Self::new(FecTransportConfig::webtransport())
    }

    pub fn webtransport_with_stream_prefix(stream_id: u64) -> Self {
        Self::new(FecTransportConfig::webtransport_with_stream_prefix(
            stream_id,
        ))
    }

    pub fn webrtc() -> Self {
        Self::new(FecTransportConfig::webrtc())
    }

    pub fn push_datagram(&mut self, datagram: &[u8]) -> Result<Option<Vec<u8>>, FecTransportError> {
        let payload = self.strip_transport_prefix(datagram)?;

        self.inner
            .push_datagram(payload)
            .map_err(FecTransportError::Fec)
    }

    pub fn push_media_datagram(
        &self,
        media: &mut MediaFecDecoder,
        datagram: &[u8],
    ) -> Result<Option<DecodedMediaFrame>, FecTransportMediaError> {
        let payload = self
            .strip_transport_prefix(datagram)
            .map_err(FecTransportMediaError::Transport)?;
        media
            .push_datagram(payload)
            .map_err(FecTransportMediaError::Media)
    }

    pub fn sequence_stats(&self) -> SequenceStats {
        self.inner.sequence_stats()
    }

    fn strip_transport_prefix<'a>(
        &self,
        datagram: &'a [u8],
    ) -> Result<&'a [u8], FecTransportError> {
        match self.stream_id_mode {
            StreamIdMode::None => Ok(datagram),
            StreamIdMode::Prefix64Be(expected) => {
                let (stream_id, payload) = split_stream_id_prefix(datagram)
                    .ok_or(FecTransportError::MissingStreamIdPrefix)?;
                if stream_id != expected {
                    return Err(FecTransportError::UnexpectedStreamId {
                        expected,
                        actual: stream_id,
                    });
                }
                Ok(payload)
            }
        }
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
    Fec(DatagramFecError),
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
pub enum FecTransportMediaError {
    Transport(FecTransportError),
    Media(MediaFecError),
}

impl fmt::Display for FecTransportMediaError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::Media(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for FecTransportMediaError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FecSendError<T> {
    Fec(DatagramFecError),
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
    use raptorq_datagram_fec::{
        AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig, MediaCodec, MediaFrameFlags,
        MediaFrameMetadata,
    };

    #[test]
    fn stream_id_prefix_is_big_endian() {
        let prefix = encode_stream_id_prefix(0x0102_0304_0506_0708);
        assert_eq!(prefix, [1, 2, 3, 4, 5, 6, 7, 8]);
    }

    #[test]
    fn webtransport_codec_roundtrips_without_per_datagram_stream_prefix() {
        let payload = b"fec over webtransport".repeat(32);
        let mut encoder = FecDatagramEncoder::webtransport();
        encoder.fec_encoder_mut().set_source_symbols(32);
        encoder.fec_encoder_mut().set_symbol_size(64);
        encoder.fec_encoder_mut().set_repair_symbols(2);

        let datagrams = encoder.encode_payload(&payload).expect("encode");
        assert_ne!(
            &datagrams[0][..STREAM_ID_PREFIX_LEN],
            encode_stream_id_prefix(42)
        );

        let mut decoder = FecDatagramDecoder::webtransport();
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
        assert!(decoder.sequence_stats().missing >= 1);
    }

    #[test]
    fn explicit_stream_prefix_roundtrips_when_requested() {
        let payload = b"fec with explicit stream prefix";
        let mut encoder = FecDatagramEncoder::webtransport_with_stream_prefix(42);
        let datagrams = encoder.encode_payload(payload).expect("encode");
        assert_eq!(
            &datagrams[0][..STREAM_ID_PREFIX_LEN],
            encode_stream_id_prefix(42)
        );

        let mut decoder = FecDatagramDecoder::webtransport_with_stream_prefix(42);
        let decoded = decoder
            .push_datagram(&datagrams[0])
            .expect("decode")
            .expect("decoded payload");
        assert_eq!(decoded, payload);
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

    #[test]
    fn webtransport_media_frame_roundtrips_without_stream_prefix() {
        let policy = AdaptiveFecPolicy {
            min_source_symbols: 4,
            max_source_symbols: 4,
            min_repair_symbols: 1,
            max_repair_symbols: 4,
            min_repair_ratio: 0.25,
            max_repair_ratio: 0.5,
            symbol_size: 96,
            ..AdaptiveFecPolicy::default()
        };
        let controller = AdaptiveFecController::new(policy, CongestionConfig::default());
        let mut media_encoder = MediaFecEncoder::new(controller);
        let transport_encoder = FecDatagramEncoder::webtransport();
        let payload = vec![0x42; 900];
        let metadata = MediaFrameMetadata {
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(
                3,
                media_encoder.allocate_sequence(),
                1_000,
                MediaCodec::H264,
            )
        };

        let encoded = transport_encoder
            .encode_media_frame(
                &mut media_encoder,
                MediaFrame {
                    metadata,
                    payload: &payload,
                },
            )
            .expect("encode media frame");
        assert_ne!(
            &encoded.datagrams[0][..STREAM_ID_PREFIX_LEN],
            &encode_stream_id_prefix(3)
        );
        assert_eq!(encoded.blocks.len(), usize::from(encoded.fragment_count));
        assert!(encoded.blocks.len() > 1);
        for block in &encoded.blocks {
            assert!(block.source_symbols >= 1);
            assert!(block.source_symbols <= policy.max_source_symbols);
            assert_eq!(
                block.source_datagram_indices().count(),
                usize::from(block.source_symbols)
            );
            assert_eq!(
                block.repair_datagram_indices().count(),
                block.repair_symbols as usize
            );
        }

        let transport_decoder = FecDatagramDecoder::webtransport();
        let mut media_decoder = MediaFecDecoder::new();
        let mut decoded = None;
        for (index, datagram) in encoded.datagrams.iter().enumerate() {
            if index == 1 {
                continue;
            }
            decoded = transport_decoder
                .push_media_datagram(&mut media_decoder, datagram)
                .expect("decode media datagram");
            if decoded.is_some() {
                break;
            }
        }

        let decoded = decoded.expect("complete media frame");
        assert_eq!(decoded.metadata, metadata);
        assert_eq!(decoded.payload, payload);
    }
}
