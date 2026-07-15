//! Datagram-transport wrappers for `raptorq-datagram-fec`.
//!
//! WebTransport datagrams and WebRTC data-channel messages both provide an
//! unordered datagram-like surface. This crate keeps the FEC layer independent
//! of any one runtime by wrapping a small async `DatagramSender` trait.

use async_trait::async_trait;
use bytes::Bytes;
use raptorq_datagram_fec::{
    DatagramFecDecoder, DatagramFecEncoder, DatagramFecError, DecodedMediaFrame,
    DecodedMultichannelAudioShard, EncodedMediaBlock, EncodedMultichannelAudioEpoch, FecDecision,
    MediaFecDecoder, MediaFecEncoder, MediaFecError, MediaFrame, MultichannelAudioDatagramRole,
    MultichannelAudioFecConfig, MultichannelAudioFecDecoder, MultichannelAudioFecError,
    SequenceStats,
};
use std::fmt;

pub const STREAM_ID_PREFIX_LEN: usize = 8;
pub const MULTICHANNEL_AUDIO_TRANSPORT_MAGIC: [u8; 4] = *b"AEP1";
pub const MULTICHANNEL_AUDIO_TRANSPORT_MAGIC_LEN: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FecTransportKind {
    Udp,
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
    pub fn udp() -> Self {
        Self {
            kind: FecTransportKind::Udp,
            stream_id_mode: StreamIdMode::None,
        }
    }

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

/// Stateless framing adapter for already-RaptorQ-encoded audio epochs.
///
/// This layer never re-encodes an epoch. It reserves any transport prefix in
/// the packetizer's MTU budget, preserves source/repair metadata for pacing,
/// and rejects source packets that appear after repair packets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultichannelAudioTransportAdapter {
    transport: FecTransportConfig,
    max_datagram_size: usize,
}

impl MultichannelAudioTransportAdapter {
    pub fn new(transport: FecTransportConfig, max_datagram_size: usize) -> Self {
        Self {
            transport,
            max_datagram_size,
        }
    }

    pub fn webtransport(max_datagram_size: usize) -> Self {
        Self::new(FecTransportConfig::webtransport(), max_datagram_size)
    }

    pub fn udp(max_datagram_size: usize) -> Self {
        Self::new(FecTransportConfig::udp(), max_datagram_size)
    }

    pub fn webtransport_with_stream_prefix(stream_id: u64, max_datagram_size: usize) -> Self {
        Self::new(
            FecTransportConfig::webtransport_with_stream_prefix(stream_id),
            max_datagram_size,
        )
    }

    pub fn webrtc(max_datagram_size: usize) -> Self {
        Self::new(FecTransportConfig::webrtc(), max_datagram_size)
    }

    pub fn max_datagram_size(&self) -> usize {
        self.max_datagram_size
    }

    pub fn transport_overhead(&self) -> usize {
        MULTICHANNEL_AUDIO_TRANSPORT_MAGIC_LEN
            + match self.transport.stream_id_mode {
                StreamIdMode::None => 0,
                StreamIdMode::Prefix64Be(_) => STREAM_ID_PREFIX_LEN,
            }
    }

    /// Makes the packetizer's size budget match this complete transport message.
    pub fn prepare_fec_config(
        &self,
        mut config: MultichannelAudioFecConfig,
    ) -> MultichannelAudioFecConfig {
        config.max_datagram_size = self.max_datagram_size;
        config.transport_overhead = self.transport_overhead();
        config
    }

    pub fn wrap_epoch(
        &self,
        encoded: EncodedMultichannelAudioEpoch,
    ) -> Result<EncodedTransportMultichannelAudioEpoch, MultichannelAudioTransportError> {
        let prefix = self.transport.stream_id_mode.prefix();
        let mut saw_repair = false;
        let mut datagrams = Vec::with_capacity(encoded.datagrams.len());

        for datagram in encoded.datagrams {
            match datagram.role {
                MultichannelAudioDatagramRole::Source { .. } if saw_repair => {
                    return Err(MultichannelAudioTransportError::SourceAfterRepair {
                        packet_sequence: datagram.packet_sequence,
                    });
                }
                MultichannelAudioDatagramRole::Source { .. } => {}
                MultichannelAudioDatagramRole::Repair { .. } => saw_repair = true,
            }
            let payload = add_prefix_bytes(prefix, add_audio_epoch_prefix(datagram.payload));
            if payload.len() > self.max_datagram_size {
                return Err(MultichannelAudioTransportError::DatagramTooLarge {
                    actual: payload.len(),
                    max: self.max_datagram_size,
                });
            }
            datagrams.push(MultichannelAudioTransportDatagram {
                block_id: datagram.block_id,
                packet_sequence: datagram.packet_sequence,
                role: datagram.role,
                playout_pts_samples: encoded.pts_samples,
                payload,
            });
        }

        Ok(EncodedTransportMultichannelAudioEpoch {
            session_id: encoded.session_id,
            config_generation: encoded.config_generation,
            epoch_id: encoded.epoch_id,
            pts_samples: encoded.pts_samples,
            sample_rate: encoded.sample_rate,
            frame_count: encoded.frame_count,
            block_id: encoded.block_id,
            source_symbols: encoded.source_symbols,
            repair_symbols: encoded.repair_symbols,
            datagrams,
        })
    }

    pub fn payload<'a>(
        &self,
        datagram: &'a [u8],
    ) -> Result<&'a [u8], MultichannelAudioTransportError> {
        let datagram = strip_transport_prefix(self.transport.stream_id_mode, datagram)
            .map_err(MultichannelAudioTransportError::Transport)?;
        strip_audio_epoch_prefix(datagram)
    }

    pub fn push_datagram(
        &self,
        decoder: &mut MultichannelAudioFecDecoder,
        datagram: &[u8],
    ) -> Result<Vec<DecodedMultichannelAudioShard>, MultichannelAudioTransportError> {
        decoder
            .push_datagram(self.payload(datagram)?)
            .map_err(MultichannelAudioTransportError::Audio)
    }

    pub async fn send_epoch<T>(
        &self,
        sender: &mut T,
        encoded: EncodedMultichannelAudioEpoch,
    ) -> Result<MultichannelAudioSendReport, MultichannelAudioSendError<T::Error>>
    where
        T: DatagramSender + Send,
    {
        let encoded = self
            .wrap_epoch(encoded)
            .map_err(MultichannelAudioSendError::Adapter)?;
        let mut report = MultichannelAudioSendReport::default();
        for datagram in encoded.datagrams {
            sender
                .send_datagram(datagram.payload)
                .await
                .map_err(MultichannelAudioSendError::Transport)?;
            match datagram.role {
                MultichannelAudioDatagramRole::Source { .. } => {
                    report.source_datagrams += 1;
                }
                MultichannelAudioDatagramRole::Repair { .. } => {
                    report.repair_datagrams += 1;
                }
            }
        }
        Ok(report)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultichannelAudioTransportDatagram {
    pub block_id: u32,
    pub packet_sequence: u32,
    pub role: MultichannelAudioDatagramRole,
    /// The hard playout deadline in the sender's sample-clock domain.
    pub playout_pts_samples: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedTransportMultichannelAudioEpoch {
    pub session_id: u64,
    pub config_generation: u32,
    pub epoch_id: u64,
    pub pts_samples: u64,
    pub sample_rate: u32,
    pub frame_count: u32,
    pub block_id: u32,
    pub source_symbols: u16,
    pub repair_symbols: u32,
    /// Strictly source packets followed by repair packets.
    pub datagrams: Vec<MultichannelAudioTransportDatagram>,
}

impl EncodedTransportMultichannelAudioEpoch {
    pub fn source_datagrams(&self) -> impl Iterator<Item = &MultichannelAudioTransportDatagram> {
        self.datagrams.iter().take_while(|datagram| {
            matches!(datagram.role, MultichannelAudioDatagramRole::Source { .. })
        })
    }

    pub fn repair_datagrams(&self) -> impl Iterator<Item = &MultichannelAudioTransportDatagram> {
        self.datagrams.iter().skip_while(|datagram| {
            matches!(datagram.role, MultichannelAudioDatagramRole::Source { .. })
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MultichannelAudioSendReport {
    pub source_datagrams: usize,
    pub repair_datagrams: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultichannelAudioTransportError {
    Transport(FecTransportError),
    Audio(MultichannelAudioFecError),
    DatagramTooLarge { actual: usize, max: usize },
    SourceAfterRepair { packet_sequence: u32 },
    MissingAudioEpochPrefix,
}

impl fmt::Display for MultichannelAudioTransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Transport(error) => write!(formatter, "{error}"),
            Self::Audio(error) => write!(formatter, "{error}"),
            Self::DatagramTooLarge { actual, max } => {
                write!(
                    formatter,
                    "audio transport datagram is {actual} bytes; maximum is {max}"
                )
            }
            Self::SourceAfterRepair { packet_sequence } => write!(
                formatter,
                "audio source packet {packet_sequence} appears after repair traffic"
            ),
            Self::MissingAudioEpochPrefix => {
                write!(formatter, "missing multichannel audio transport prefix")
            }
        }
    }
}

impl std::error::Error for MultichannelAudioTransportError {}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultichannelAudioSendError<T> {
    Adapter(MultichannelAudioTransportError),
    Transport(T),
}

impl<T> fmt::Display for MultichannelAudioSendError<T>
where
    T: fmt::Display,
{
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Adapter(error) => write!(formatter, "{error}"),
            Self::Transport(error) => write!(formatter, "{error}"),
        }
    }
}

impl<T> std::error::Error for MultichannelAudioSendError<T> where T: fmt::Debug + fmt::Display {}

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
        strip_transport_prefix(self.stream_id_mode, datagram)
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

fn add_prefix_bytes(prefix: Option<[u8; STREAM_ID_PREFIX_LEN]>, datagram: Bytes) -> Bytes {
    let Some(prefix) = prefix else {
        return datagram;
    };
    let mut prefixed = Vec::with_capacity(STREAM_ID_PREFIX_LEN + datagram.len());
    prefixed.extend_from_slice(&prefix);
    prefixed.extend_from_slice(&datagram);
    Bytes::from(prefixed)
}

fn add_audio_epoch_prefix(datagram: Bytes) -> Bytes {
    let mut prefixed = Vec::with_capacity(MULTICHANNEL_AUDIO_TRANSPORT_MAGIC_LEN + datagram.len());
    prefixed.extend_from_slice(&MULTICHANNEL_AUDIO_TRANSPORT_MAGIC);
    prefixed.extend_from_slice(&datagram);
    Bytes::from(prefixed)
}

pub fn is_multichannel_audio_transport_datagram(datagram: &[u8]) -> bool {
    datagram.starts_with(&MULTICHANNEL_AUDIO_TRANSPORT_MAGIC)
}

pub fn strip_audio_epoch_prefix(datagram: &[u8]) -> Result<&[u8], MultichannelAudioTransportError> {
    datagram
        .strip_prefix(&MULTICHANNEL_AUDIO_TRANSPORT_MAGIC)
        .ok_or(MultichannelAudioTransportError::MissingAudioEpochPrefix)
}

fn strip_transport_prefix(
    stream_id_mode: StreamIdMode,
    datagram: &[u8],
) -> Result<&[u8], FecTransportError> {
    match stream_id_mode {
        StreamIdMode::None => Ok(datagram),
        StreamIdMode::Prefix64Be(expected) => {
            let (stream_id, payload) =
                split_stream_id_prefix(datagram).ok_or(FecTransportError::MissingStreamIdPrefix)?;
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

#[cfg(test)]
mod tests {
    use super::*;
    use raptorq_datagram_fec::{
        AdaptiveFecController, AdaptiveFecPolicy, AudioPayloadKind, AudioSampleFormat,
        CongestionConfig, MediaCodec, MediaFrameFlags, MediaFrameMetadata, MultichannelAudioEpoch,
        MultichannelAudioFecEncoder, MultichannelAudioGroup,
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
    fn multichannel_audio_adapter_reserves_prefix_and_preserves_source_first_order() {
        let adapter = MultichannelAudioTransportAdapter::webtransport_with_stream_prefix(42, 1200);
        let config = adapter.prepare_fec_config(MultichannelAudioFecConfig {
            repair_symbols: 3,
            ..MultichannelAudioFecConfig::default()
        });
        assert_eq!(
            config.transport_overhead,
            STREAM_ID_PREFIX_LEN + MULTICHANNEL_AUDIO_TRANSPORT_MAGIC_LEN
        );
        let mut encoder = MultichannelAudioFecEncoder::new(config);
        let payload = vec![0x55; 8_000];
        let groups = [MultichannelAudioGroup {
            group_id: 1,
            channel_start: 0,
            channel_count: 16,
            payload_kind: AudioPayloadKind::Pcm,
            sample_format: AudioSampleFormat::S24Le,
            flags: 0,
            payload: &payload,
        }];
        let encoded = encoder
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 9,
                config_generation: 1,
                epoch_id: 2,
                pts_samples: 240,
                sample_rate: 48_000,
                frame_count: 240,
                groups: &groups,
            })
            .unwrap();

        let wrapped = adapter.wrap_epoch(encoded).unwrap();
        assert!(wrapped
            .datagrams
            .iter()
            .all(|packet| packet.payload.len() <= 1200));
        assert_eq!(
            wrapped.source_datagrams().count(),
            wrapped.source_symbols as usize
        );
        assert_eq!(wrapped.repair_datagrams().count(), 3);
        assert!(wrapped
            .datagrams
            .iter()
            .all(|packet| packet.playout_pts_samples == 240));
        assert!(wrapped.datagrams.iter().all(|packet| {
            packet.payload[STREAM_ID_PREFIX_LEN..].starts_with(&MULTICHANNEL_AUDIO_TRANSPORT_MAGIC)
        }));

        let mut decoder = MultichannelAudioFecDecoder::new();
        let first = adapter
            .push_datagram(&mut decoder, &wrapped.datagrams[0].payload)
            .unwrap();
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].header.pts_samples, 240);
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
