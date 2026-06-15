use crate::{source_symbol_count, DatagramFecDecoder, DatagramFecEncoder, DatagramFecError};
use bytes::Bytes;
use std::fmt;

pub const MUSIC_AUDIO_BLOCK_MAGIC: [u8; 4] = *b"MAB1";
pub const MUSIC_AUDIO_BLOCK_VERSION: u8 = 1;
pub const MUSIC_AUDIO_BLOCK_FIXED_HEADER_LEN: usize = 48;
pub const MUSIC_AUDIO_FRAME_DESCRIPTOR_LEN: usize = 12;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MusicAudioMicroBlockConfig {
    pub stream_id: u64,
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub frame_duration_samples: u32,
    pub frames_per_block: u16,
    pub repair_symbols: u32,
    pub symbol_size: u16,
    pub max_source_symbols: u16,
}

impl MusicAudioMicroBlockConfig {
    pub fn pcm48_stereo_2_5ms(stream_id: u64) -> Self {
        Self {
            stream_id,
            sample_rate_hz: 48_000,
            channels: 2,
            frame_duration_samples: 120,
            frames_per_block: 4,
            repair_symbols: 1,
            symbol_size: 1024,
            max_source_symbols: 4,
        }
    }

    pub fn pcm48_stereo_5ms(stream_id: u64) -> Self {
        Self {
            stream_id,
            sample_rate_hz: 48_000,
            channels: 2,
            frame_duration_samples: 240,
            frames_per_block: 2,
            repair_symbols: 1,
            symbol_size: 2048,
            max_source_symbols: 2,
        }
    }

    pub fn normalized(self) -> Self {
        Self {
            stream_id: self.stream_id,
            sample_rate_hz: self.sample_rate_hz.max(1),
            channels: self.channels.max(1),
            frame_duration_samples: self.frame_duration_samples.max(1),
            frames_per_block: self.frames_per_block.max(1),
            repair_symbols: self.repair_symbols,
            symbol_size: self.symbol_size.max(1),
            max_source_symbols: self.max_source_symbols.max(1),
        }
    }
}

impl Default for MusicAudioMicroBlockConfig {
    fn default() -> Self {
        Self::pcm48_stereo_2_5ms(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MusicAudioFrame<'a> {
    pub sequence: u64,
    pub pts_samples: u64,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedMusicAudioFrame {
    pub sequence: u64,
    pub pts_samples: u64,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedMusicAudioMicroBlock {
    pub stream_id: u64,
    pub first_sequence: u64,
    pub first_pts_samples: u64,
    pub frame_count: u16,
    pub source_symbols: u16,
    pub repair_symbols: u32,
    pub symbol_size: u16,
    pub datagrams: Vec<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedMusicAudioMicroBlock {
    pub stream_id: u64,
    pub sample_rate_hz: u32,
    pub channels: u16,
    pub frame_duration_samples: u32,
    pub frames: Vec<DecodedMusicAudioFrame>,
}

#[derive(Debug, Clone)]
pub struct MusicAudioMicroBlockEncoder {
    config: MusicAudioMicroBlockConfig,
    fec: DatagramFecEncoder,
    pending: Vec<PendingMusicAudioFrame>,
}

impl Default for MusicAudioMicroBlockEncoder {
    fn default() -> Self {
        Self::new(MusicAudioMicroBlockConfig::default())
    }
}

impl MusicAudioMicroBlockEncoder {
    pub fn new(config: MusicAudioMicroBlockConfig) -> Self {
        let config = config.normalized();
        Self {
            pending: Vec::with_capacity(usize::from(config.frames_per_block)),
            config,
            fec: DatagramFecEncoder::new(),
        }
    }

    pub fn config(&self) -> MusicAudioMicroBlockConfig {
        self.config
    }

    pub fn config_mut(&mut self) -> &mut MusicAudioMicroBlockConfig {
        &mut self.config
    }

    pub fn pending_frames(&self) -> usize {
        self.pending.len()
    }

    pub fn push_frame(
        &mut self,
        frame: MusicAudioFrame<'_>,
    ) -> Result<Option<EncodedMusicAudioMicroBlock>, MusicAudioFecError> {
        self.pending.push(PendingMusicAudioFrame {
            sequence: frame.sequence,
            pts_samples: frame.pts_samples,
            payload: frame.payload.to_vec(),
        });

        if self.pending.len() < usize::from(self.config.normalized().frames_per_block) {
            return Ok(None);
        }

        self.encode_pending().map(Some)
    }

    pub fn flush(&mut self) -> Result<Option<EncodedMusicAudioMicroBlock>, MusicAudioFecError> {
        if self.pending.is_empty() {
            return Ok(None);
        }

        self.encode_pending().map(Some)
    }

    pub fn encode_block(
        &mut self,
        frames: &[MusicAudioFrame<'_>],
    ) -> Result<EncodedMusicAudioMicroBlock, MusicAudioFecError> {
        let config = self.config.normalized();
        if frames.is_empty() {
            return Err(MusicAudioFecError::EmptyBlock);
        }
        if frames.len() > usize::from(config.frames_per_block) {
            return Err(MusicAudioFecError::TooManyFrames {
                actual: frames.len(),
                max: config.frames_per_block,
            });
        }

        let block = encode_music_audio_micro_block_payload(config, frames)?;
        let source_symbols = source_symbol_count(block.len(), config.symbol_size);
        if source_symbols > config.max_source_symbols {
            return Err(MusicAudioFecError::SourceSymbolBudgetExceeded {
                actual: source_symbols,
                max: config.max_source_symbols,
            });
        }

        self.fec.set_source_symbols(source_symbols);
        self.fec.set_symbol_size(config.symbol_size);
        let datagrams = self
            .fec
            .encode_block_with_repair_symbols(&block, config.repair_symbols)?;

        Ok(EncodedMusicAudioMicroBlock {
            stream_id: config.stream_id,
            first_sequence: frames[0].sequence,
            first_pts_samples: frames[0].pts_samples,
            frame_count: frames.len() as u16,
            source_symbols,
            repair_symbols: config.repair_symbols,
            symbol_size: config.symbol_size,
            datagrams,
        })
    }

    fn encode_pending(&mut self) -> Result<EncodedMusicAudioMicroBlock, MusicAudioFecError> {
        let pending = std::mem::take(&mut self.pending);
        let result = {
            let frames: Vec<_> = pending
                .iter()
                .map(|frame| MusicAudioFrame {
                    sequence: frame.sequence,
                    pts_samples: frame.pts_samples,
                    payload: &frame.payload,
                })
                .collect();
            self.encode_block(&frames)
        };

        match result {
            Ok(encoded) => Ok(encoded),
            Err(error) => {
                self.pending = pending;
                Err(error)
            }
        }
    }
}

#[derive(Debug, Clone)]
struct PendingMusicAudioFrame {
    sequence: u64,
    pts_samples: u64,
    payload: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct MusicAudioMicroBlockDecoder {
    fec: DatagramFecDecoder,
}

impl MusicAudioMicroBlockDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_datagram(
        &mut self,
        datagram: &[u8],
    ) -> Result<Option<DecodedMusicAudioMicroBlock>, MusicAudioFecError> {
        let Some(block) = self.fec.push_datagram(datagram)? else {
            return Ok(None);
        };

        decode_music_audio_micro_block_payload(&block).map(Some)
    }
}

fn encode_music_audio_micro_block_payload(
    config: MusicAudioMicroBlockConfig,
    frames: &[MusicAudioFrame<'_>],
) -> Result<Vec<u8>, MusicAudioFecError> {
    let frame_count = frames.len();
    let header_len = MUSIC_AUDIO_BLOCK_FIXED_HEADER_LEN
        .checked_add(
            frame_count
                .checked_mul(MUSIC_AUDIO_FRAME_DESCRIPTOR_LEN)
                .ok_or(MusicAudioFecError::HeaderTooLarge { actual: usize::MAX })?,
        )
        .ok_or(MusicAudioFecError::HeaderTooLarge { actual: usize::MAX })?;
    if header_len > u16::MAX as usize {
        return Err(MusicAudioFecError::HeaderTooLarge { actual: header_len });
    }

    let first_sequence = frames[0].sequence;
    let first_pts_samples = frames[0].pts_samples;
    let mut payload_len = 0usize;
    for frame in frames {
        if frame.payload.len() > u32::MAX as usize {
            return Err(MusicAudioFecError::FramePayloadTooLarge {
                actual: frame.payload.len(),
            });
        }
        payload_len = payload_len
            .checked_add(frame.payload.len())
            .ok_or(MusicAudioFecError::FramePayloadTooLarge { actual: usize::MAX })?;
        sequence_delta(first_sequence, frame.sequence)?;
        pts_delta(first_pts_samples, frame.pts_samples)?;
    }
    if payload_len > u32::MAX as usize {
        return Err(MusicAudioFecError::BlockPayloadTooLarge {
            actual: payload_len,
        });
    }

    let mut block = vec![0; header_len];
    block[0..4].copy_from_slice(&MUSIC_AUDIO_BLOCK_MAGIC);
    block[4] = MUSIC_AUDIO_BLOCK_VERSION;
    block[5] = 0;
    block[6..8].copy_from_slice(&(header_len as u16).to_le_bytes());
    block[8..16].copy_from_slice(&config.stream_id.to_le_bytes());
    block[16..20].copy_from_slice(&config.sample_rate_hz.to_le_bytes());
    block[20..22].copy_from_slice(&config.channels.to_le_bytes());
    block[22..24].copy_from_slice(&(frame_count as u16).to_le_bytes());
    block[24..32].copy_from_slice(&first_sequence.to_le_bytes());
    block[32..40].copy_from_slice(&first_pts_samples.to_le_bytes());
    block[40..44].copy_from_slice(&config.frame_duration_samples.to_le_bytes());
    block[44..48].copy_from_slice(&(payload_len as u32).to_le_bytes());

    for (index, frame) in frames.iter().enumerate() {
        let desc = MUSIC_AUDIO_BLOCK_FIXED_HEADER_LEN + index * MUSIC_AUDIO_FRAME_DESCRIPTOR_LEN;
        let seq_delta = sequence_delta(first_sequence, frame.sequence)?;
        let pts_delta = pts_delta(first_pts_samples, frame.pts_samples)?;
        block[desc..desc + 2].copy_from_slice(&seq_delta.to_le_bytes());
        block[desc + 2..desc + 6].copy_from_slice(&pts_delta.to_le_bytes());
        block[desc + 6..desc + 10].copy_from_slice(&(frame.payload.len() as u32).to_le_bytes());
        block[desc + 10..desc + 12].fill(0);
    }

    block.reserve(payload_len);
    for frame in frames {
        block.extend_from_slice(frame.payload);
    }

    Ok(block)
}

pub fn decode_music_audio_micro_block_payload(
    block: &[u8],
) -> Result<DecodedMusicAudioMicroBlock, MusicAudioFecError> {
    if block.len() < MUSIC_AUDIO_BLOCK_FIXED_HEADER_LEN {
        return Err(MusicAudioFecError::HeaderTooShort {
            actual: block.len(),
        });
    }

    let magic: [u8; 4] = block[0..4].try_into().expect("header length checked");
    if magic != MUSIC_AUDIO_BLOCK_MAGIC {
        return Err(MusicAudioFecError::InvalidMagic { actual: magic });
    }
    let version = block[4];
    if version != MUSIC_AUDIO_BLOCK_VERSION {
        return Err(MusicAudioFecError::UnsupportedVersion(version));
    }

    let header_len = usize::from(u16::from_le_bytes(
        block[6..8].try_into().expect("header length checked"),
    ));
    let stream_id = u64::from_le_bytes(block[8..16].try_into().expect("header length checked"));
    let sample_rate_hz =
        u32::from_le_bytes(block[16..20].try_into().expect("header length checked"));
    let channels = u16::from_le_bytes(block[20..22].try_into().expect("header length checked"));
    let frame_count = usize::from(u16::from_le_bytes(
        block[22..24].try_into().expect("header length checked"),
    ));
    let first_sequence =
        u64::from_le_bytes(block[24..32].try_into().expect("header length checked"));
    let first_pts_samples =
        u64::from_le_bytes(block[32..40].try_into().expect("header length checked"));
    let frame_duration_samples =
        u32::from_le_bytes(block[40..44].try_into().expect("header length checked"));
    let payload_len = usize::try_from(u32::from_le_bytes(
        block[44..48].try_into().expect("header length checked"),
    ))
    .expect("u32 fits usize");

    let expected_header_len = MUSIC_AUDIO_BLOCK_FIXED_HEADER_LEN
        .checked_add(
            frame_count
                .checked_mul(MUSIC_AUDIO_FRAME_DESCRIPTOR_LEN)
                .ok_or(MusicAudioFecError::HeaderTooLarge { actual: usize::MAX })?,
        )
        .ok_or(MusicAudioFecError::HeaderTooLarge { actual: usize::MAX })?;
    if header_len != expected_header_len || block.len() < header_len {
        return Err(MusicAudioFecError::HeaderLengthMismatch {
            expected: expected_header_len,
            actual: header_len,
        });
    }

    let expected_block_len = header_len
        .checked_add(payload_len)
        .ok_or(MusicAudioFecError::BlockPayloadTooLarge { actual: usize::MAX })?;
    if block.len() != expected_block_len {
        return Err(MusicAudioFecError::PayloadLengthMismatch {
            expected: payload_len,
            actual: block.len().saturating_sub(header_len),
        });
    }

    let mut payload_offset = header_len;
    let mut frames = Vec::with_capacity(frame_count);
    for index in 0..frame_count {
        let desc = MUSIC_AUDIO_BLOCK_FIXED_HEADER_LEN + index * MUSIC_AUDIO_FRAME_DESCRIPTOR_LEN;
        let seq_delta = u16::from_le_bytes(
            block[desc..desc + 2]
                .try_into()
                .expect("desc length checked"),
        );
        let pts_delta = u32::from_le_bytes(
            block[desc + 2..desc + 6]
                .try_into()
                .expect("desc length checked"),
        );
        let frame_len = usize::try_from(u32::from_le_bytes(
            block[desc + 6..desc + 10]
                .try_into()
                .expect("desc length checked"),
        ))
        .expect("u32 fits usize");
        let payload_end = payload_offset
            .checked_add(frame_len)
            .ok_or(MusicAudioFecError::FramePayloadTooLarge { actual: usize::MAX })?;
        if payload_end > block.len() {
            return Err(MusicAudioFecError::TruncatedFramePayload {
                expected: frame_len,
                actual: block.len().saturating_sub(payload_offset),
            });
        }

        let sequence = first_sequence.checked_add(u64::from(seq_delta)).ok_or(
            MusicAudioFecError::SequenceOutOfRange {
                first: first_sequence,
                sequence: first_sequence.saturating_add(u64::from(seq_delta)),
            },
        )?;
        let pts_samples = first_pts_samples.checked_add(u64::from(pts_delta)).ok_or(
            MusicAudioFecError::PtsOutOfRange {
                first: first_pts_samples,
                pts_samples: first_pts_samples.saturating_add(u64::from(pts_delta)),
            },
        )?;

        frames.push(DecodedMusicAudioFrame {
            sequence,
            pts_samples,
            payload: Bytes::copy_from_slice(&block[payload_offset..payload_end]),
        });
        payload_offset = payload_end;
    }

    Ok(DecodedMusicAudioMicroBlock {
        stream_id,
        sample_rate_hz,
        channels,
        frame_duration_samples,
        frames,
    })
}

fn sequence_delta(first: u64, sequence: u64) -> Result<u16, MusicAudioFecError> {
    let delta = sequence
        .checked_sub(first)
        .ok_or(MusicAudioFecError::SequenceOutOfRange { first, sequence })?;
    u16::try_from(delta).map_err(|_| MusicAudioFecError::SequenceOutOfRange { first, sequence })
}

fn pts_delta(first: u64, pts_samples: u64) -> Result<u32, MusicAudioFecError> {
    let delta = pts_samples
        .checked_sub(first)
        .ok_or(MusicAudioFecError::PtsOutOfRange { first, pts_samples })?;
    u32::try_from(delta).map_err(|_| MusicAudioFecError::PtsOutOfRange { first, pts_samples })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MusicAudioFecError {
    Fec(DatagramFecError),
    EmptyBlock,
    TooManyFrames { actual: usize, max: u16 },
    HeaderTooShort { actual: usize },
    HeaderTooLarge { actual: usize },
    InvalidMagic { actual: [u8; 4] },
    UnsupportedVersion(u8),
    HeaderLengthMismatch { expected: usize, actual: usize },
    PayloadLengthMismatch { expected: usize, actual: usize },
    FramePayloadTooLarge { actual: usize },
    BlockPayloadTooLarge { actual: usize },
    SourceSymbolBudgetExceeded { actual: u16, max: u16 },
    SequenceOutOfRange { first: u64, sequence: u64 },
    PtsOutOfRange { first: u64, pts_samples: u64 },
    TruncatedFramePayload { expected: usize, actual: usize },
}

impl From<DatagramFecError> for MusicAudioFecError {
    fn from(error: DatagramFecError) -> Self {
        Self::Fec(error)
    }
}

impl fmt::Display for MusicAudioFecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fec(error) => write!(formatter, "{error}"),
            Self::EmptyBlock => write!(formatter, "music audio micro-block has no frames"),
            Self::TooManyFrames { actual, max } => write!(
                formatter,
                "music audio micro-block has too many frames: got {actual}, max is {max}"
            ),
            Self::HeaderTooShort { actual } => write!(
                formatter,
                "music audio micro-block header too short: expected {MUSIC_AUDIO_BLOCK_FIXED_HEADER_LEN}, got {actual}"
            ),
            Self::HeaderTooLarge { actual } => write!(
                formatter,
                "music audio micro-block header too large for compact header: {actual}"
            ),
            Self::InvalidMagic { actual } => write!(
                formatter,
                "invalid music audio micro-block magic: expected {:?}, got {:?}",
                MUSIC_AUDIO_BLOCK_MAGIC, actual
            ),
            Self::UnsupportedVersion(version) => write!(
                formatter,
                "unsupported music audio micro-block version: expected {MUSIC_AUDIO_BLOCK_VERSION}, got {version}"
            ),
            Self::HeaderLengthMismatch { expected, actual } => write!(
                formatter,
                "music audio micro-block header length mismatch: expected {expected}, got {actual}"
            ),
            Self::PayloadLengthMismatch { expected, actual } => write!(
                formatter,
                "music audio micro-block payload length mismatch: expected {expected}, got {actual}"
            ),
            Self::FramePayloadTooLarge { actual } => write!(
                formatter,
                "music audio frame payload too large for compact header: {actual}"
            ),
            Self::BlockPayloadTooLarge { actual } => write!(
                formatter,
                "music audio micro-block payload too large for compact header: {actual}"
            ),
            Self::SourceSymbolBudgetExceeded { actual, max } => write!(
                formatter,
                "music audio micro-block source-symbol budget exceeded: got {actual}, max is {max}"
            ),
            Self::SequenceOutOfRange { first, sequence } => write!(
                formatter,
                "music audio frame sequence out of compact delta range: first={first}, sequence={sequence}"
            ),
            Self::PtsOutOfRange { first, pts_samples } => write!(
                formatter,
                "music audio frame PTS out of compact delta range: first={first}, pts={pts_samples}"
            ),
            Self::TruncatedFramePayload { expected, actual } => write!(
                formatter,
                "music audio frame payload truncated: expected {expected}, got {actual}"
            ),
        }
    }
}

impl std::error::Error for MusicAudioFecError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn micro_block_recovers_exact_frames_after_one_lost_source_datagram() {
        let payloads: Vec<Vec<u8>> = (0..4).map(|seed| test_payload(seed, 960)).collect();
        let frames = music_frames(&payloads, 10, 1_000, 120);
        let mut encoder =
            MusicAudioMicroBlockEncoder::new(MusicAudioMicroBlockConfig::pcm48_stereo_2_5ms(7));

        let encoded = encoder.encode_block(&frames).expect("encode");
        assert_eq!(encoded.source_symbols, 4);
        assert_eq!(encoded.repair_symbols, 1);
        assert_eq!(encoded.datagrams.len(), 5);

        let mut decoder = MusicAudioMicroBlockDecoder::new();
        let mut decoded = None;
        for (index, datagram) in encoded.datagrams.iter().enumerate() {
            if index == 1 {
                continue;
            }
            decoded = decoder.push_datagram(datagram).expect("decode");
            if decoded.is_some() {
                break;
            }
        }

        let decoded = decoded.expect("recovered block");
        assert_eq!(decoded.stream_id, 7);
        assert_eq!(decoded.sample_rate_hz, 48_000);
        assert_eq!(decoded.channels, 2);
        assert_eq!(decoded.frame_duration_samples, 120);
        assert_eq!(decoded.frames.len(), payloads.len());
        for (index, frame) in decoded.frames.iter().enumerate() {
            assert_eq!(frame.sequence, 10 + index as u64);
            assert_eq!(frame.pts_samples, 1_000 + index as u64 * 120);
            assert_eq!(frame.payload.as_ref(), payloads[index].as_slice());
        }
    }

    #[test]
    fn push_frame_emits_at_configured_micro_block_size_and_flushes_partial() {
        let config = MusicAudioMicroBlockConfig::pcm48_stereo_5ms(3);
        let mut encoder = MusicAudioMicroBlockEncoder::new(config);
        let a = test_payload(1, 1440);
        let b = test_payload(2, 1440);
        let c = test_payload(3, 1440);

        assert!(encoder
            .push_frame(MusicAudioFrame {
                sequence: 0,
                pts_samples: 0,
                payload: &a,
            })
            .unwrap()
            .is_none());
        assert_eq!(encoder.pending_frames(), 1);

        let encoded = encoder
            .push_frame(MusicAudioFrame {
                sequence: 1,
                pts_samples: 240,
                payload: &b,
            })
            .unwrap()
            .expect("full micro-block");
        assert_eq!(encoded.frame_count, 2);
        assert_eq!(encoder.pending_frames(), 0);

        assert!(encoder
            .push_frame(MusicAudioFrame {
                sequence: 2,
                pts_samples: 480,
                payload: &c,
            })
            .unwrap()
            .is_none());
        let flushed = encoder.flush().unwrap().expect("partial block");
        assert_eq!(flushed.frame_count, 1);
        assert_eq!(encoder.pending_frames(), 0);
    }

    #[test]
    fn rejects_payloads_that_exceed_micro_block_source_budget() {
        let payloads: Vec<Vec<u8>> = (0..4).map(|seed| test_payload(seed, 1200)).collect();
        let frames = music_frames(&payloads, 0, 0, 120);
        let mut encoder =
            MusicAudioMicroBlockEncoder::new(MusicAudioMicroBlockConfig::pcm48_stereo_2_5ms(1));

        let error = encoder.encode_block(&frames).unwrap_err();
        assert_eq!(
            error,
            MusicAudioFecError::SourceSymbolBudgetExceeded { actual: 5, max: 4 }
        );
    }

    fn music_frames<'a>(
        payloads: &'a [Vec<u8>],
        first_sequence: u64,
        first_pts: u64,
        duration_samples: u64,
    ) -> Vec<MusicAudioFrame<'a>> {
        payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| MusicAudioFrame {
                sequence: first_sequence + index as u64,
                pts_samples: first_pts + index as u64 * duration_samples,
                payload,
            })
            .collect()
    }

    fn test_payload(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| seed.wrapping_add(index as u8))
            .collect()
    }
}
