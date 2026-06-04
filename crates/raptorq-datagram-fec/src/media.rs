use crate::{
    source_symbol_count, AdaptiveFecController, DatagramBufferPool, DatagramFecDecoder,
    DatagramFecEncoder, DatagramFecError, FecDecision, MediaPriority,
};
use bytes::Bytes;
use std::collections::{HashMap, HashSet};
use std::fmt;

pub const MEDIA_FRAME_HEADER_LEN: usize = 44;
const NO_DTS_DELTA_MS: i16 = i16::MIN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum MediaCodec {
    Unknown = 0,
    H264 = 1,
    Opus = 2,
    Aac = 3,
    Data = 255,
}

impl MediaCodec {
    fn from_u8(value: u8) -> Self {
        match value {
            1 => Self::H264,
            2 => Self::Opus,
            3 => Self::Aac,
            255 => Self::Data,
            _ => Self::Unknown,
        }
    }

    fn priority(self, flags: MediaFrameFlags) -> MediaPriority {
        match self {
            Self::Opus | Self::Aac => MediaPriority::Audio,
            Self::H264 if flags.is_keyframe() => MediaPriority::VideoKey,
            Self::H264 => MediaPriority::VideoDelta,
            Self::Unknown | Self::Data => MediaPriority::Data,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MediaFrameFlags(u16);

impl MediaFrameFlags {
    pub const KEYFRAME: u16 = 1 << 0;
    pub const CODEC_CONFIG: u16 = 1 << 1;
    pub const DISCONTINUITY: u16 = 1 << 2;
    pub const END_OF_STREAM: u16 = 1 << 3;

    pub fn new(bits: u16) -> Self {
        Self(bits)
    }

    pub fn keyframe() -> Self {
        Self(Self::KEYFRAME)
    }

    pub fn bits(self) -> u16 {
        self.0
    }

    pub fn is_keyframe(self) -> bool {
        self.0 & Self::KEYFRAME != 0
    }

    pub fn contains(self, bit: u16) -> bool {
        self.0 & bit != 0
    }

    pub fn with(mut self, bit: u16) -> Self {
        self.0 |= bit;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaFrameMetadata {
    pub stream_id: u64,
    pub sequence: u64,
    pub pts_ms: u64,
    pub dts_ms: Option<u64>,
    pub duration_ms: u32,
    pub codec: MediaCodec,
    pub flags: MediaFrameFlags,
}

impl MediaFrameMetadata {
    pub fn new(stream_id: u64, sequence: u64, pts_ms: u64, codec: MediaCodec) -> Self {
        Self {
            stream_id,
            sequence,
            pts_ms,
            dts_ms: None,
            duration_ms: 0,
            codec,
            flags: MediaFrameFlags::default(),
        }
    }

    pub fn priority(self) -> MediaPriority {
        self.codec.priority(self.flags)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaFragmentHeader {
    pub metadata: MediaFrameMetadata,
    pub fragment_index: u16,
    pub fragment_count: u16,
    pub access_unit_len: u32,
    pub fragment_offset: u32,
}

impl MediaFragmentHeader {
    pub fn encode(&self, bytes: &mut [u8]) -> Result<(), MediaFecError> {
        if bytes.len() < MEDIA_FRAME_HEADER_LEN {
            return Err(MediaFecError::HeaderTooShort {
                actual: bytes.len(),
            });
        }
        if self.metadata.flags.bits() > u16::from(u8::MAX) {
            return Err(MediaFecError::FlagsTooLarge {
                actual: self.metadata.flags.bits(),
            });
        }
        if self.metadata.duration_ms > u32::from(u16::MAX) {
            return Err(MediaFecError::DurationTooLarge {
                actual: self.metadata.duration_ms,
            });
        }
        let dts_delta_ms = encode_dts_delta_ms(self.metadata)?;

        bytes[0] = self.metadata.codec as u8;
        bytes[1] = self.metadata.flags.bits() as u8;
        bytes[2..4].copy_from_slice(&self.fragment_index.to_le_bytes());
        bytes[4..6].copy_from_slice(&self.fragment_count.to_le_bytes());
        bytes[6..8].copy_from_slice(&(self.metadata.duration_ms as u16).to_le_bytes());
        bytes[8..16].copy_from_slice(&self.metadata.stream_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.metadata.sequence.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.metadata.pts_ms.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.access_unit_len.to_le_bytes());
        bytes[36..40].copy_from_slice(&self.fragment_offset.to_le_bytes());
        bytes[40..42].copy_from_slice(&dts_delta_ms.to_le_bytes());
        bytes[42..44].fill(0);
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, MediaFecError> {
        if bytes.len() < MEDIA_FRAME_HEADER_LEN {
            return Err(MediaFecError::HeaderTooShort {
                actual: bytes.len(),
            });
        }

        let pts_ms = u64::from_le_bytes(bytes[24..32].try_into().expect("header length checked"));
        let dts_delta_ms =
            i16::from_le_bytes(bytes[40..42].try_into().expect("header length checked"));
        Ok(Self {
            metadata: MediaFrameMetadata {
                codec: MediaCodec::from_u8(bytes[0]),
                flags: MediaFrameFlags::new(u16::from(bytes[1])),
                stream_id: u64::from_le_bytes(
                    bytes[8..16].try_into().expect("header length checked"),
                ),
                sequence: u64::from_le_bytes(
                    bytes[16..24].try_into().expect("header length checked"),
                ),
                pts_ms,
                dts_ms: decode_dts_ms(pts_ms, dts_delta_ms)?,
                duration_ms: u32::from(u16::from_le_bytes(
                    bytes[6..8].try_into().expect("header length checked"),
                )),
            },
            fragment_index: u16::from_le_bytes(
                bytes[2..4].try_into().expect("header length checked"),
            ),
            fragment_count: u16::from_le_bytes(
                bytes[4..6].try_into().expect("header length checked"),
            ),
            access_unit_len: u32::from_le_bytes(
                bytes[32..36].try_into().expect("header length checked"),
            ),
            fragment_offset: u32::from_le_bytes(
                bytes[36..40].try_into().expect("header length checked"),
            ),
        })
    }
}

fn encode_dts_delta_ms(metadata: MediaFrameMetadata) -> Result<i16, MediaFecError> {
    let Some(dts_ms) = metadata.dts_ms else {
        return Ok(NO_DTS_DELTA_MS);
    };
    let delta = dts_ms as i128 - metadata.pts_ms as i128;
    if delta < i128::from(NO_DTS_DELTA_MS + 1) || delta > i128::from(i16::MAX) {
        return Err(MediaFecError::DtsDeltaOutOfRange {
            pts_ms: metadata.pts_ms,
            dts_ms,
        });
    }
    Ok(delta as i16)
}

fn decode_dts_ms(pts_ms: u64, delta_ms: i16) -> Result<Option<u64>, MediaFecError> {
    if delta_ms == NO_DTS_DELTA_MS {
        return Ok(None);
    }
    if delta_ms < 0 {
        pts_ms
            .checked_sub(delta_ms.unsigned_abs().into())
            .map(Some)
            .ok_or(MediaFecError::InvalidDtsDelta { pts_ms, delta_ms })
    } else {
        pts_ms
            .checked_add(delta_ms as u64)
            .map(Some)
            .ok_or(MediaFecError::InvalidDtsDelta { pts_ms, delta_ms })
    }
}

pub struct MediaFrame<'a> {
    pub metadata: MediaFrameMetadata,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedMediaFrame {
    pub metadata: MediaFrameMetadata,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerializedMediaAccessUnit {
    pub metadata: MediaFrameMetadata,
    pub payload: Bytes,
}

pub fn decode_serialized_media_access_unit(
    bytes: Bytes,
) -> Result<Option<SerializedMediaAccessUnit>, String> {
    if bytes.len() < MEDIA_FRAME_HEADER_LEN {
        return Ok(None);
    }
    let Ok(header) = MediaFragmentHeader::decode(&bytes[..MEDIA_FRAME_HEADER_LEN]) else {
        return Ok(None);
    };
    if header.fragment_index != 0 || header.fragment_count != 1 || header.fragment_offset != 0 {
        return Ok(None);
    }

    let payload_len = bytes
        .len()
        .checked_sub(MEDIA_FRAME_HEADER_LEN)
        .ok_or_else(|| "media access-unit frame underflow".to_string())?;
    if header.access_unit_len as usize != payload_len {
        return Err(format!(
            "media access-unit payload length mismatch: header={}, body={payload_len}",
            header.access_unit_len
        ));
    }

    Ok(Some(SerializedMediaAccessUnit {
        metadata: header.metadata,
        payload: bytes.slice(MEDIA_FRAME_HEADER_LEN..),
    }))
}

#[derive(Debug, Clone, PartialEq)]
pub struct EncodedMediaFrame {
    pub sequence: u64,
    pub metadata: MediaFrameMetadata,
    pub priority: MediaPriority,
    pub fragment_count: u16,
    pub decision: FecDecision,
    pub blocks: Vec<EncodedMediaBlock>,
    pub datagrams: Vec<Vec<u8>>,
}

impl EncodedMediaFrame {
    pub fn stats(&self) -> MediaFecFrameStats {
        let source_datagrams = self
            .blocks
            .iter()
            .map(|block| usize::from(block.source_symbols))
            .sum::<usize>();
        let repair_datagrams = self
            .blocks
            .iter()
            .map(|block| block.repair_symbols as usize)
            .sum::<usize>();
        let protected_bytes = self
            .blocks
            .iter()
            .map(|block| block.payload_len)
            .sum::<usize>();
        let wire_bytes = self
            .datagrams
            .iter()
            .map(|datagram| datagram.len())
            .sum::<usize>();

        MediaFecFrameStats {
            block_count: self.blocks.len(),
            source_datagrams,
            repair_datagrams,
            wire_datagrams: self.datagrams.len(),
            protected_bytes,
            wire_bytes,
            recoverable_source_datagrams: self
                .blocks
                .iter()
                .map(|block| block.repair_symbols as usize)
                .sum(),
            max_block_datagrams: self
                .blocks
                .iter()
                .map(|block| block.datagram_count)
                .max()
                .unwrap_or(0),
        }
    }

    pub fn datagram_role(&self, datagram_index: usize) -> Option<MediaDatagramRole> {
        self.blocks
            .iter()
            .find_map(|block| block.datagram_role(datagram_index))
    }

    pub fn source_first_datagram_indices(&self) -> Vec<usize> {
        self.datagram_send_plan(MediaDatagramOrder::SourceFirst)
            .into_iter()
            .map(|entry| entry.datagram_index)
            .collect()
    }

    pub fn datagram_send_plan(&self, order: MediaDatagramOrder) -> Vec<MediaDatagramSend> {
        let mut plan = Vec::with_capacity(self.datagrams.len());
        match order {
            MediaDatagramOrder::Encoded => {
                for block in &self.blocks {
                    push_media_datagram_send_entries(&mut plan, block, MediaDatagramRole::Source);
                    push_media_datagram_send_entries(&mut plan, block, MediaDatagramRole::Repair);
                }
            }
            MediaDatagramOrder::SourceFirst => {
                for block in &self.blocks {
                    push_media_datagram_send_entries(&mut plan, block, MediaDatagramRole::Source);
                }
                for block in &self.blocks {
                    push_media_datagram_send_entries(&mut plan, block, MediaDatagramRole::Repair);
                }
            }
        }
        plan
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaDatagramOrder {
    Encoded,
    SourceFirst,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaDatagramRole {
    Source,
    Repair,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaDatagramSend {
    pub datagram_index: usize,
    pub block_id: u32,
    pub fragment_index: u16,
    pub role: MediaDatagramRole,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MediaFecFrameStats {
    pub block_count: usize,
    pub source_datagrams: usize,
    pub repair_datagrams: usize,
    pub wire_datagrams: usize,
    pub protected_bytes: usize,
    pub wire_bytes: usize,
    pub recoverable_source_datagrams: usize,
    pub max_block_datagrams: usize,
}

impl MediaFecFrameStats {
    pub fn overhead_bytes(self) -> usize {
        self.wire_bytes.saturating_sub(self.protected_bytes)
    }

    pub fn datagram_overhead_ratio(self) -> f32 {
        if self.source_datagrams == 0 {
            0.0
        } else {
            self.wire_datagrams as f32 / self.source_datagrams as f32
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedMediaBlock {
    pub block_id: u32,
    pub fragment_index: u16,
    pub source_symbols: u16,
    pub repair_symbols: u32,
    pub first_datagram_index: usize,
    pub datagram_count: usize,
    pub payload_len: usize,
}

impl EncodedMediaBlock {
    pub fn source_datagram_indices(&self) -> std::ops::Range<usize> {
        self.first_datagram_index
            ..self
                .first_datagram_index
                .saturating_add(usize::from(self.source_symbols))
                .min(
                    self.first_datagram_index
                        .saturating_add(self.datagram_count),
                )
    }

    pub fn repair_datagram_indices(&self) -> std::ops::Range<usize> {
        let repair_start = self
            .first_datagram_index
            .saturating_add(usize::from(self.source_symbols))
            .min(
                self.first_datagram_index
                    .saturating_add(self.datagram_count),
            );
        repair_start
            ..self
                .first_datagram_index
                .saturating_add(self.datagram_count)
    }

    pub fn datagram_role(&self, datagram_index: usize) -> Option<MediaDatagramRole> {
        if self.source_datagram_indices().contains(&datagram_index) {
            Some(MediaDatagramRole::Source)
        } else if self.repair_datagram_indices().contains(&datagram_index) {
            Some(MediaDatagramRole::Repair)
        } else {
            None
        }
    }
}

fn push_media_datagram_send_entries(
    plan: &mut Vec<MediaDatagramSend>,
    block: &EncodedMediaBlock,
    role: MediaDatagramRole,
) {
    let range = match role {
        MediaDatagramRole::Source => block.source_datagram_indices(),
        MediaDatagramRole::Repair => block.repair_datagram_indices(),
    };
    plan.extend(range.map(|datagram_index| MediaDatagramSend {
        datagram_index,
        block_id: block.block_id,
        fragment_index: block.fragment_index,
        role,
    }));
}

#[derive(Debug, Clone)]
pub struct MediaFecEncoder {
    fec: DatagramFecEncoder,
    controller: AdaptiveFecController,
    next_sequence: u64,
}

impl Default for MediaFecEncoder {
    fn default() -> Self {
        Self::new(AdaptiveFecController::default())
    }
}

impl MediaFecEncoder {
    pub fn new(controller: AdaptiveFecController) -> Self {
        Self {
            fec: DatagramFecEncoder::new(),
            controller,
            next_sequence: 0,
        }
    }

    pub fn controller(&self) -> &AdaptiveFecController {
        &self.controller
    }

    pub fn controller_mut(&mut self) -> &mut AdaptiveFecController {
        &mut self.controller
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn allocate_sequence(&mut self) -> u64 {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        sequence
    }

    pub fn encode_frame(
        &mut self,
        frame: MediaFrame<'_>,
    ) -> Result<EncodedMediaFrame, MediaFecError> {
        self.encode_frame_with_optional_pool(frame, None)
    }

    pub fn encode_frame_reusing(
        &mut self,
        frame: MediaFrame<'_>,
        pool: &mut DatagramBufferPool,
    ) -> Result<EncodedMediaFrame, MediaFecError> {
        self.encode_frame_with_optional_pool(frame, Some(pool))
    }

    fn encode_frame_with_optional_pool(
        &mut self,
        frame: MediaFrame<'_>,
        mut pool: Option<&mut DatagramBufferPool>,
    ) -> Result<EncodedMediaFrame, MediaFecError> {
        let priority = frame.metadata.priority();
        let initial_decision = self
            .controller
            .decide(frame.payload.len() + MEDIA_FRAME_HEADER_LEN, priority);
        let max_block_payload = initial_decision.config.max_payload_len();
        let max_fragment_payload = max_block_payload
            .checked_sub(MEDIA_FRAME_HEADER_LEN)
            .filter(|value| *value > 0)
            .ok_or(MediaFecError::BlockTooSmall {
                max_block_payload,
                header_len: MEDIA_FRAME_HEADER_LEN,
            })?;
        let fragment_count = frame.payload.len().div_ceil(max_fragment_payload).max(1);
        if fragment_count > u16::MAX as usize {
            return Err(MediaFecError::TooManyFragments {
                actual: fragment_count,
            });
        }
        if frame.payload.len() > u32::MAX as usize {
            return Err(MediaFecError::AccessUnitTooLarge {
                actual: frame.payload.len(),
            });
        }

        let mut blocks = Vec::new();
        let mut datagrams = Vec::new();
        for fragment_index in 0..fragment_count {
            let start = fragment_index * max_fragment_payload;
            let end = (start + max_fragment_payload).min(frame.payload.len());
            let fragment = if start < end {
                &frame.payload[start..end]
            } else {
                &[]
            };
            let header = MediaFragmentHeader {
                metadata: frame.metadata,
                fragment_index: fragment_index as u16,
                fragment_count: fragment_count as u16,
                access_unit_len: frame.payload.len() as u32,
                fragment_offset: start as u32,
            };

            let mut block = Vec::with_capacity(MEDIA_FRAME_HEADER_LEN + fragment.len());
            block.resize(MEDIA_FRAME_HEADER_LEN, 0);
            header.encode(&mut block[..MEDIA_FRAME_HEADER_LEN])?;
            block.extend_from_slice(fragment);

            let block_source_symbols =
                source_symbol_count(block.len(), initial_decision.config.symbol_size);
            let repair_symbols = self
                .controller
                .repair_symbols_for(block_source_symbols, priority);
            self.fec
                .set_source_symbols(initial_decision.config.source_symbols);
            self.fec
                .set_symbol_size(initial_decision.config.symbol_size);
            let block_id = self.fec.block_id();
            let first_datagram_index = datagrams.len();
            let block_datagrams = if let Some(pool) = pool.as_deref_mut() {
                self.fec
                    .encode_block_with_repair_symbols_reusing(&block, repair_symbols, pool)?
            } else {
                self.fec
                    .encode_block_with_repair_symbols(&block, repair_symbols)?
            };
            let datagram_count = block_datagrams.len();
            datagrams.extend(block_datagrams);
            blocks.push(EncodedMediaBlock {
                block_id,
                fragment_index: fragment_index as u16,
                source_symbols: block_source_symbols,
                repair_symbols,
                first_datagram_index,
                datagram_count,
                payload_len: block.len(),
            });
        }

        Ok(EncodedMediaFrame {
            sequence: frame.metadata.sequence,
            metadata: frame.metadata,
            priority,
            fragment_count: fragment_count as u16,
            decision: initial_decision,
            blocks,
            datagrams,
        })
    }
}

#[derive(Debug, Default)]
pub struct MediaFecDecoder {
    fec: DatagramFecDecoder,
    partial: HashMap<(u64, u64), PartialFrame>,
    completed: HashSet<(u64, u64)>,
}

impl MediaFecDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_datagram(
        &mut self,
        datagram: &[u8],
    ) -> Result<Option<DecodedMediaFrame>, MediaFecError> {
        let Some(block) = self.fec.push_datagram(datagram)? else {
            return Ok(None);
        };
        if block.len() < MEDIA_FRAME_HEADER_LEN {
            return Err(MediaFecError::HeaderTooShort {
                actual: block.len(),
            });
        }

        let header = MediaFragmentHeader::decode(&block[..MEDIA_FRAME_HEADER_LEN])?;
        if header.fragment_count == 0 || header.fragment_index >= header.fragment_count {
            return Err(MediaFecError::InvalidFragmentIndex {
                index: header.fragment_index,
                count: header.fragment_count,
            });
        }
        let fragment = block[MEDIA_FRAME_HEADER_LEN..].to_vec();
        let key = (header.metadata.stream_id, header.metadata.sequence);
        if self.completed.contains(&key) {
            return Ok(None);
        }

        let partial = self.partial.entry(key).or_insert_with(|| {
            PartialFrame::new(
                header.metadata,
                header.fragment_count,
                header.access_unit_len as usize,
            )
        });
        partial.insert(header, fragment)?;

        if !partial.is_complete() {
            return Ok(None);
        }

        let partial = self.partial.remove(&key).expect("entry exists");
        self.completed.insert(key);
        prune_completed(&mut self.completed, key);
        partial.finish().map(Some)
    }
}

#[derive(Debug)]
struct PartialFrame {
    metadata: MediaFrameMetadata,
    access_unit_len: usize,
    fragments: Vec<Option<Vec<u8>>>,
    received: u16,
}

impl PartialFrame {
    fn new(metadata: MediaFrameMetadata, fragment_count: u16, access_unit_len: usize) -> Self {
        Self {
            metadata,
            access_unit_len,
            fragments: vec![None; usize::from(fragment_count)],
            received: 0,
        }
    }

    fn insert(
        &mut self,
        header: MediaFragmentHeader,
        fragment: Vec<u8>,
    ) -> Result<(), MediaFecError> {
        if header.metadata != self.metadata {
            return Err(MediaFecError::ConflictingMetadata);
        }
        if header.access_unit_len as usize != self.access_unit_len {
            return Err(MediaFecError::ConflictingAccessUnitLength);
        }
        let index = usize::from(header.fragment_index);
        if index >= self.fragments.len() {
            return Err(MediaFecError::InvalidFragmentIndex {
                index: header.fragment_index,
                count: self.fragments.len() as u16,
            });
        }
        if self.fragments[index].is_none() {
            self.fragments[index] = Some(fragment);
            self.received = self.received.saturating_add(1);
        }
        Ok(())
    }

    fn is_complete(&self) -> bool {
        usize::from(self.received) == self.fragments.len()
    }

    fn finish(self) -> Result<DecodedMediaFrame, MediaFecError> {
        let mut payload = Vec::with_capacity(self.access_unit_len);
        for fragment in self.fragments {
            let Some(fragment) = fragment else {
                return Err(MediaFecError::IncompleteFrame);
            };
            payload.extend_from_slice(&fragment);
        }
        if payload.len() != self.access_unit_len {
            return Err(MediaFecError::AccessUnitLengthMismatch {
                expected: self.access_unit_len,
                actual: payload.len(),
            });
        }

        Ok(DecodedMediaFrame {
            metadata: self.metadata,
            payload,
        })
    }
}

fn prune_completed(completed: &mut HashSet<(u64, u64)>, current: (u64, u64)) {
    completed.retain(|candidate| {
        candidate.0 != current.0 || current.1 < 128 || candidate.1 >= current.1.saturating_sub(128)
    });
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaFecError {
    Fec(DatagramFecError),
    HeaderTooShort {
        actual: usize,
    },
    FlagsTooLarge {
        actual: u16,
    },
    DurationTooLarge {
        actual: u32,
    },
    DtsDeltaOutOfRange {
        pts_ms: u64,
        dts_ms: u64,
    },
    InvalidDtsDelta {
        pts_ms: u64,
        delta_ms: i16,
    },
    BlockTooSmall {
        max_block_payload: usize,
        header_len: usize,
    },
    TooManyFragments {
        actual: usize,
    },
    AccessUnitTooLarge {
        actual: usize,
    },
    InvalidFragmentIndex {
        index: u16,
        count: u16,
    },
    ConflictingMetadata,
    ConflictingAccessUnitLength,
    AccessUnitLengthMismatch {
        expected: usize,
        actual: usize,
    },
    IncompleteFrame,
}

impl From<DatagramFecError> for MediaFecError {
    fn from(error: DatagramFecError) -> Self {
        Self::Fec(error)
    }
}

impl fmt::Display for MediaFecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fec(error) => write!(formatter, "{error}"),
            Self::HeaderTooShort { actual } => write!(
                formatter,
                "media frame header too short: expected {MEDIA_FRAME_HEADER_LEN}, got {actual}"
            ),
            Self::FlagsTooLarge { actual } => {
                write!(formatter, "media frame flags too large for compact header: {actual}")
            }
            Self::DurationTooLarge { actual } => {
                write!(
                    formatter,
                    "media frame duration too large for compact header: {actual} ms"
                )
            }
            Self::DtsDeltaOutOfRange { pts_ms, dts_ms } => {
                write!(
                    formatter,
                    "media frame DTS delta out of compact header range: pts={pts_ms}, dts={dts_ms}"
                )
            }
            Self::InvalidDtsDelta { pts_ms, delta_ms } => {
                write!(
                    formatter,
                    "invalid media frame DTS delta {delta_ms} for pts={pts_ms}"
                )
            }
            Self::BlockTooSmall {
                max_block_payload,
                header_len,
            } => write!(
                formatter,
                "FEC block payload too small for media header: block {max_block_payload}, header {header_len}"
            ),
            Self::TooManyFragments { actual } => {
                write!(formatter, "too many media frame fragments: {actual}")
            }
            Self::AccessUnitTooLarge { actual } => {
                write!(formatter, "media access unit too large for u32 length: {actual}")
            }
            Self::InvalidFragmentIndex { index, count } => {
                write!(formatter, "invalid media fragment index {index} for count {count}")
            }
            Self::ConflictingMetadata => write!(formatter, "conflicting media frame metadata"),
            Self::ConflictingAccessUnitLength => {
                write!(formatter, "conflicting media access-unit length")
            }
            Self::AccessUnitLengthMismatch { expected, actual } => write!(
                formatter,
                "media access-unit length mismatch: expected {expected}, got {actual}"
            ),
            Self::IncompleteFrame => write!(formatter, "media frame is incomplete"),
        }
    }
}

impl std::error::Error for MediaFecError {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{AdaptiveFecPolicy, CongestionConfig};

    #[test]
    fn media_frame_header_roundtrips_keyframe_metadata() {
        let metadata = MediaFrameMetadata {
            stream_id: u64::from(u32::MAX) + 7,
            sequence: 99,
            pts_ms: 12_345,
            dts_ms: Some(12_000),
            duration_ms: 33,
            codec: MediaCodec::H264,
            flags: MediaFrameFlags::keyframe().with(MediaFrameFlags::CODEC_CONFIG),
        };
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 2,
            fragment_count: 3,
            access_unit_len: 1000,
            fragment_offset: 500,
        };
        let mut bytes = [0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut bytes).expect("encode");

        assert_eq!(MediaFragmentHeader::decode(&bytes).expect("decode"), header);
    }

    #[test]
    fn decodes_serialized_media_access_unit() {
        let metadata = MediaFrameMetadata {
            duration_ms: 20,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(91, 2, 400, MediaCodec::Opus)
        };
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: b"opus-frame".len() as u32,
            fragment_offset: 0,
        };
        let mut bytes = vec![0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut bytes[..]).unwrap();
        bytes.extend_from_slice(b"opus-frame");

        let unit = decode_serialized_media_access_unit(Bytes::from(bytes))
            .unwrap()
            .unwrap();
        assert_eq!(unit.metadata, metadata);
        assert_eq!(unit.payload, Bytes::from_static(b"opus-frame"));
    }

    #[test]
    fn rejects_serialized_media_access_unit_payload_length_mismatch() {
        let metadata = MediaFrameMetadata::new(1, 0, 0, MediaCodec::Data);
        let header = MediaFragmentHeader {
            metadata,
            fragment_index: 0,
            fragment_count: 1,
            access_unit_len: 99,
            fragment_offset: 0,
        };
        let mut bytes = vec![0; MEDIA_FRAME_HEADER_LEN];
        header.encode(&mut bytes[..]).unwrap();
        bytes.extend_from_slice(b"short");

        let error = decode_serialized_media_access_unit(Bytes::from(bytes)).unwrap_err();
        assert!(error.contains("payload length mismatch"));
    }

    #[test]
    fn default_policy_repairs_large_low_loss_delta_frame() {
        let mut encoder = MediaFecEncoder::default();
        let payload = vec![0x31; 18_000];
        let metadata = MediaFrameMetadata {
            duration_ms: 16,
            ..MediaFrameMetadata::new(1, encoder.allocate_sequence(), 1_000, MediaCodec::H264)
        };
        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode media");

        assert_eq!(encoded.priority, MediaPriority::VideoDelta);
        assert_eq!(encoded.fragment_count, 1);
        assert_eq!(encoded.blocks.len(), 1);
        assert_eq!(
            encoded.blocks[0].repair_symbols, 1,
            "large low-loss deltas should get a bounded repair floor"
        );

        let dropped_source = encoded.blocks[0].source_datagram_indices().start;
        let mut decoder = MediaFecDecoder::new();
        let mut decoded = None;
        for (index, datagram) in encoded.datagrams.iter().enumerate() {
            if index == dropped_source {
                continue;
            }
            decoded = decoder.push_datagram(datagram).expect("decode media");
            if decoded.is_some() {
                break;
            }
        }

        let decoded = decoded.expect("complete frame");
        assert_eq!(decoded.metadata, metadata);
        assert_eq!(decoded.payload, payload);
    }

    #[test]
    fn media_encoder_can_reuse_datagram_buffers() {
        let mut encoder = MediaFecEncoder::default();
        let mut pool = DatagramBufferPool::new();
        let payload = vec![0x44; 4_000];
        let metadata = MediaFrameMetadata {
            duration_ms: 16,
            ..MediaFrameMetadata::new(1, encoder.allocate_sequence(), 1_000, MediaCodec::H264)
        };
        let encoded = encoder
            .encode_frame_reusing(
                MediaFrame {
                    metadata,
                    payload: &payload,
                },
                &mut pool,
            )
            .expect("encode media");
        let first_ptr = encoded.datagrams[0].as_ptr();
        pool.recycle_many(encoded.datagrams);

        let metadata = MediaFrameMetadata {
            duration_ms: 16,
            ..MediaFrameMetadata::new(1, encoder.allocate_sequence(), 1_016, MediaCodec::H264)
        };
        let encoded = encoder
            .encode_frame_reusing(
                MediaFrame {
                    metadata,
                    payload: &payload,
                },
                &mut pool,
            )
            .expect("encode media with recycled buffers");

        assert_eq!(encoded.datagrams[0].as_ptr(), first_ptr);
    }

    #[test]
    fn source_first_plan_prioritizes_block_fill_before_repair() {
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
        let mut encoder = MediaFecEncoder::new(controller);
        let payload = vec![0xC7; 1500];
        let metadata = MediaFrameMetadata {
            codec: MediaCodec::H264,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(1, encoder.allocate_sequence(), 777, MediaCodec::H264)
        };
        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode media");
        let stats = encoded.stats();

        assert!(stats.block_count > 1);
        assert!(stats.repair_datagrams > 0);
        assert_eq!(stats.wire_datagrams, encoded.datagrams.len());
        assert_eq!(
            stats.source_datagrams + stats.repair_datagrams,
            encoded.datagrams.len()
        );
        assert!(stats.overhead_bytes() > 0);
        assert!(stats.datagram_overhead_ratio() > 1.0);

        let encoded_order = encoded
            .datagram_send_plan(MediaDatagramOrder::Encoded)
            .into_iter()
            .map(|entry| entry.datagram_index)
            .collect::<Vec<_>>();
        let source_first = encoded.datagram_send_plan(MediaDatagramOrder::SourceFirst);
        let source_first_indices = encoded.source_first_datagram_indices();

        assert_eq!(
            encoded_order,
            (0..encoded.datagrams.len()).collect::<Vec<_>>()
        );
        assert_eq!(
            source_first_indices,
            source_first
                .iter()
                .map(|entry| entry.datagram_index)
                .collect::<Vec<_>>()
        );
        assert_ne!(
            source_first_indices, encoded_order,
            "multi-block frames should be able to send later source symbols before earlier repair"
        );

        let mut sorted = source_first_indices.clone();
        sorted.sort_unstable();
        assert_eq!(sorted, encoded_order);

        for entry in source_first.iter().take(stats.source_datagrams) {
            assert_eq!(entry.role, MediaDatagramRole::Source);
            assert_eq!(
                encoded.datagram_role(entry.datagram_index),
                Some(MediaDatagramRole::Source)
            );
        }
        for entry in source_first.iter().skip(stats.source_datagrams) {
            assert_eq!(entry.role, MediaDatagramRole::Repair);
            assert_eq!(
                encoded.datagram_role(entry.datagram_index),
                Some(MediaDatagramRole::Repair)
            );
        }
    }

    #[test]
    fn fragmented_media_frame_roundtrips_with_missing_source_symbol() {
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
        let mut encoder = MediaFecEncoder::new(controller);
        let payload = vec![0xA5; 1500];
        let metadata = MediaFrameMetadata {
            codec: MediaCodec::H264,
            flags: MediaFrameFlags::keyframe(),
            ..MediaFrameMetadata::new(1, encoder.allocate_sequence(), 777, MediaCodec::H264)
        };
        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode media");
        assert!(encoded.fragment_count > 1);
        assert_eq!(encoded.blocks.len(), usize::from(encoded.fragment_count));
        for (fragment_index, block) in encoded.blocks.iter().enumerate() {
            assert_eq!(block.fragment_index, fragment_index as u16);
            assert!(block.source_symbols >= 1);
            assert!(block.source_symbols <= policy.max_source_symbols);
            assert!(block.repair_symbols >= 1);
            assert_eq!(
                block.source_datagram_indices().count(),
                usize::from(block.source_symbols)
            );
            assert_eq!(
                block.repair_datagram_indices().count(),
                block.repair_symbols as usize
            );
            assert_eq!(
                block.datagram_count,
                usize::from(block.source_symbols) + block.repair_symbols as usize
            );
            assert!(
                block.payload_len
                    <= policy.max_source_symbols as usize * policy.symbol_size as usize
            );
        }

        let mut decoder = MediaFecDecoder::new();
        let mut decoded = None;
        for (index, datagram) in encoded.datagrams.iter().enumerate() {
            if index == 1 {
                continue;
            }
            decoded = decoder.push_datagram(datagram).expect("decode media");
            if decoded.is_some() {
                break;
            }
        }

        let decoded = decoded.expect("complete frame");
        assert_eq!(decoded.metadata, metadata);
        assert_eq!(decoded.payload, payload);
    }
}
