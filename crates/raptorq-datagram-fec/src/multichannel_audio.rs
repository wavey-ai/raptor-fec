//! Same-epoch multichannel audio packetization over the existing RaptorQ core.
//!
//! One RaptorQ source block contains only channel/stem fragments that share a
//! playout timestamp. Each fixed-size source symbol is independently
//! self-describing, so systematic packets can be delivered immediately while
//! repair packets continue through the normal RaptorQ decoder.

use crate::{
    decode_header, DatagramFecDecoder, DatagramFecEncoder, DatagramFecError, SequenceStats,
    ENCODING_PACKET_HEADER_LEN, HEADER_LEN,
};
use bytes::Bytes;
use raptorq::EncodingPacket;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt;

pub const MULTICHANNEL_AUDIO_SHARD_MAGIC: [u8; 4] = *b"MAE1";
pub const MULTICHANNEL_AUDIO_SHARD_VERSION: u8 = 1;
pub const MULTICHANNEL_AUDIO_SHARD_HEADER_LEN: usize = 72;
pub const DEFAULT_AUDIO_MAX_DATAGRAM_SIZE: usize = 1200;
pub const DEFAULT_AUDIO_MAX_SOURCE_SYMBOLS: u16 = 256;
pub const DEFAULT_AUDIO_REPAIR_SYMBOLS: u32 = 4;
const DEFAULT_COMPLETED_AUDIO_BLOCKS: usize = 128;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AudioPayloadKind {
    Pcm = 1,
    Opus = 2,
    Flac = 3,
}

impl TryFrom<u8> for AudioPayloadKind {
    type Error = MultichannelAudioFecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            1 => Ok(Self::Pcm),
            2 => Ok(Self::Opus),
            3 => Ok(Self::Flac),
            actual => Err(MultichannelAudioFecError::UnsupportedPayloadKind(actual)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum AudioSampleFormat {
    Unspecified = 0,
    S16Le = 1,
    S24Le = 2,
    S32Le = 3,
    F32Le = 4,
}

impl TryFrom<u8> for AudioSampleFormat {
    type Error = MultichannelAudioFecError;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(Self::Unspecified),
            1 => Ok(Self::S16Le),
            2 => Ok(Self::S24Le),
            3 => Ok(Self::S32Le),
            4 => Ok(Self::F32Le),
            actual => Err(MultichannelAudioFecError::UnsupportedSampleFormat(actual)),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultichannelAudioFecConfig {
    /// Maximum complete transport message, including any transport prefix.
    pub max_datagram_size: usize,
    /// Bytes added outside RQD2, such as the optional WebTransport stream id.
    pub transport_overhead: usize,
    pub repair_symbols: u32,
    pub max_source_symbols: u16,
}

impl Default for MultichannelAudioFecConfig {
    fn default() -> Self {
        Self {
            max_datagram_size: DEFAULT_AUDIO_MAX_DATAGRAM_SIZE,
            transport_overhead: 0,
            repair_symbols: DEFAULT_AUDIO_REPAIR_SYMBOLS,
            max_source_symbols: DEFAULT_AUDIO_MAX_SOURCE_SYMBOLS,
        }
    }
}

impl MultichannelAudioFecConfig {
    fn framing_size(self) -> Result<usize, MultichannelAudioFecError> {
        self.transport_overhead
            .checked_add(HEADER_LEN + ENCODING_PACKET_HEADER_LEN)
            .ok_or(MultichannelAudioFecError::DatagramBudgetTooSmall {
                max_datagram_size: self.max_datagram_size,
                required: usize::MAX,
            })
    }

    pub fn symbol_size(self) -> Result<u16, MultichannelAudioFecError> {
        let framing = self.framing_size()?;
        let required = framing + MULTICHANNEL_AUDIO_SHARD_HEADER_LEN + 1;
        if self.max_datagram_size < required {
            return Err(MultichannelAudioFecError::DatagramBudgetTooSmall {
                max_datagram_size: self.max_datagram_size,
                required,
            });
        }
        let symbol_size = self.max_datagram_size - framing;
        u16::try_from(symbol_size).map_err(|_| MultichannelAudioFecError::SymbolSizeTooLarge {
            actual: symbol_size,
        })
    }

    pub fn max_fragment_payload(self) -> Result<usize, MultichannelAudioFecError> {
        Ok(usize::from(self.symbol_size()?) - MULTICHANNEL_AUDIO_SHARD_HEADER_LEN)
    }

    /// Selects a fixed RaptorQ symbol size for this epoch that minimizes source
    /// bytes on the wire. The transport MTU is a ceiling, not a reason to pad
    /// every short channel-group tail to the largest possible symbol.
    pub fn geometry_for_groups(
        self,
        groups: &[MultichannelAudioGroup<'_>],
    ) -> Result<MultichannelAudioFecGeometry, MultichannelAudioFecError> {
        if groups.is_empty() {
            return Err(MultichannelAudioFecError::EmptyEpoch);
        }
        let framing_size = self.framing_size()?;
        let max_payload = self.max_fragment_payload()?;
        let mut candidates = BTreeSet::from([max_payload]);
        let max_source_symbols = usize::from(self.max_source_symbols);

        for group in groups {
            let payload_len = group.payload.len().max(1);
            let minimum_fragments = payload_len.div_ceil(max_payload);
            for fragments in minimum_fragments..=max_source_symbols {
                let candidate = payload_len.div_ceil(fragments);
                if candidate <= max_payload {
                    candidates.insert(candidate.max(1));
                }
            }
        }

        let mut best: Option<(usize, usize, usize)> = None;
        for fragment_payload in candidates {
            let source_symbols = groups.iter().try_fold(0usize, |total, group| {
                total.checked_add(group.payload.len().max(1).div_ceil(fragment_payload))
            });
            let Some(source_symbols) = source_symbols else {
                continue;
            };
            if source_symbols == 0
                || source_symbols > max_source_symbols
                || source_symbols > u16::MAX as usize
            {
                continue;
            }
            let datagram_size =
                framing_size + MULTICHANNEL_AUDIO_SHARD_HEADER_LEN + fragment_payload;
            let source_wire_bytes = source_symbols.saturating_mul(datagram_size);
            let score = (source_wire_bytes, source_symbols, fragment_payload);
            if best.is_none_or(|current| score < current) {
                best = Some(score);
            }
        }

        let Some((source_wire_bytes, source_symbols, fragment_payload)) = best else {
            let minimum = groups
                .iter()
                .map(|group| group.payload.len().max(1).div_ceil(max_payload))
                .sum();
            return Err(MultichannelAudioFecError::TooManySourceSymbols {
                actual: minimum,
                max: self.max_source_symbols,
            });
        };
        let symbol_size = MULTICHANNEL_AUDIO_SHARD_HEADER_LEN + fragment_payload;
        Ok(MultichannelAudioFecGeometry {
            symbol_size: symbol_size as u16,
            fragment_payload,
            source_symbols: source_symbols as u16,
            source_wire_bytes,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultichannelAudioFecGeometry {
    pub symbol_size: u16,
    pub fragment_payload: usize,
    pub source_symbols: u16,
    pub source_wire_bytes: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultichannelAudioGroup<'a> {
    pub group_id: u16,
    pub channel_start: u16,
    pub channel_count: u16,
    pub payload_kind: AudioPayloadKind,
    pub sample_format: AudioSampleFormat,
    pub flags: u8,
    pub payload: &'a [u8],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultichannelAudioEpoch<'a> {
    pub session_id: u64,
    pub config_generation: u32,
    pub epoch_id: u64,
    pub pts_samples: u64,
    pub sample_rate: u32,
    pub frame_count: u32,
    pub groups: &'a [MultichannelAudioGroup<'a>],
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultichannelAudioShardHeader {
    pub payload_kind: AudioPayloadKind,
    pub sample_format: AudioSampleFormat,
    pub flags: u8,
    pub session_id: u64,
    pub config_generation: u32,
    pub sample_rate: u32,
    pub frame_count: u32,
    pub epoch_id: u64,
    pub pts_samples: u64,
    pub group_id: u16,
    pub group_index: u16,
    pub group_count: u16,
    pub channel_start: u16,
    pub channel_count: u16,
    pub fragment_index: u16,
    pub fragment_count: u16,
    pub source_index: u16,
    pub source_count: u16,
    pub payload_len: u16,
    pub payload_offset: u32,
    pub group_payload_len: u32,
}

impl MultichannelAudioShardHeader {
    pub fn encode(self, symbol: &mut [u8]) -> Result<(), MultichannelAudioFecError> {
        if symbol.len() < MULTICHANNEL_AUDIO_SHARD_HEADER_LEN {
            return Err(MultichannelAudioFecError::ShardTooShort {
                actual: symbol.len(),
            });
        }
        self.validate(symbol.len())?;

        symbol[..MULTICHANNEL_AUDIO_SHARD_HEADER_LEN].fill(0);
        symbol[0..4].copy_from_slice(&MULTICHANNEL_AUDIO_SHARD_MAGIC);
        symbol[4] = MULTICHANNEL_AUDIO_SHARD_VERSION;
        symbol[5] = self.payload_kind as u8;
        symbol[6] = self.sample_format as u8;
        symbol[7] = self.flags;
        symbol[8..16].copy_from_slice(&self.session_id.to_le_bytes());
        symbol[16..20].copy_from_slice(&self.config_generation.to_le_bytes());
        symbol[20..24].copy_from_slice(&self.frame_count.to_le_bytes());
        symbol[24..32].copy_from_slice(&self.epoch_id.to_le_bytes());
        symbol[32..40].copy_from_slice(&self.pts_samples.to_le_bytes());
        symbol[40..42].copy_from_slice(&self.group_id.to_le_bytes());
        symbol[42..44].copy_from_slice(&self.group_index.to_le_bytes());
        symbol[44..46].copy_from_slice(&self.group_count.to_le_bytes());
        symbol[46..48].copy_from_slice(&self.channel_start.to_le_bytes());
        symbol[48..50].copy_from_slice(&self.channel_count.to_le_bytes());
        symbol[50..52].copy_from_slice(&self.fragment_index.to_le_bytes());
        symbol[52..54].copy_from_slice(&self.fragment_count.to_le_bytes());
        symbol[54..56].copy_from_slice(&self.source_index.to_le_bytes());
        symbol[56..58].copy_from_slice(&self.source_count.to_le_bytes());
        symbol[58..60].copy_from_slice(&self.payload_len.to_le_bytes());
        symbol[60..64].copy_from_slice(&self.payload_offset.to_le_bytes());
        symbol[64..68].copy_from_slice(&self.group_payload_len.to_le_bytes());
        symbol[68..72].copy_from_slice(&self.sample_rate.to_le_bytes());
        Ok(())
    }

    pub fn decode(symbol: &[u8]) -> Result<Self, MultichannelAudioFecError> {
        if symbol.len() < MULTICHANNEL_AUDIO_SHARD_HEADER_LEN {
            return Err(MultichannelAudioFecError::ShardTooShort {
                actual: symbol.len(),
            });
        }
        let magic: [u8; 4] = symbol[0..4].try_into().expect("shard length checked");
        if magic != MULTICHANNEL_AUDIO_SHARD_MAGIC {
            return Err(MultichannelAudioFecError::InvalidShardMagic { actual: magic });
        }
        if symbol[4] != MULTICHANNEL_AUDIO_SHARD_VERSION {
            return Err(MultichannelAudioFecError::UnsupportedShardVersion(
                symbol[4],
            ));
        }

        let header = Self {
            payload_kind: AudioPayloadKind::try_from(symbol[5])?,
            sample_format: AudioSampleFormat::try_from(symbol[6])?,
            flags: symbol[7],
            session_id: u64::from_le_bytes(symbol[8..16].try_into().unwrap()),
            config_generation: u32::from_le_bytes(symbol[16..20].try_into().unwrap()),
            sample_rate: u32::from_le_bytes(symbol[68..72].try_into().unwrap()),
            frame_count: u32::from_le_bytes(symbol[20..24].try_into().unwrap()),
            epoch_id: u64::from_le_bytes(symbol[24..32].try_into().unwrap()),
            pts_samples: u64::from_le_bytes(symbol[32..40].try_into().unwrap()),
            group_id: u16::from_le_bytes(symbol[40..42].try_into().unwrap()),
            group_index: u16::from_le_bytes(symbol[42..44].try_into().unwrap()),
            group_count: u16::from_le_bytes(symbol[44..46].try_into().unwrap()),
            channel_start: u16::from_le_bytes(symbol[46..48].try_into().unwrap()),
            channel_count: u16::from_le_bytes(symbol[48..50].try_into().unwrap()),
            fragment_index: u16::from_le_bytes(symbol[50..52].try_into().unwrap()),
            fragment_count: u16::from_le_bytes(symbol[52..54].try_into().unwrap()),
            source_index: u16::from_le_bytes(symbol[54..56].try_into().unwrap()),
            source_count: u16::from_le_bytes(symbol[56..58].try_into().unwrap()),
            payload_len: u16::from_le_bytes(symbol[58..60].try_into().unwrap()),
            payload_offset: u32::from_le_bytes(symbol[60..64].try_into().unwrap()),
            group_payload_len: u32::from_le_bytes(symbol[64..68].try_into().unwrap()),
        };
        header.validate(symbol.len())?;
        Ok(header)
    }

    pub fn payload(self, symbol: &[u8]) -> Result<&[u8], MultichannelAudioFecError> {
        self.validate(symbol.len())?;
        let start = MULTICHANNEL_AUDIO_SHARD_HEADER_LEN;
        let end = start + usize::from(self.payload_len);
        Ok(&symbol[start..end])
    }

    fn validate(self, symbol_len: usize) -> Result<(), MultichannelAudioFecError> {
        if self.frame_count == 0 {
            return Err(MultichannelAudioFecError::InvalidFrameCount);
        }
        if self.sample_rate == 0 {
            return Err(MultichannelAudioFecError::InvalidSampleRate);
        }
        if matches!(self.payload_kind, AudioPayloadKind::Flac)
            && matches!(self.sample_format, AudioSampleFormat::Unspecified)
        {
            return Err(MultichannelAudioFecError::FlacFormatRequired {
                group_id: self.group_id,
            });
        }
        if self.group_count == 0 || self.group_index >= self.group_count {
            return Err(MultichannelAudioFecError::InvalidGroupIndex {
                index: self.group_index,
                count: self.group_count,
            });
        }
        if self.channel_count == 0 {
            return Err(MultichannelAudioFecError::InvalidChannelCount {
                group_id: self.group_id,
            });
        }
        if self.fragment_count == 0 || self.fragment_index >= self.fragment_count {
            return Err(MultichannelAudioFecError::InvalidFragmentIndex {
                index: self.fragment_index,
                count: self.fragment_count,
            });
        }
        if self.source_count == 0 || self.source_index >= self.source_count {
            return Err(MultichannelAudioFecError::InvalidSourceIndex {
                index: self.source_index,
                count: self.source_count,
            });
        }
        let payload_end = usize::try_from(self.payload_offset)
            .unwrap_or(usize::MAX)
            .saturating_add(usize::from(self.payload_len));
        if payload_end > self.group_payload_len as usize {
            return Err(MultichannelAudioFecError::InvalidPayloadRange {
                offset: self.payload_offset,
                len: self.payload_len,
                total: self.group_payload_len,
            });
        }
        if MULTICHANNEL_AUDIO_SHARD_HEADER_LEN + usize::from(self.payload_len) > symbol_len {
            return Err(MultichannelAudioFecError::ShardPayloadTooLong {
                actual: usize::from(self.payload_len),
                max: symbol_len.saturating_sub(MULTICHANNEL_AUDIO_SHARD_HEADER_LEN),
            });
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultichannelAudioDatagramRole {
    Source { source_index: u16 },
    Repair { encoding_symbol_id: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MultichannelAudioDatagram {
    pub block_id: u32,
    pub packet_sequence: u32,
    pub role: MultichannelAudioDatagramRole,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedMultichannelAudioEpoch {
    pub session_id: u64,
    pub config_generation: u32,
    pub epoch_id: u64,
    pub pts_samples: u64,
    pub sample_rate: u32,
    pub frame_count: u32,
    pub block_id: u32,
    pub source_symbols: u16,
    pub repair_symbols: u32,
    pub symbol_size: u16,
    pub datagrams: Vec<MultichannelAudioDatagram>,
}

impl EncodedMultichannelAudioEpoch {
    pub fn source_datagram_count(&self) -> usize {
        self.datagrams
            .iter()
            .filter(|packet| matches!(packet.role, MultichannelAudioDatagramRole::Source { .. }))
            .count()
    }

    pub fn repair_datagram_count(&self) -> usize {
        self.datagrams.len() - self.source_datagram_count()
    }
}

#[derive(Debug, Clone)]
pub struct MultichannelAudioFecEncoder {
    config: MultichannelAudioFecConfig,
    fec: DatagramFecEncoder,
}

impl MultichannelAudioFecEncoder {
    pub fn new(config: MultichannelAudioFecConfig) -> Self {
        Self {
            config,
            fec: DatagramFecEncoder::new(),
        }
    }

    /// Select the first FEC block identifier used by this encoder.
    ///
    /// Callers that multiplex independent contributors should provide
    /// independently selected namespaces before encoding the first epoch.
    pub fn with_initial_block_id(mut self, block_id: u32) -> Self {
        self.fec.set_block_id(block_id);
        self
    }

    /// Set the FEC block identifier used by the next encoded epoch.
    pub fn set_block_id(&mut self, block_id: u32) {
        self.fec.set_block_id(block_id);
    }

    /// Return the FEC block identifier that will be used by the next epoch.
    pub fn block_id(&self) -> u32 {
        self.fec.block_id()
    }

    pub fn config(&self) -> MultichannelAudioFecConfig {
        self.config
    }

    pub fn config_mut(&mut self) -> &mut MultichannelAudioFecConfig {
        &mut self.config
    }

    pub fn encode_epoch(
        &mut self,
        epoch: MultichannelAudioEpoch<'_>,
    ) -> Result<EncodedMultichannelAudioEpoch, MultichannelAudioFecError> {
        if epoch.frame_count == 0 {
            return Err(MultichannelAudioFecError::InvalidFrameCount);
        }
        if epoch.sample_rate == 0 {
            return Err(MultichannelAudioFecError::InvalidSampleRate);
        }
        if epoch.groups.is_empty() {
            return Err(MultichannelAudioFecError::EmptyEpoch);
        }
        let group_count = u16::try_from(epoch.groups.len()).map_err(|_| {
            MultichannelAudioFecError::TooManyGroups {
                actual: epoch.groups.len(),
            }
        })?;
        for group in epoch.groups {
            validate_group(group)?;
            if group.payload.len() > u32::MAX as usize {
                return Err(MultichannelAudioFecError::GroupPayloadTooLong {
                    group_id: group.group_id,
                    actual: group.payload.len(),
                });
            }
        }
        let geometry = self.config.geometry_for_groups(epoch.groups)?;
        let symbol_size = geometry.symbol_size;
        let fragment_payload = geometry.fragment_payload;
        let source_count_u16 = geometry.source_symbols;
        let source_count = usize::from(source_count_u16);
        let mut object = vec![0u8; source_count * usize::from(symbol_size)];
        let mut source_index = 0usize;

        for (group_index, group) in epoch.groups.iter().enumerate() {
            let fragment_count = group.payload.len().max(1).div_ceil(fragment_payload);
            let fragment_count_u16 = u16::try_from(fragment_count).map_err(|_| {
                MultichannelAudioFecError::TooManyFragments {
                    group_id: group.group_id,
                    actual: fragment_count,
                }
            })?;

            for fragment_index in 0..fragment_count {
                let payload_offset = fragment_index * fragment_payload;
                let payload_end = (payload_offset + fragment_payload).min(group.payload.len());
                let payload = if payload_offset < group.payload.len() {
                    &group.payload[payload_offset..payload_end]
                } else {
                    &[]
                };
                let symbol_start = source_index * usize::from(symbol_size);
                let symbol_end = symbol_start + usize::from(symbol_size);
                let symbol = &mut object[symbol_start..symbol_end];
                let header = MultichannelAudioShardHeader {
                    payload_kind: group.payload_kind,
                    sample_format: group.sample_format,
                    flags: group.flags,
                    session_id: epoch.session_id,
                    config_generation: epoch.config_generation,
                    sample_rate: epoch.sample_rate,
                    frame_count: epoch.frame_count,
                    epoch_id: epoch.epoch_id,
                    pts_samples: epoch.pts_samples,
                    group_id: group.group_id,
                    group_index: group_index as u16,
                    group_count,
                    channel_start: group.channel_start,
                    channel_count: group.channel_count,
                    fragment_index: fragment_index as u16,
                    fragment_count: fragment_count_u16,
                    source_index: source_index as u16,
                    source_count: source_count_u16,
                    payload_len: payload.len() as u16,
                    payload_offset: payload_offset as u32,
                    group_payload_len: group.payload.len() as u32,
                };
                header.encode(symbol)?;
                let payload_start = MULTICHANNEL_AUDIO_SHARD_HEADER_LEN;
                symbol[payload_start..payload_start + payload.len()].copy_from_slice(payload);
                source_index += 1;
            }
        }

        let block_id = self.fec.block_id();
        self.fec.set_source_symbols(source_count_u16);
        self.fec.set_symbol_size(symbol_size);
        let encoded = self
            .fec
            .encode_block_with_repair_symbols(&object, self.config.repair_symbols)?;
        let mut sources = Vec::with_capacity(source_count);
        let mut repairs = Vec::with_capacity(self.config.repair_symbols as usize);

        for datagram in encoded {
            if datagram.len() + self.config.transport_overhead > self.config.max_datagram_size {
                return Err(MultichannelAudioFecError::EncodedDatagramTooLarge {
                    actual: datagram.len() + self.config.transport_overhead,
                    max: self.config.max_datagram_size,
                });
            }
            let fec_header = decode_header(&datagram)?;
            let packet = EncodingPacket::deserialize(fec_header.payload(&datagram)?);
            let encoding_symbol_id = packet.payload_id().encoding_symbol_id();
            let role = if encoding_symbol_id < u32::from(source_count_u16) {
                MultichannelAudioDatagramRole::Source {
                    source_index: encoding_symbol_id as u16,
                }
            } else {
                MultichannelAudioDatagramRole::Repair { encoding_symbol_id }
            };
            let output = MultichannelAudioDatagram {
                block_id,
                packet_sequence: fec_header.packet_sequence,
                role,
                payload: Bytes::from(datagram),
            };
            match role {
                MultichannelAudioDatagramRole::Source { .. } => sources.push(output),
                MultichannelAudioDatagramRole::Repair { .. } => repairs.push(output),
            }
        }
        sources.sort_by_key(|packet| match packet.role {
            MultichannelAudioDatagramRole::Source { source_index } => source_index,
            MultichannelAudioDatagramRole::Repair { .. } => u16::MAX,
        });
        sources.extend(repairs);

        Ok(EncodedMultichannelAudioEpoch {
            session_id: epoch.session_id,
            config_generation: epoch.config_generation,
            epoch_id: epoch.epoch_id,
            pts_samples: epoch.pts_samples,
            sample_rate: epoch.sample_rate,
            frame_count: epoch.frame_count,
            block_id,
            source_symbols: source_count_u16,
            repair_symbols: self.config.repair_symbols,
            symbol_size,
            datagrams: sources,
        })
    }
}

fn validate_group(group: &MultichannelAudioGroup<'_>) -> Result<(), MultichannelAudioFecError> {
    if group.channel_count == 0 {
        return Err(MultichannelAudioFecError::InvalidChannelCount {
            group_id: group.group_id,
        });
    }
    if matches!(group.payload_kind, AudioPayloadKind::Pcm)
        && matches!(group.sample_format, AudioSampleFormat::Unspecified)
    {
        return Err(MultichannelAudioFecError::PcmFormatRequired {
            group_id: group.group_id,
        });
    }
    if matches!(group.payload_kind, AudioPayloadKind::Flac)
        && matches!(group.sample_format, AudioSampleFormat::Unspecified)
    {
        return Err(MultichannelAudioFecError::FlacFormatRequired {
            group_id: group.group_id,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MultichannelAudioRecovery {
    Systematic,
    RaptorQ,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedMultichannelAudioShard {
    pub block_id: u32,
    pub header: MultichannelAudioShardHeader,
    pub recovery: MultichannelAudioRecovery,
    pub payload: Bytes,
}

#[derive(Debug)]
pub struct MultichannelAudioFecDecoder {
    fec: DatagramFecDecoder,
    delivered: HashMap<u32, HashSet<u16>>,
    completed: HashSet<u32>,
    completed_order: VecDeque<u32>,
    max_completed_blocks: usize,
}

impl Default for MultichannelAudioFecDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl MultichannelAudioFecDecoder {
    pub fn new() -> Self {
        Self {
            fec: DatagramFecDecoder::new(),
            delivered: HashMap::new(),
            completed: HashSet::new(),
            completed_order: VecDeque::new(),
            max_completed_blocks: DEFAULT_COMPLETED_AUDIO_BLOCKS,
        }
    }

    pub fn with_completed_window(mut self, max_completed_blocks: usize) -> Self {
        self.max_completed_blocks = max_completed_blocks.max(1);
        self
    }

    pub fn push_datagram(
        &mut self,
        datagram: &[u8],
    ) -> Result<Vec<DecodedMultichannelAudioShard>, MultichannelAudioFecError> {
        let fec_header = decode_header(datagram)?;
        if self.completed.contains(&fec_header.block_id) {
            return Ok(Vec::new());
        }
        let serialized = fec_header.payload(datagram)?;
        if serialized.len() < ENCODING_PACKET_HEADER_LEN {
            return Err(MultichannelAudioFecError::Fec(
                DatagramFecError::PacketTooShort {
                    actual: datagram.len(),
                },
            ));
        }
        let packet = EncodingPacket::deserialize(serialized);
        let esi = packet.payload_id().encoding_symbol_id();
        let source_candidate = if esi < u32::from(fec_header.source_symbols) {
            Some(decode_symbol(
                fec_header.block_id,
                packet.data(),
                fec_header.source_symbols,
                esi as u16,
                MultichannelAudioRecovery::Systematic,
            )?)
        } else {
            None
        };

        let completed_object = self.fec.push_datagram(datagram)?;
        let delivered = self.delivered.entry(fec_header.block_id).or_default();
        let mut output = Vec::new();
        if let Some(source) = source_candidate {
            if delivered.insert(source.header.source_index) {
                output.push(source);
            }
        }

        if let Some(object) = completed_object {
            let symbol_size = usize::from(fec_header.symbol_size);
            let expected_len = symbol_size * usize::from(fec_header.source_symbols);
            if object.len() != expected_len {
                return Err(MultichannelAudioFecError::DecodedObjectLengthMismatch {
                    expected: expected_len,
                    actual: object.len(),
                });
            }
            for (source_index, symbol) in object.chunks_exact(symbol_size).enumerate() {
                let source_index = source_index as u16;
                if delivered.contains(&source_index) {
                    continue;
                }
                let shard = decode_symbol(
                    fec_header.block_id,
                    symbol,
                    fec_header.source_symbols,
                    source_index,
                    MultichannelAudioRecovery::RaptorQ,
                )?;
                delivered.insert(source_index);
                output.push(shard);
            }
            self.mark_completed(fec_header.block_id);
        }

        output.sort_by_key(|shard| shard.header.source_index);
        Ok(output)
    }

    pub fn sequence_stats(&self) -> SequenceStats {
        self.fec.sequence_stats()
    }

    fn mark_completed(&mut self, block_id: u32) {
        if self.completed.insert(block_id) {
            self.completed_order.push_back(block_id);
        }
        while self.completed_order.len() > self.max_completed_blocks {
            if let Some(expired) = self.completed_order.pop_front() {
                self.completed.remove(&expired);
                self.delivered.remove(&expired);
            }
        }
    }
}

fn decode_symbol(
    block_id: u32,
    symbol: &[u8],
    expected_source_count: u16,
    expected_source_index: u16,
    recovery: MultichannelAudioRecovery,
) -> Result<DecodedMultichannelAudioShard, MultichannelAudioFecError> {
    let header = MultichannelAudioShardHeader::decode(symbol)?;
    if header.source_count != expected_source_count || header.source_index != expected_source_index
    {
        return Err(MultichannelAudioFecError::SourceIdentityMismatch {
            expected_index: expected_source_index,
            actual_index: header.source_index,
            expected_count: expected_source_count,
            actual_count: header.source_count,
        });
    }
    Ok(DecodedMultichannelAudioShard {
        block_id,
        header,
        recovery,
        payload: Bytes::copy_from_slice(header.payload(symbol)?),
    })
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultichannelAudioFecError {
    Fec(DatagramFecError),
    DatagramBudgetTooSmall {
        max_datagram_size: usize,
        required: usize,
    },
    SymbolSizeTooLarge {
        actual: usize,
    },
    EmptyEpoch,
    InvalidFrameCount,
    InvalidSampleRate,
    TooManyGroups {
        actual: usize,
    },
    InvalidChannelCount {
        group_id: u16,
    },
    PcmFormatRequired {
        group_id: u16,
    },
    FlacFormatRequired {
        group_id: u16,
    },
    GroupPayloadTooLong {
        group_id: u16,
        actual: usize,
    },
    TooManyFragments {
        group_id: u16,
        actual: usize,
    },
    TooManySourceSymbols {
        actual: usize,
        max: u16,
    },
    EncodedDatagramTooLarge {
        actual: usize,
        max: usize,
    },
    ShardTooShort {
        actual: usize,
    },
    InvalidShardMagic {
        actual: [u8; 4],
    },
    UnsupportedShardVersion(u8),
    UnsupportedPayloadKind(u8),
    UnsupportedSampleFormat(u8),
    InvalidGroupIndex {
        index: u16,
        count: u16,
    },
    InvalidFragmentIndex {
        index: u16,
        count: u16,
    },
    InvalidSourceIndex {
        index: u16,
        count: u16,
    },
    InvalidPayloadRange {
        offset: u32,
        len: u16,
        total: u32,
    },
    ShardPayloadTooLong {
        actual: usize,
        max: usize,
    },
    SourceIdentityMismatch {
        expected_index: u16,
        actual_index: u16,
        expected_count: u16,
        actual_count: u16,
    },
    DecodedObjectLengthMismatch {
        expected: usize,
        actual: usize,
    },
}

impl From<DatagramFecError> for MultichannelAudioFecError {
    fn from(error: DatagramFecError) -> Self {
        Self::Fec(error)
    }
}

impl fmt::Display for MultichannelAudioFecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fec(error) => write!(formatter, "{error}"),
            Self::DatagramBudgetTooSmall {
                max_datagram_size,
                required,
            } => write!(
                formatter,
                "audio datagram budget {max_datagram_size} is smaller than required {required}"
            ),
            Self::SymbolSizeTooLarge { actual } => {
                write!(formatter, "audio RaptorQ symbol size {actual} exceeds u16")
            }
            Self::EmptyEpoch => write!(formatter, "multichannel audio epoch has no groups"),
            Self::InvalidFrameCount => write!(formatter, "audio epoch frame count must be nonzero"),
            Self::InvalidSampleRate => write!(formatter, "audio epoch sample rate must be nonzero"),
            Self::TooManyGroups { actual } => {
                write!(formatter, "audio epoch has too many groups: {actual}")
            }
            Self::InvalidChannelCount { group_id } => {
                write!(formatter, "audio group {group_id} has no channels")
            }
            Self::PcmFormatRequired { group_id } => {
                write!(formatter, "PCM audio group {group_id} requires a sample format")
            }
            Self::FlacFormatRequired { group_id } => write!(
                formatter,
                "FLAC audio group {group_id} requires a decoded sample format"
            ),
            Self::GroupPayloadTooLong { group_id, actual } => write!(
                formatter,
                "audio group {group_id} payload is too long: {actual} bytes"
            ),
            Self::TooManyFragments { group_id, actual } => write!(
                formatter,
                "audio group {group_id} requires too many fragments: {actual}"
            ),
            Self::TooManySourceSymbols { actual, max } => write!(
                formatter,
                "audio epoch requires {actual} source symbols, configured maximum is {max}"
            ),
            Self::EncodedDatagramTooLarge { actual, max } => write!(
                formatter,
                "encoded audio datagram is {actual} bytes, transport maximum is {max}"
            ),
            Self::ShardTooShort { actual } => write!(
                formatter,
                "multichannel audio shard header is too short: expected {MULTICHANNEL_AUDIO_SHARD_HEADER_LEN}, got {actual}"
            ),
            Self::InvalidShardMagic { actual } => write!(
                formatter,
                "invalid multichannel audio shard magic: {actual:?}"
            ),
            Self::UnsupportedShardVersion(version) => {
                write!(formatter, "unsupported multichannel audio shard version {version}")
            }
            Self::UnsupportedPayloadKind(kind) => {
                write!(formatter, "unsupported multichannel audio payload kind {kind}")
            }
            Self::UnsupportedSampleFormat(format) => {
                write!(formatter, "unsupported multichannel audio sample format {format}")
            }
            Self::InvalidGroupIndex { index, count } => {
                write!(formatter, "invalid audio group index {index} of {count}")
            }
            Self::InvalidFragmentIndex { index, count } => {
                write!(formatter, "invalid audio fragment index {index} of {count}")
            }
            Self::InvalidSourceIndex { index, count } => {
                write!(formatter, "invalid audio source index {index} of {count}")
            }
            Self::InvalidPayloadRange { offset, len, total } => write!(
                formatter,
                "invalid audio payload range {offset}+{len} for group length {total}"
            ),
            Self::ShardPayloadTooLong { actual, max } => write!(
                formatter,
                "audio shard payload is {actual} bytes, symbol capacity is {max}"
            ),
            Self::SourceIdentityMismatch {
                expected_index,
                actual_index,
                expected_count,
                actual_count,
            } => write!(
                formatter,
                "audio source identity mismatch: expected {expected_index}/{expected_count}, got {actual_index}/{actual_count}"
            ),
            Self::DecodedObjectLengthMismatch { expected, actual } => write!(
                formatter,
                "decoded audio object length mismatch: expected {expected}, got {actual}"
            ),
        }
    }
}

impl std::error::Error for MultichannelAudioFecError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn pcm_group<'a>(
        group_id: u16,
        channel_start: u16,
        channels: u16,
        payload: &'a [u8],
    ) -> MultichannelAudioGroup<'a> {
        MultichannelAudioGroup {
            group_id,
            channel_start,
            channel_count: channels,
            payload_kind: AudioPayloadKind::Pcm,
            sample_format: AudioSampleFormat::S24Le,
            flags: 0,
            payload,
        }
    }

    #[test]
    fn multichannel_epoch_is_mtu_safe_and_source_first() {
        let pcm = vec![0x5a; 128 * 240 * 3];
        let groups = [pcm_group(0, 0, 128, &pcm)];
        let mut encoder = MultichannelAudioFecEncoder::new(MultichannelAudioFecConfig {
            repair_symbols: 8,
            ..MultichannelAudioFecConfig::default()
        });
        let encoded = encoder
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 7,
                config_generation: 2,
                epoch_id: 11,
                pts_samples: 2_640,
                sample_rate: 48_000,
                frame_count: 240,
                groups: &groups,
            })
            .expect("encode epoch");

        assert!(encoded.source_symbols > 64);
        assert_eq!(
            encoded.source_datagram_count(),
            encoded.source_symbols as usize
        );
        assert_eq!(encoded.repair_datagram_count(), 8);
        assert!(encoded
            .datagrams
            .iter()
            .all(|packet| packet.payload.len() <= DEFAULT_AUDIO_MAX_DATAGRAM_SIZE));
        assert!(encoded
            .datagrams
            .iter()
            .take(encoded.source_symbols as usize)
            .all(|packet| matches!(packet.role, MultichannelAudioDatagramRole::Source { .. })));
    }

    #[test]
    fn systematic_shards_are_delivered_before_block_completion() {
        let a = vec![1; 400];
        let b = vec![2; 400];
        let groups = [pcm_group(10, 0, 2, &a), pcm_group(11, 2, 2, &b)];
        let mut encoder = MultichannelAudioFecEncoder::new(MultichannelAudioFecConfig::default());
        let encoded = encoder
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 9,
                config_generation: 1,
                epoch_id: 1,
                pts_samples: 0,
                sample_rate: 48_000,
                frame_count: 240,
                groups: &groups,
            })
            .unwrap();
        assert_eq!(encoded.source_symbols, 2);

        let mut decoder = MultichannelAudioFecDecoder::new();
        let first = decoder
            .push_datagram(&encoded.datagrams[0].payload)
            .expect("decode source");
        assert_eq!(first.len(), 1);
        assert_eq!(first[0].header.group_id, 10);
        assert_eq!(first[0].recovery, MultichannelAudioRecovery::Systematic);
        assert_eq!(first[0].payload.as_ref(), a.as_slice());
    }

    #[test]
    fn raptorq_recovers_missing_same_epoch_groups_exactly() {
        let payloads: Vec<Vec<u8>> = (0..24)
            .map(|index| vec![index as u8; 300 + index * 3])
            .collect();
        let groups: Vec<_> = payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| pcm_group(index as u16, (index * 2) as u16, 2, payload))
            .collect();
        let mut encoder = MultichannelAudioFecEncoder::new(MultichannelAudioFecConfig {
            repair_symbols: 6,
            ..MultichannelAudioFecConfig::default()
        });
        let encoded = encoder
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 44,
                config_generation: 3,
                epoch_id: 98,
                pts_samples: 23_520,
                sample_rate: 48_000,
                frame_count: 240,
                groups: &groups,
            })
            .unwrap();
        let dropped = [2u16, 7, 19];
        let mut decoder = MultichannelAudioFecDecoder::new();
        let mut decoded = HashMap::new();

        for packet in &encoded.datagrams {
            if matches!(packet.role, MultichannelAudioDatagramRole::Source { source_index } if dropped.contains(&source_index))
            {
                continue;
            }
            for shard in decoder.push_datagram(&packet.payload).unwrap() {
                decoded.insert(shard.header.group_id, shard);
            }
        }

        assert_eq!(decoded.len(), groups.len());
        for (group_id, expected) in payloads.iter().enumerate() {
            let actual = decoded.get(&(group_id as u16)).expect("decoded group");
            assert_eq!(actual.payload.as_ref(), expected.as_slice());
            if dropped.contains(&(group_id as u16)) {
                assert_eq!(actual.recovery, MultichannelAudioRecovery::RaptorQ);
            }
        }
    }

    #[test]
    fn transport_prefix_is_included_in_datagram_budget() {
        let config = MultichannelAudioFecConfig {
            max_datagram_size: 1200,
            transport_overhead: 8,
            ..MultichannelAudioFecConfig::default()
        };
        assert_eq!(
            crate::datagram_size_for_symbol_size(config.symbol_size().unwrap()) + 8,
            1200
        );
    }

    #[test]
    fn stereo_stem_geometry_avoids_padding_every_tail_to_the_mtu() {
        let payloads = (0..64).map(|_| vec![0u8; 1_440]).collect::<Vec<_>>();
        let groups = payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| pcm_group(index as u16, index as u16 * 2, 2, payload))
            .collect::<Vec<_>>();
        let config = MultichannelAudioFecConfig {
            max_datagram_size: 1200,
            transport_overhead: 4,
            ..MultichannelAudioFecConfig::default()
        };

        let geometry = config.geometry_for_groups(&groups).unwrap();

        assert_eq!(geometry.fragment_payload, 720);
        assert_eq!(geometry.symbol_size, 792);
        assert_eq!(geometry.source_symbols, 128);
        assert_eq!(geometry.source_wire_bytes, 128 * 832);
    }
}
