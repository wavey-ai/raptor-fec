//! RaptorQ forward-error-correction framing for low-latency datagrams.
//!
//! The crate keeps the wire protocol intentionally small: every datagram starts
//! with a self-identifying v2 header followed by a serialized RaptorQ
//! `EncodingPacket`.

mod adaptive;
mod backfill;
mod media;
mod multichannel_audio;
mod schedule;
mod sequence;
mod telemetry;

pub use adaptive::{
    AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig, CongestionDecision, FecDecision,
    MediaPriority, NetworkMetrics, NetworkMetricsObservation,
};
pub use backfill::{
    MediaBackfillDatagram, MediaBackfillFrame, MediaBackfillKey, MediaBackfillRequest,
    MediaBackfillResponse, MediaBackfillStore,
};
pub use media::{
    decode_serialized_media_access_unit, DecodedMediaFrame, EncodedMediaBlock, EncodedMediaFrame,
    MediaCodec, MediaDatagramOrder, MediaDatagramRole, MediaDatagramSend, MediaFecDecoder,
    MediaFecEncoder, MediaFecError, MediaFecFrameStats, MediaFragmentHeader, MediaFrame,
    MediaFrameFlags, MediaFrameMetadata, SerializedMediaAccessUnit, MEDIA_FRAME_HEADER_LEN,
};
pub use multichannel_audio::{
    inspect_multichannel_audio_datagram, AudioPayloadKind, AudioSampleFormat,
    DecodedMultichannelAudioShard, EncodedMultichannelAudioEpoch, MultichannelAudioDatagram,
    MultichannelAudioDatagramIdentity, MultichannelAudioDatagramRole, MultichannelAudioEpoch,
    MultichannelAudioFecConfig, MultichannelAudioFecDecoder, MultichannelAudioFecEncoder,
    MultichannelAudioFecError, MultichannelAudioFecGeometry, MultichannelAudioGroup,
    MultichannelAudioRecovery, MultichannelAudioShardHeader, DEFAULT_AUDIO_MAX_DATAGRAM_SIZE,
    DEFAULT_AUDIO_MAX_SOURCE_SYMBOLS, DEFAULT_AUDIO_REPAIR_SYMBOLS,
    MULTICHANNEL_AUDIO_SHARD_HEADER_LEN, MULTICHANNEL_AUDIO_SHARD_MAGIC,
    MULTICHANNEL_AUDIO_SHARD_VERSION,
};
pub use schedule::{
    decide_media_recovery, plan_media_datagrams, plan_media_datagrams_with_deadlines,
    MediaDatagramClass, MediaDeadline, MediaDeadlineOutcome, MediaDropReason, MediaDroppedDatagram,
    MediaFrameSchedule, MediaObjectKind, MediaPathIntent, MediaQueueState, MediaRecoveryAction,
    MediaRecoveryDecision, MediaRecoveryInput, MediaRecoveryPolicy, MediaScheduleState,
    MediaScheduledDatagram, MediaSendPlan, MediaSendPolicy, DEFAULT_MAX_DATAGRAMS_PER_PLAN,
    HARD_MAX_BLOCK_VISITS_PER_PLAN, HARD_MAX_DATAGRAMS_PER_PLAN, HARD_MAX_EXTRA_REPAIR_SYMBOLS,
    HARD_MAX_FRAMES_SCANNED_PER_PLAN, URGENT_REPAIR_WINDOW_US,
};
pub use sequence::{SequenceObservation, SequenceStats, SequenceTracker};
pub use telemetry::{MediaFecLossOutcome, MediaFecRepairCounters};

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use std::collections::{HashMap, HashSet};
use std::fmt;

#[cfg(feature = "udp")]
use std::net::SocketAddr;

#[cfg(feature = "udp")]
use tokio::net::UdpSocket;

/// Four-byte magic prefix carried by every Wavey RaptorQ datagram.
pub const DATAGRAM_MAGIC: [u8; 4] = *b"RQD2";
/// Current Wavey RaptorQ datagram wire version.
pub const DATAGRAM_VERSION: u8 = 2;
/// RaptorQ encoding-packet datagram kind.
pub const DATAGRAM_KIND_RAPTORQ: u8 = 1;
/// Packet CRC32 is present and must verify against the header prefix and payload.
pub const DATAGRAM_FLAG_PACKET_CRC32: u8 = 1 << 0;
/// An eight-byte little-endian audio session id follows the RaptorQ payload.
pub const DATAGRAM_FLAG_AUDIO_SESSION_ID: u8 = 1 << 1;
pub const DATAGRAM_AUDIO_SESSION_ID_LEN: usize = 8;
const SUPPORTED_PACKET_FLAGS: u8 = DATAGRAM_FLAG_PACKET_CRC32 | DATAGRAM_FLAG_AUDIO_SESSION_ID;
/// Bytes in the per-datagram header.
pub const HEADER_LEN: usize = 32;
/// Bytes in RaptorQ's serialized encoding-packet header.
pub const ENCODING_PACKET_HEADER_LEN: usize = 4;
/// Default symbol size, chosen to fit typical Ethernet MTU after IP/UDP headers.
pub const DEFAULT_SYMBOL_SIZE: u16 = 1316;
/// Default source symbols per application block.
pub const DEFAULT_SOURCE_SYMBOLS: u16 = 4;
/// Default repair symbols emitted for each block.
pub const DEFAULT_REPAIR_SYMBOLS: u32 = 1;
/// RFC 6330 maximum number of source symbols in one RaptorQ source block.
pub const MAX_SOURCE_SYMBOLS_PER_BLOCK: u32 = 56_403;
/// RaptorQ payload IDs carry a 24-bit Encoding Symbol ID.
pub const RAPTORQ_ENCODING_SYMBOL_ID_LIMIT: u32 = 1 << 24;
/// Absolute canonical-envelope bound admitted by the on-demand repair encoder.
/// This covers a 16 MiB media payload plus the bounded media-object v1 envelope.
pub const HARD_MAX_REPAIR_SOURCE_BYTES: usize = 16 * 1024 * 1024 + 256 * 1024;
/// Absolute allocation bound for one additional-repair response.
pub const HARD_MAX_ADDITIONAL_REPAIR_BYTES: usize = 16 * 1024 * 1024;
/// Number of completed block ids retained for duplicate suppression.
pub const COMPLETED_WINDOW: u32 = 64;

/// Encoder configuration for one protected application block.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramFecConfig {
    pub source_symbols: u16,
    pub repair_symbols: u32,
    pub symbol_size: u16,
}

impl Default for DatagramFecConfig {
    fn default() -> Self {
        Self {
            source_symbols: DEFAULT_SOURCE_SYMBOLS,
            repair_symbols: DEFAULT_REPAIR_SYMBOLS,
            symbol_size: DEFAULT_SYMBOL_SIZE,
        }
    }
}

impl DatagramFecConfig {
    pub fn max_payload_len(self) -> usize {
        usize::from(self.source_symbols.max(1)) * usize::from(self.symbol_size.max(1))
    }

    pub fn datagram_size(self) -> usize {
        datagram_size_for_symbol_size(self.symbol_size)
    }
}

#[derive(Debug, Default)]
pub struct DatagramBufferPool {
    buffers: Vec<Vec<u8>>,
}

impl DatagramBufferPool {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_capacity(buffer_count: usize) -> Self {
        Self {
            buffers: Vec::with_capacity(buffer_count),
        }
    }

    pub fn available(&self) -> usize {
        self.buffers.len()
    }

    pub fn take(&mut self, min_capacity: usize) -> Vec<u8> {
        let mut best_index = None;
        let mut best_capacity = usize::MAX;
        for (index, buffer) in self.buffers.iter().enumerate() {
            let capacity = buffer.capacity();
            if capacity >= min_capacity && capacity < best_capacity {
                best_index = Some(index);
                best_capacity = capacity;
            }
        }

        if let Some(index) = best_index {
            let mut buffer = self.buffers.swap_remove(index);
            buffer.clear();
            buffer
        } else {
            Vec::with_capacity(min_capacity)
        }
    }

    pub fn recycle(&mut self, mut buffer: Vec<u8>) {
        buffer.clear();
        self.buffers.push(buffer);
    }

    pub fn recycle_many<I>(&mut self, buffers: I)
    where
        I: IntoIterator<Item = Vec<u8>>,
    {
        for buffer in buffers {
            self.recycle(buffer);
        }
    }
}

/// The v2 prefix carried by every encoded datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramFecHeader {
    pub packet_kind: u8,
    pub packet_flags: u8,
    pub block_id: u32,
    pub transfer_length: u32,
    pub packet_sequence: u32,
    pub source_symbols: u16,
    pub symbol_size: u16,
    pub payload_len: u32,
    pub packet_crc32: u32,
}

impl DatagramFecHeader {
    pub fn raptorq(
        block_id: u32,
        transfer_length: u32,
        packet_sequence: u32,
        source_symbols: u16,
        symbol_size: u16,
        payload: &[u8],
    ) -> Result<Self, DatagramFecError> {
        if payload.len() > u32::MAX as usize {
            return Err(DatagramFecError::PayloadTooLong {
                actual: payload.len(),
            });
        }

        let mut header = Self {
            packet_kind: DATAGRAM_KIND_RAPTORQ,
            packet_flags: DATAGRAM_FLAG_PACKET_CRC32,
            block_id,
            transfer_length,
            packet_sequence,
            source_symbols,
            symbol_size,
            payload_len: payload.len() as u32,
            packet_crc32: 0,
        };
        header.packet_crc32 = header.compute_packet_crc32(payload)?;
        Ok(header)
    }

    pub fn encode(&self, bytes: &mut [u8]) -> Result<(), DatagramFecError> {
        if bytes.len() < HEADER_LEN {
            return Err(DatagramFecError::HeaderTooShort {
                actual: bytes.len(),
            });
        }
        self.validate_fields()?;

        self.encode_prefix(bytes)?;
        bytes[28..32].copy_from_slice(&self.packet_crc32.to_le_bytes());
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DatagramFecError> {
        if bytes.len() < HEADER_LEN {
            return Err(DatagramFecError::HeaderTooShort {
                actual: bytes.len(),
            });
        }

        let magic: [u8; 4] = bytes[0..4].try_into().expect("header length checked");
        if magic != DATAGRAM_MAGIC {
            return Err(DatagramFecError::InvalidMagic { actual: magic });
        }

        let version = bytes[4];
        if version != DATAGRAM_VERSION {
            return Err(DatagramFecError::UnsupportedVersion(version));
        }

        let header_len = bytes[5];
        if usize::from(header_len) != HEADER_LEN {
            return Err(DatagramFecError::UnsupportedHeaderLength(header_len));
        }

        let packet_kind = bytes[6];
        let packet_flags = bytes[7];
        let source_symbols =
            u16::from_le_bytes(bytes[20..22].try_into().expect("header length checked"));
        let symbol_size =
            u16::from_le_bytes(bytes[22..24].try_into().expect("header length checked"));

        let header = Self {
            packet_kind,
            packet_flags,
            block_id: u32::from_le_bytes(bytes[8..12].try_into().expect("header length checked")),
            transfer_length: u32::from_le_bytes(
                bytes[12..16].try_into().expect("header length checked"),
            ),
            packet_sequence: u32::from_le_bytes(
                bytes[16..20].try_into().expect("header length checked"),
            ),
            source_symbols,
            symbol_size,
            payload_len: u32::from_le_bytes(
                bytes[24..28].try_into().expect("header length checked"),
            ),
            packet_crc32: u32::from_le_bytes(
                bytes[28..32].try_into().expect("header length checked"),
            ),
        };
        header.validate_fields()?;
        Ok(header)
    }

    pub fn payload<'a>(&self, datagram: &'a [u8]) -> Result<&'a [u8], DatagramFecError> {
        if datagram.len() < HEADER_LEN {
            return Err(DatagramFecError::HeaderTooShort {
                actual: datagram.len(),
            });
        }

        let expected = self.payload_len as usize + self.payload_extension_len();
        let actual = datagram.len() - HEADER_LEN;
        if actual != expected {
            return Err(DatagramFecError::PayloadLengthMismatch { expected, actual });
        }

        let wire_payload = &datagram[HEADER_LEN..];
        if self.packet_flags & DATAGRAM_FLAG_PACKET_CRC32 != 0 {
            let actual_crc32 = self.compute_packet_crc32(wire_payload)?;
            if actual_crc32 != self.packet_crc32 {
                return Err(DatagramFecError::PacketCrc32Mismatch {
                    expected: self.packet_crc32,
                    actual: actual_crc32,
                });
            }
        }

        Ok(&wire_payload[..self.payload_len as usize])
    }

    pub fn audio_session_id(&self, datagram: &[u8]) -> Result<Option<u64>, DatagramFecError> {
        let _ = self.payload(datagram)?;
        if self.packet_flags & DATAGRAM_FLAG_AUDIO_SESSION_ID == 0 {
            return Ok(None);
        }
        let start = HEADER_LEN + self.payload_len as usize;
        Ok(Some(u64::from_le_bytes(
            datagram[start..start + DATAGRAM_AUDIO_SESSION_ID_LEN]
                .try_into()
                .expect("validated audio session trailer length"),
        )))
    }

    pub fn compute_packet_crc32(&self, payload: &[u8]) -> Result<u32, DatagramFecError> {
        let expected_len = self.payload_len as usize + self.payload_extension_len();
        if payload.len() != expected_len {
            return Err(DatagramFecError::PayloadLengthMismatch {
                expected: expected_len,
                actual: payload.len(),
            });
        }

        let mut prefix = [0u8; HEADER_LEN - 4];
        self.encode_prefix(&mut prefix)?;
        Ok(packet_crc32(&prefix, payload))
    }

    pub fn datagram_size(&self) -> usize {
        datagram_size_for_symbol_size(self.symbol_size) + self.payload_extension_len()
    }

    const fn payload_extension_len(&self) -> usize {
        if self.packet_flags & DATAGRAM_FLAG_AUDIO_SESSION_ID != 0 {
            DATAGRAM_AUDIO_SESSION_ID_LEN
        } else {
            0
        }
    }

    /// Return the stable block identity and OTI geometry carried by this packet.
    pub fn block_profile(&self) -> Result<RaptorQBlockProfile, DatagramFecError> {
        RaptorQBlockProfile::from_header(*self)
    }

    fn oti(&self) -> ObjectTransmissionInformation {
        exact_one_block_oti(self.transfer_length as u64, self.symbol_size)
    }

    fn encode_prefix(&self, bytes: &mut [u8]) -> Result<(), DatagramFecError> {
        if bytes.len() < HEADER_LEN - 4 {
            return Err(DatagramFecError::HeaderTooShort {
                actual: bytes.len(),
            });
        }
        self.validate_fields()?;

        bytes[0..4].copy_from_slice(&DATAGRAM_MAGIC);
        bytes[4] = DATAGRAM_VERSION;
        bytes[5] = HEADER_LEN as u8;
        bytes[6] = self.packet_kind;
        bytes[7] = self.packet_flags;
        bytes[8..12].copy_from_slice(&self.block_id.to_le_bytes());
        bytes[12..16].copy_from_slice(&self.transfer_length.to_le_bytes());
        bytes[16..20].copy_from_slice(&self.packet_sequence.to_le_bytes());
        bytes[20..22].copy_from_slice(&self.source_symbols.to_le_bytes());
        bytes[22..24].copy_from_slice(&self.symbol_size.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.payload_len.to_le_bytes());
        Ok(())
    }

    fn validate_fields(&self) -> Result<(), DatagramFecError> {
        if self.packet_kind != DATAGRAM_KIND_RAPTORQ {
            return Err(DatagramFecError::UnsupportedPacketKind(self.packet_kind));
        }
        if self.packet_flags & !SUPPORTED_PACKET_FLAGS != 0 {
            return Err(DatagramFecError::UnsupportedPacketFlags(self.packet_flags));
        }
        validate_one_block_geometry(self.transfer_length, self.source_symbols, self.symbol_size)
    }
}

/// Stable RQD2 block namespace and exact one-block RaptorQ OTI geometry.
///
/// A reliable object announcement can carry these four values so independent
/// parents regenerate symbols in the same RFC 6330 namespace. The caller is
/// still responsible for binding the profile to an authenticated immutable
/// object identity and payload hash.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RaptorQBlockProfile {
    block_id: u32,
    transfer_length: u32,
    source_symbols: u16,
    symbol_size: u16,
}

impl RaptorQBlockProfile {
    pub fn new(
        block_id: u32,
        transfer_length: u32,
        source_symbols: u16,
        symbol_size: u16,
    ) -> Result<Self, DatagramFecError> {
        validate_one_block_geometry(transfer_length, source_symbols, symbol_size)?;
        Ok(Self {
            block_id,
            transfer_length,
            source_symbols,
            symbol_size,
        })
    }

    pub fn from_header(header: DatagramFecHeader) -> Result<Self, DatagramFecError> {
        header.validate_fields()?;
        Self::new(
            header.block_id,
            header.transfer_length,
            header.source_symbols,
            header.symbol_size,
        )
    }

    /// Parse and validate a complete RQD2 source or repair datagram.
    pub fn from_datagram(datagram: &[u8]) -> Result<Self, DatagramFecError> {
        if datagram.len() < HEADER_LEN + ENCODING_PACKET_HEADER_LEN {
            return Err(DatagramFecError::PacketTooShort {
                actual: datagram.len(),
            });
        }
        let header = DatagramFecHeader::decode(datagram)?;
        let payload = header.payload(datagram)?;
        if payload.len() < ENCODING_PACKET_HEADER_LEN {
            return Err(DatagramFecError::PacketTooShort {
                actual: datagram.len(),
            });
        }
        let packet = EncodingPacket::deserialize(payload);
        let source_block_number = packet.payload_id().source_block_number();
        if source_block_number != 0 {
            return Err(DatagramFecError::UnsupportedSourceBlockNumber(
                source_block_number,
            ));
        }
        if packet.data().len() != usize::from(header.symbol_size) {
            return Err(DatagramFecError::SymbolSizeMismatch {
                expected: usize::from(header.symbol_size),
                actual: packet.data().len(),
            });
        }
        Self::from_header(header)
    }

    pub const fn block_id(self) -> u32 {
        self.block_id
    }

    pub const fn transfer_length(self) -> u32 {
        self.transfer_length
    }

    pub const fn source_symbols(self) -> u16 {
        self.source_symbols
    }

    pub const fn symbol_size(self) -> u16 {
        self.symbol_size
    }

    fn oti(self) -> ObjectTransmissionInformation {
        exact_one_block_oti(u64::from(self.transfer_length), self.symbol_size)
    }
}

fn validate_one_block_geometry(
    transfer_length: u32,
    source_symbols: u16,
    symbol_size: u16,
) -> Result<(), DatagramFecError> {
    if source_symbols == 0 {
        return Err(DatagramFecError::InvalidSourceSymbols(source_symbols));
    }
    if symbol_size == 0 {
        return Err(DatagramFecError::InvalidSymbolSize(symbol_size));
    }
    if transfer_length == 0 {
        return Err(DatagramFecError::InvalidTransferLength(transfer_length));
    }

    let declared_source_symbols = u32::from(source_symbols);
    if declared_source_symbols > MAX_SOURCE_SYMBOLS_PER_BLOCK {
        return Err(DatagramFecError::SourceSymbolLimitExceeded {
            actual: declared_source_symbols,
            max: MAX_SOURCE_SYMBOLS_PER_BLOCK,
        });
    }

    let required_source_symbols = transfer_length.div_ceil(u32::from(symbol_size));
    if required_source_symbols > MAX_SOURCE_SYMBOLS_PER_BLOCK {
        return Err(DatagramFecError::SourceSymbolLimitExceeded {
            actual: required_source_symbols,
            max: MAX_SOURCE_SYMBOLS_PER_BLOCK,
        });
    }
    if declared_source_symbols != required_source_symbols {
        return Err(DatagramFecError::SourceSymbolCountMismatch {
            declared: source_symbols,
            required: required_source_symbols,
        });
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatagramFecError {
    HeaderTooShort {
        actual: usize,
    },
    PacketTooShort {
        actual: usize,
    },
    InvalidMagic {
        actual: [u8; 4],
    },
    UnsupportedVersion(u8),
    UnsupportedHeaderLength(u8),
    UnsupportedPacketKind(u8),
    UnsupportedPacketFlags(u8),
    InvalidSourceSymbols(u16),
    InvalidSymbolSize(u16),
    InvalidTransferLength(u32),
    SourceSymbolLimitExceeded {
        actual: u32,
        max: u32,
    },
    SourceSymbolCountMismatch {
        declared: u16,
        required: u32,
    },
    PayloadLengthMismatch {
        expected: usize,
        actual: usize,
    },
    SymbolSizeMismatch {
        expected: usize,
        actual: usize,
    },
    UnsupportedSourceBlockNumber(u8),
    InconsistentBlockGeometry {
        block_id: u32,
    },
    PacketCrc32Mismatch {
        expected: u32,
        actual: u32,
    },
    PayloadTooLong {
        actual: usize,
    },
    PayloadTooLargeForBlock {
        actual: usize,
        max: usize,
    },
    TransferLengthMismatch {
        expected: u32,
        actual: usize,
    },
    RepairSourceTooLarge {
        actual: usize,
        max: usize,
    },
    AdditionalRepairSymbolCount {
        actual: u32,
        max: u32,
    },
    AdditionalRepairOutputTooLarge {
        actual: usize,
        max: usize,
    },
    RepairSymbolIdExhausted {
        source_symbols: u16,
        next_repair_symbol: u32,
        requested: u32,
    },
}

impl fmt::Display for DatagramFecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HeaderTooShort { actual } => {
                write!(
                    formatter,
                    "datagram FEC header too short: expected {HEADER_LEN}, got {actual}"
                )
            }
            Self::PacketTooShort { actual } => {
                write!(
                    formatter,
                    "datagram FEC packet too short: expected at least {}, got {actual}",
                    HEADER_LEN + ENCODING_PACKET_HEADER_LEN
                )
            }
            Self::InvalidMagic { actual } => {
                write!(
                    formatter,
                    "invalid datagram FEC magic: expected {:?}, got {:?}",
                    DATAGRAM_MAGIC, actual
                )
            }
            Self::UnsupportedVersion(version) => {
                write!(
                    formatter,
                    "unsupported datagram FEC version: expected {DATAGRAM_VERSION}, got {version}"
                )
            }
            Self::UnsupportedHeaderLength(header_len) => {
                write!(
                    formatter,
                    "unsupported datagram FEC header length: expected {HEADER_LEN}, got {header_len}"
                )
            }
            Self::UnsupportedPacketKind(packet_kind) => {
                write!(
                    formatter,
                    "unsupported datagram FEC packet kind: {packet_kind}"
                )
            }
            Self::UnsupportedPacketFlags(packet_flags) => {
                write!(
                    formatter,
                    "unsupported datagram FEC packet flags: {packet_flags:#04x}"
                )
            }
            Self::InvalidSourceSymbols(value) => {
                write!(
                    formatter,
                    "invalid datagram FEC source symbol count: {value}"
                )
            }
            Self::InvalidSymbolSize(value) => {
                write!(formatter, "invalid datagram FEC symbol size: {value}")
            }
            Self::InvalidTransferLength(value) => {
                write!(formatter, "invalid datagram FEC transfer length: {value}")
            }
            Self::SourceSymbolLimitExceeded { actual, max } => {
                write!(
                    formatter,
                    "datagram FEC source symbol count {actual} exceeds the one-block limit {max}"
                )
            }
            Self::SourceSymbolCountMismatch { declared, required } => {
                write!(
                    formatter,
                    "datagram FEC source symbol count mismatch: header declares {declared}, transfer geometry requires {required}"
                )
            }
            Self::PayloadLengthMismatch { expected, actual } => {
                write!(
                    formatter,
                    "datagram FEC payload length mismatch: expected {expected} bytes, got {actual}"
                )
            }
            Self::SymbolSizeMismatch { expected, actual } => {
                write!(
                    formatter,
                    "datagram FEC symbol size mismatch: expected {expected} bytes, got {actual}"
                )
            }
            Self::UnsupportedSourceBlockNumber(value) => {
                write!(
                    formatter,
                    "datagram FEC source block number must be zero for one-block framing, got {value}"
                )
            }
            Self::InconsistentBlockGeometry { block_id } => {
                write!(
                    formatter,
                    "datagram FEC block {block_id} changed transmission geometry before completion"
                )
            }
            Self::PacketCrc32Mismatch { expected, actual } => {
                write!(
                    formatter,
                    "datagram FEC packet CRC32 mismatch: expected {expected:#010x}, got {actual:#010x}"
                )
            }
            Self::PayloadTooLong { actual } => {
                write!(
                    formatter,
                    "datagram FEC payload too long for u32 header: {actual}"
                )
            }
            Self::PayloadTooLargeForBlock { actual, max } => {
                write!(
                    formatter,
                    "datagram FEC block payload too large: got {actual} bytes, max is {max}"
                )
            }
            Self::TransferLengthMismatch { expected, actual } => {
                write!(
                    formatter,
                    "datagram FEC source bytes do not match the coding profile: expected {expected} bytes, got {actual}"
                )
            }
            Self::RepairSourceTooLarge { actual, max } => {
                write!(
                    formatter,
                    "RaptorQ repair source is {actual} bytes, exceeding the {max}-byte bound"
                )
            }
            Self::AdditionalRepairSymbolCount { actual, max } => {
                write!(
                    formatter,
                    "additional RaptorQ repair count must be between 1 and {max}, got {actual}"
                )
            }
            Self::AdditionalRepairOutputTooLarge { actual, max } => {
                write!(
                    formatter,
                    "additional RaptorQ repair output is {actual} bytes, exceeding the {max}-byte bound"
                )
            }
            Self::RepairSymbolIdExhausted {
                source_symbols,
                next_repair_symbol,
                requested,
            } => {
                write!(
                    formatter,
                    "RaptorQ repair ESI namespace exhausted: K={source_symbols}, next repair ordinal={next_repair_symbol}, requested={requested}"
                )
            }
        }
    }
}

impl std::error::Error for DatagramFecError {}

pub fn crc32_ieee(bytes: &[u8]) -> u32 {
    crc32_ieee_update(0, bytes)
}

pub fn crc32_ieee_update(previous: u32, bytes: &[u8]) -> u32 {
    let mut hasher = crc32fast::Hasher::new_with_initial(previous);
    hasher.update(bytes);
    hasher.finalize()
}

pub fn packet_crc32(header_without_crc: &[u8], payload: &[u8]) -> u32 {
    crc32_ieee_update(crc32_ieee(header_without_crc), payload)
}

/// Stateful generator for repair symbols that continue an existing block's ESI namespace.
///
/// Construct exactly one instance for each `(authenticated object, block profile)`
/// repair producer and keep it for the lifetime of that namespace. Every
/// successful call advances the repair-symbol cursor; failed calls leave it
/// unchanged. This makes repeated on-demand responses disjoint without tying
/// the coding operation to UDP, QUIC, or any other carrier.
#[derive(Debug)]
pub struct RaptorQRepairEncoder {
    profile: RaptorQBlockProfile,
    encoder: Encoder,
    next_repair_symbol: u32,
    next_packet_sequence: u32,
}

impl RaptorQRepairEncoder {
    /// Recreate an existing source block and continue after repair ordinals
    /// already emitted by the primary or another repair producer.
    ///
    /// `already_emitted_repair_symbols` is the next zero-based repair ordinal:
    /// if repair ordinals 0, 1, and 2 already exist, pass 3. The RFC 6330 ESI
    /// of the next packet will be `profile.source_symbols() + 3`.
    /// `next_packet_sequence` only continues RQD2 flow telemetry; it does not
    /// participate in the RaptorQ coding identity.
    pub fn new(
        source: &[u8],
        profile: RaptorQBlockProfile,
        already_emitted_repair_symbols: u32,
        next_packet_sequence: u32,
    ) -> Result<Self, DatagramFecError> {
        validate_one_block_geometry(
            profile.transfer_length,
            profile.source_symbols,
            profile.symbol_size,
        )?;
        if profile.transfer_length as usize > HARD_MAX_REPAIR_SOURCE_BYTES {
            return Err(DatagramFecError::RepairSourceTooLarge {
                actual: profile.transfer_length as usize,
                max: HARD_MAX_REPAIR_SOURCE_BYTES,
            });
        }
        if source.len() != profile.transfer_length as usize {
            return Err(DatagramFecError::TransferLengthMismatch {
                expected: profile.transfer_length,
                actual: source.len(),
            });
        }
        validate_repair_symbol_range(profile, already_emitted_repair_symbols, 0)?;

        Ok(Self {
            profile,
            encoder: Encoder::new(source, profile.oti()),
            next_repair_symbol: already_emitted_repair_symbols,
            next_packet_sequence,
        })
    }

    pub const fn profile(&self) -> RaptorQBlockProfile {
        self.profile
    }

    /// Zero-based repair ordinal that the next successful call will emit.
    pub const fn next_repair_symbol(&self) -> u32 {
        self.next_repair_symbol
    }

    /// Packet-sequence value that the next successful call will place in RQD2.
    pub const fn next_packet_sequence(&self) -> u32 {
        self.next_packet_sequence
    }

    /// RFC 6330 Encoding Symbol ID of the next repair symbol, or `None` when
    /// the 24-bit payload-ID namespace is exhausted.
    pub fn next_encoding_symbol_id(&self) -> Option<u32> {
        let encoding_symbol_id =
            u32::from(self.profile.source_symbols).checked_add(self.next_repair_symbol)?;
        (encoding_symbol_id < RAPTORQ_ENCODING_SYMBOL_ID_LIMIT).then_some(encoding_symbol_id)
    }

    /// Generate a bounded, disjoint batch of additional repair datagrams.
    pub fn encode_additional(
        &mut self,
        repair_symbols: u32,
    ) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        if repair_symbols == 0 || repair_symbols > HARD_MAX_EXTRA_REPAIR_SYMBOLS {
            return Err(DatagramFecError::AdditionalRepairSymbolCount {
                actual: repair_symbols,
                max: HARD_MAX_EXTRA_REPAIR_SYMBOLS,
            });
        }

        let output_bytes = datagram_size_for_symbol_size(self.profile.symbol_size)
            .saturating_mul(repair_symbols as usize);
        if output_bytes > HARD_MAX_ADDITIONAL_REPAIR_BYTES {
            return Err(DatagramFecError::AdditionalRepairOutputTooLarge {
                actual: output_bytes,
                max: HARD_MAX_ADDITIONAL_REPAIR_BYTES,
            });
        }
        validate_repair_symbol_range(self.profile, self.next_repair_symbol, repair_symbols)?;

        let source_block = self
            .encoder
            .get_block_encoders()
            .first()
            .expect("validated non-empty one-block OTI has one source block");
        let packets = source_block.repair_packets(self.next_repair_symbol, repair_symbols);
        let mut datagrams = Vec::with_capacity(packets.len());
        let mut packet_sequence = self.next_packet_sequence;
        for packet in packets {
            let serialized = packet.serialize();
            let header = DatagramFecHeader::raptorq(
                self.profile.block_id,
                self.profile.transfer_length,
                packet_sequence,
                self.profile.source_symbols,
                self.profile.symbol_size,
                &serialized,
            )?;
            let mut datagram = vec![0; HEADER_LEN];
            header.encode(&mut datagram)?;
            datagram.extend_from_slice(&serialized);
            datagrams.push(datagram);
            packet_sequence = packet_sequence.wrapping_add(1);
        }

        self.next_repair_symbol += repair_symbols;
        self.next_packet_sequence = packet_sequence;
        Ok(datagrams)
    }
}

fn validate_repair_symbol_range(
    profile: RaptorQBlockProfile,
    next_repair_symbol: u32,
    requested: u32,
) -> Result<(), DatagramFecError> {
    let Some(end_repair_symbol) = next_repair_symbol.checked_add(requested) else {
        return Err(DatagramFecError::RepairSymbolIdExhausted {
            source_symbols: profile.source_symbols,
            next_repair_symbol,
            requested,
        });
    };
    let repair_symbol_capacity =
        RAPTORQ_ENCODING_SYMBOL_ID_LIMIT - u32::from(profile.source_symbols);
    if end_repair_symbol > repair_symbol_capacity {
        return Err(DatagramFecError::RepairSymbolIdExhausted {
            source_symbols: profile.source_symbols,
            next_repair_symbol,
            requested,
        });
    }
    Ok(())
}

/// Stateful RaptorQ encoder that assigns monotonically increasing block ids.
#[derive(Debug, Clone)]
pub struct DatagramFecEncoder {
    block_id: u32,
    packet_sequence: u32,
    config: DatagramFecConfig,
}

impl Default for DatagramFecEncoder {
    fn default() -> Self {
        Self::new()
    }
}

impl DatagramFecEncoder {
    pub fn new() -> Self {
        Self {
            block_id: 0,
            packet_sequence: 0,
            config: DatagramFecConfig::default(),
        }
    }

    pub fn with_source_symbols(mut self, source_symbols: u16) -> Self {
        self.config.source_symbols = source_symbols.max(1);
        self
    }

    pub fn with_repair_symbols(mut self, repair_symbols: u32) -> Self {
        self.config.repair_symbols = repair_symbols;
        self
    }

    pub fn with_symbol_size(mut self, symbol_size: u16) -> Self {
        self.config.symbol_size = symbol_size.max(1);
        self
    }

    pub fn set_source_symbols(&mut self, source_symbols: u16) {
        self.config.source_symbols = source_symbols.max(1);
    }

    pub fn set_symbol_size(&mut self, symbol_size: u16) {
        self.config.symbol_size = symbol_size.max(1);
    }

    pub fn set_repair_symbols(&mut self, repair_symbols: u32) {
        self.config.repair_symbols = repair_symbols;
    }

    pub fn block_id(&self) -> u32 {
        self.block_id
    }

    pub fn packet_sequence(&self) -> u32 {
        self.packet_sequence
    }

    pub fn with_initial_block_id(mut self, block_id: u32) -> Self {
        self.block_id = block_id;
        self
    }

    pub fn set_block_id(&mut self, block_id: u32) {
        self.block_id = block_id;
    }

    pub fn config(&self) -> DatagramFecConfig {
        self.config
    }

    pub fn source_symbols(&self) -> u16 {
        self.config.source_symbols
    }

    pub fn symbol_size(&self) -> u16 {
        self.config.symbol_size
    }

    pub fn repair_symbols(&self) -> u32 {
        self.config.repair_symbols
    }

    /// Encode exactly one configured FEC block.
    pub fn encode_block(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        self.encode_block_with_repair_symbols(data, self.config.repair_symbols)
    }

    /// Encode exactly one configured FEC block using caller-owned datagram
    /// buffers for the returned packet storage.
    pub fn encode_block_reusing(
        &mut self,
        data: &[u8],
        pool: &mut DatagramBufferPool,
    ) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        self.encode_block_with_repair_symbols_reusing(data, self.config.repair_symbols, pool)
    }

    /// Encode one complete application object, even when it needs more source
    /// symbols than the configured low-latency block size.
    pub fn encode_object(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        self.encode_object_with_repair_symbols(data, self.config.repair_symbols)
    }

    /// Encode one complete application object with a caller-selected repair count.
    pub fn encode_object_with_repair_symbols(
        &mut self,
        data: &[u8],
        repair_symbols: u32,
    ) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        if data.len() > u32::MAX as usize {
            return Err(DatagramFecError::PayloadTooLong { actual: data.len() });
        }

        self.encode_one_block_with_repair_symbols(data, repair_symbols)
    }

    /// Encode exactly one FEC block with a caller-selected repair count.
    pub fn encode_block_with_repair_symbols(
        &mut self,
        data: &[u8],
        repair_symbols: u32,
    ) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        if data.len() > u32::MAX as usize {
            return Err(DatagramFecError::PayloadTooLong { actual: data.len() });
        }

        let max = self.config.max_payload_len();
        if data.len() > max {
            return Err(DatagramFecError::PayloadTooLargeForBlock {
                actual: data.len(),
                max,
            });
        }

        self.encode_one_block_with_repair_symbols(data, repair_symbols)
    }

    /// Encode exactly one FEC block with caller-selected repair count and
    /// caller-owned datagram buffers.
    pub fn encode_block_with_repair_symbols_reusing(
        &mut self,
        data: &[u8],
        repair_symbols: u32,
        pool: &mut DatagramBufferPool,
    ) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        if data.len() > u32::MAX as usize {
            return Err(DatagramFecError::PayloadTooLong { actual: data.len() });
        }

        let max = self.config.max_payload_len();
        if data.len() > max {
            return Err(DatagramFecError::PayloadTooLargeForBlock {
                actual: data.len(),
                max,
            });
        }

        self.encode_one_block_with_repair_symbols_reusing(data, repair_symbols, pool)
    }

    /// Split `data` into configured block-sized chunks and encode all chunks.
    pub fn encode_payload(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        if data.len() > u32::MAX as usize {
            return Err(DatagramFecError::PayloadTooLong { actual: data.len() });
        }

        let mut datagrams = Vec::new();
        let max_payload_len = self.config.max_payload_len().max(1);
        for chunk in data.chunks(max_payload_len) {
            datagrams.extend(self.encode_one_block(chunk)?);
        }

        if data.is_empty() {
            datagrams.extend(self.encode_one_block(data)?);
        }

        Ok(datagrams)
    }

    /// Split `data` into configured block-sized chunks and encode all chunks
    /// using caller-owned datagram buffers.
    pub fn encode_payload_reusing(
        &mut self,
        data: &[u8],
        pool: &mut DatagramBufferPool,
    ) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        if data.len() > u32::MAX as usize {
            return Err(DatagramFecError::PayloadTooLong { actual: data.len() });
        }

        let mut datagrams = Vec::new();
        let max_payload_len = self.config.max_payload_len().max(1);
        for chunk in data.chunks(max_payload_len) {
            datagrams.extend(self.encode_one_block_with_repair_symbols_reusing(
                chunk,
                self.config.repair_symbols,
                pool,
            )?);
        }

        if data.is_empty() {
            datagrams.extend(self.encode_one_block_with_repair_symbols_reusing(
                data,
                self.config.repair_symbols,
                pool,
            )?);
        }

        Ok(datagrams)
    }

    fn encode_one_block(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        self.encode_one_block_with_repair_symbols(data, self.config.repair_symbols)
    }

    fn encode_one_block_with_repair_symbols(
        &mut self,
        data: &[u8],
        repair_symbols: u32,
    ) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        self.encode_one_block_with_optional_pool(data, repair_symbols, None)
    }

    fn encode_one_block_with_repair_symbols_reusing(
        &mut self,
        data: &[u8],
        repair_symbols: u32,
        pool: &mut DatagramBufferPool,
    ) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        self.encode_one_block_with_optional_pool(data, repair_symbols, Some(pool))
    }

    fn encode_one_block_with_optional_pool(
        &mut self,
        data: &[u8],
        repair_symbols: u32,
        mut pool: Option<&mut DatagramBufferPool>,
    ) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        let encoder = Encoder::new(
            data,
            exact_one_block_oti(data.len() as u64, self.config.symbol_size),
        );
        let raptor_config = encoder.get_config();
        let packets = encoder.get_encoded_packets(repair_symbols);
        let block_id = self.block_id;
        let transfer_length = data.len() as u32;
        let source_symbols = source_symbol_count(data.len(), raptor_config.symbol_size());
        let symbol_size = raptor_config.symbol_size();

        let mut datagrams = Vec::with_capacity(packets.len());
        for packet in packets {
            let serialized = packet.serialize();
            let header = DatagramFecHeader::raptorq(
                block_id,
                transfer_length,
                self.packet_sequence,
                source_symbols,
                symbol_size,
                &serialized,
            )?;
            let mut datagram = if let Some(pool) = pool.as_deref_mut() {
                pool.take(HEADER_LEN + serialized.len())
            } else {
                Vec::with_capacity(HEADER_LEN + serialized.len())
            };
            datagram.clear();
            datagram.resize(HEADER_LEN, 0);
            header.encode(&mut datagram[..HEADER_LEN])?;
            datagram.extend_from_slice(&serialized);
            datagrams.push(datagram);
            self.packet_sequence = self.packet_sequence.wrapping_add(1);
        }

        self.block_id = self.block_id.wrapping_add(1);
        Ok(datagrams)
    }
}

#[derive(Debug)]
struct BlockState {
    decoder: Decoder,
    transfer_length: u32,
    source_symbols: u16,
    symbol_size: u16,
}

/// Stateful decoder for one ordered datagram flow.
#[derive(Debug, Default)]
pub struct DatagramFecDecoder {
    blocks: HashMap<u32, BlockState>,
    completed: HashSet<u32>,
    sequence_tracker: SequenceTracker,
}

impl DatagramFecDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push_datagram(&mut self, datagram: &[u8]) -> Result<Option<Vec<u8>>, DatagramFecError> {
        if datagram.len() < HEADER_LEN + ENCODING_PACKET_HEADER_LEN {
            return Err(DatagramFecError::PacketTooShort {
                actual: datagram.len(),
            });
        }

        let header = DatagramFecHeader::decode(datagram)?;
        let payload = header.payload(datagram)?;
        if payload.len() < ENCODING_PACKET_HEADER_LEN {
            return Err(DatagramFecError::PacketTooShort {
                actual: datagram.len(),
            });
        }
        self.sequence_tracker.observe(header.packet_sequence);
        if self.completed.contains(&header.block_id) {
            return Ok(None);
        }

        let packet = EncodingPacket::deserialize(payload);
        let source_block_number = packet.payload_id().source_block_number();
        if source_block_number != 0 {
            return Err(DatagramFecError::UnsupportedSourceBlockNumber(
                source_block_number,
            ));
        }
        if packet.data().len() != usize::from(header.symbol_size) {
            return Err(DatagramFecError::SymbolSizeMismatch {
                expected: usize::from(header.symbol_size),
                actual: packet.data().len(),
            });
        }
        if self.blocks.get(&header.block_id).is_some_and(|block| {
            block.transfer_length != header.transfer_length
                || block.source_symbols != header.source_symbols
                || block.symbol_size != header.symbol_size
        }) {
            self.blocks.remove(&header.block_id);
            return Err(DatagramFecError::InconsistentBlockGeometry {
                block_id: header.block_id,
            });
        }
        let block = self
            .blocks
            .entry(header.block_id)
            .or_insert_with(|| BlockState {
                decoder: Decoder::new(header.oti()),
                transfer_length: header.transfer_length,
                source_symbols: header.source_symbols,
                symbol_size: header.symbol_size,
            });

        let Some(decoded) = block.decoder.decode(packet) else {
            return Ok(None);
        };

        self.blocks.remove(&header.block_id);
        self.completed.insert(header.block_id);
        self.prune(header.block_id);
        Ok(Some(decoded))
    }

    pub fn sequence_stats(&self) -> SequenceStats {
        self.sequence_tracker.stats()
    }

    /// Stop retaining recovery state for a block whose delivery deadline has
    /// passed.
    ///
    /// The block id remains in the bounded duplicate-suppression window, so a
    /// late datagram cannot recreate the discarded decoder and grow memory
    /// again. Live callers should expire incomplete blocks when their playout
    /// deadline passes.
    pub fn expire_block(&mut self, block_id: u32) {
        self.blocks.remove(&block_id);
        self.completed.insert(block_id);
        self.prune(block_id);
    }

    /// Number of source blocks currently retaining RaptorQ decoder state.
    pub fn in_flight_block_count(&self) -> usize {
        self.blocks.len()
    }

    fn prune(&mut self, current_block_id: u32) {
        let cutoff = current_block_id.wrapping_sub(COMPLETED_WINDOW);
        self.blocks
            .retain(|block_id, _| current_block_id < COMPLETED_WINDOW || *block_id >= cutoff);
        self.completed
            .retain(|block_id| current_block_id < COMPLETED_WINDOW || *block_id >= cutoff);
    }
}

#[cfg(feature = "udp")]
#[derive(Debug)]
pub struct UdpFecSender {
    socket: UdpSocket,
    target: SocketAddr,
    encoder: DatagramFecEncoder,
}

#[cfg(feature = "udp")]
impl UdpFecSender {
    pub async fn new(target: SocketAddr) -> std::io::Result<Self> {
        let bind_addr: SocketAddr = if target.is_ipv6() {
            "[::]:0".parse().expect("valid IPv6 bind address")
        } else {
            "0.0.0.0:0".parse().expect("valid IPv4 bind address")
        };
        Self::bind(bind_addr, target).await
    }

    pub async fn bind(bind_addr: SocketAddr, target: SocketAddr) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            socket,
            target,
            encoder: DatagramFecEncoder::new(),
        })
    }

    pub fn with_source_symbols(mut self, source_symbols: u16) -> Self {
        self.encoder.set_source_symbols(source_symbols);
        self
    }

    pub fn with_repair_symbols(mut self, repair_symbols: u32) -> Self {
        self.encoder.set_repair_symbols(repair_symbols);
        self
    }

    pub fn with_symbol_size(mut self, symbol_size: u16) -> Self {
        self.encoder.set_symbol_size(symbol_size);
        self
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub fn target(&self) -> SocketAddr {
        self.target
    }

    pub fn encoder(&self) -> &DatagramFecEncoder {
        &self.encoder
    }

    pub fn encoder_mut(&mut self) -> &mut DatagramFecEncoder {
        &mut self.encoder
    }

    pub async fn send(&mut self, data: &[u8]) -> std::io::Result<()> {
        let datagrams = self.encode_payload_as_io(data)?;
        for datagram in datagrams {
            self.socket.send_to(&datagram, self.target).await?;
        }
        Ok(())
    }

    pub async fn send_block(&mut self, data: &[u8]) -> std::io::Result<()> {
        let datagrams = self.encode_block_as_io(data)?;
        for datagram in datagrams {
            self.socket.send_to(&datagram, self.target).await?;
        }
        Ok(())
    }

    fn encode_payload_as_io(&mut self, data: &[u8]) -> std::io::Result<Vec<Vec<u8>>> {
        self.encoder
            .encode_payload(data)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
    }

    fn encode_block_as_io(&mut self, data: &[u8]) -> std::io::Result<Vec<Vec<u8>>> {
        self.encoder
            .encode_block(data)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))
    }
}

#[cfg(feature = "udp")]
#[derive(Debug)]
pub struct UdpFecReceiver {
    socket: UdpSocket,
    decoders: HashMap<SocketAddr, DatagramFecDecoder>,
    datagram: Vec<u8>,
}

#[cfg(feature = "udp")]
impl UdpFecReceiver {
    pub async fn bind(bind_addr: SocketAddr) -> std::io::Result<Self> {
        let socket = UdpSocket::bind(bind_addr).await?;
        Ok(Self {
            socket,
            decoders: HashMap::new(),
            datagram: vec![0; 65536],
        })
    }

    pub fn local_addr(&self) -> std::io::Result<SocketAddr> {
        self.socket.local_addr()
    }

    pub async fn recv_decoded(&mut self) -> std::io::Result<(SocketAddr, Vec<u8>)> {
        loop {
            let (len, peer) = self.socket.recv_from(&mut self.datagram).await?;
            let decoder = self.decoders.entry(peer).or_default();
            match decoder.push_datagram(&self.datagram[..len]) {
                Ok(Some(decoded)) => return Ok((peer, decoded)),
                Ok(None) => {}
                Err(error) => {
                    return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, error));
                }
            }
        }
    }
}

pub fn decode_header(datagram: &[u8]) -> Result<DatagramFecHeader, DatagramFecError> {
    DatagramFecHeader::decode(datagram)
}

fn exact_one_block_oti(transfer_length: u64, symbol_size: u16) -> ObjectTransmissionInformation {
    ObjectTransmissionInformation::new(transfer_length, symbol_size.max(1), 1, 1, 1)
}

pub fn source_symbol_count(byte_len: usize, symbol_size: u16) -> u16 {
    if byte_len == 0 {
        return 1;
    }
    let symbol_size = usize::from(symbol_size.max(1));
    byte_len.div_ceil(symbol_size).min(u16::MAX as usize) as u16
}

pub fn datagram_size_for_symbol_size(symbol_size: u16) -> usize {
    HEADER_LEN + ENCODING_PACKET_HEADER_LEN + usize::from(symbol_size)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unchecked_datagram(
        transfer_length: u32,
        source_symbols: u16,
        symbol_size: u16,
        source_block_number: u8,
    ) -> Vec<u8> {
        let payload_len = ENCODING_PACKET_HEADER_LEN + usize::from(symbol_size);
        let mut datagram = vec![0; HEADER_LEN + payload_len];
        datagram[0..4].copy_from_slice(&DATAGRAM_MAGIC);
        datagram[4] = DATAGRAM_VERSION;
        datagram[5] = HEADER_LEN as u8;
        datagram[6] = DATAGRAM_KIND_RAPTORQ;
        datagram[12..16].copy_from_slice(&transfer_length.to_le_bytes());
        datagram[20..22].copy_from_slice(&source_symbols.to_le_bytes());
        datagram[22..24].copy_from_slice(&symbol_size.to_le_bytes());
        datagram[24..28].copy_from_slice(&(payload_len as u32).to_le_bytes());
        datagram[HEADER_LEN] = source_block_number;
        datagram
    }

    fn encoding_symbol_id(datagram: &[u8]) -> u32 {
        let header = decode_header(datagram).expect("header");
        let payload = header.payload(datagram).expect("payload");
        EncodingPacket::deserialize(payload)
            .payload_id()
            .encoding_symbol_id()
    }

    #[test]
    fn header_roundtrips() {
        let payload = b"serialized-raptorq-packet";
        let header = DatagramFecHeader::raptorq(7, 1024, 99, 4, 256, payload).expect("header");
        let mut bytes = [0; HEADER_LEN];
        header.encode(&mut bytes).expect("encode header");
        assert_eq!(&bytes[0..4], &DATAGRAM_MAGIC);
        assert_eq!(bytes[4], DATAGRAM_VERSION);
        assert_eq!(bytes[5], HEADER_LEN as u8);
        assert_eq!(
            DatagramFecHeader::decode(&bytes).expect("decode header"),
            header
        );
        assert_eq!(
            header.compute_packet_crc32(payload).expect("packet crc"),
            header.packet_crc32
        );
    }

    #[test]
    fn crc32_ieee_uses_standard_vector() {
        assert_eq!(crc32_ieee(b"123456789"), 0xcbf4_3926);
    }

    #[test]
    fn crc32_ieee_fast_path_matches_the_bitwise_wire_reference() {
        fn reference_update(previous: u32, bytes: &[u8]) -> u32 {
            let mut crc = !previous;
            for &byte in bytes {
                crc ^= u32::from(byte);
                for _ in 0..8 {
                    let mask = (crc & 1).wrapping_neg();
                    crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
                }
            }
            !crc
        }

        let payload = (0..4_096_u32)
            .map(|index| index.wrapping_mul(1_103_515_245).wrapping_add(12_345) as u8)
            .collect::<Vec<_>>();
        let expected = reference_update(0, &payload);
        assert_eq!(crc32_ieee(&payload), expected);
        for split in [0, 1, 31, 32, 255, 2_048, payload.len()] {
            let first = crc32_ieee(&payload[..split]);
            assert_eq!(
                crc32_ieee_update(first, &payload[split..]),
                expected,
                "incremental CRC diverged at split {split}"
            );
        }
    }

    #[test]
    fn header_rejects_wrong_magic() {
        let payload = b"serialized-raptorq-packet";
        let header = DatagramFecHeader::raptorq(7, 1024, 99, 4, 256, payload).expect("header");
        let mut bytes = [0; HEADER_LEN];
        header.encode(&mut bytes).expect("encode header");
        bytes[0] = 0;

        assert!(matches!(
            DatagramFecHeader::decode(&bytes),
            Err(DatagramFecError::InvalidMagic { .. })
        ));
    }

    #[test]
    fn decoder_accepts_the_maximum_one_block_source_symbol_geometry() {
        let datagram = unchecked_datagram(
            MAX_SOURCE_SYMBOLS_PER_BLOCK,
            MAX_SOURCE_SYMBOLS_PER_BLOCK as u16,
            1,
            0,
        );
        let mut decoder = DatagramFecDecoder::new();

        assert_eq!(
            decoder.push_datagram(&datagram).expect("valid geometry"),
            None
        );
    }

    #[test]
    fn decoder_rejects_geometry_above_the_one_block_source_symbol_limit() {
        let source_symbols = MAX_SOURCE_SYMBOLS_PER_BLOCK + 1;
        let datagram = unchecked_datagram(source_symbols, source_symbols as u16, 1, 0);
        let mut decoder = DatagramFecDecoder::new();

        assert!(matches!(
            decoder.push_datagram(&datagram),
            Err(DatagramFecError::SourceSymbolLimitExceeded {
                actual,
                max: MAX_SOURCE_SYMBOLS_PER_BLOCK,
            }) if actual == source_symbols
        ));
    }

    #[test]
    fn decoder_rejects_mismatched_transfer_geometry() {
        let datagram = unchecked_datagram(129, 2, 64, 0);
        let mut decoder = DatagramFecDecoder::new();

        assert!(matches!(
            decoder.push_datagram(&datagram),
            Err(DatagramFecError::SourceSymbolCountMismatch {
                declared: 2,
                required: 3,
            })
        ));
    }

    #[test]
    fn decoder_rejects_zero_transfer_length_and_symbol_size() {
        let zero_transfer = unchecked_datagram(0, 1, 64, 0);
        let zero_symbol_size = unchecked_datagram(1, 1, 0, 0);
        let mut decoder = DatagramFecDecoder::new();

        assert!(matches!(
            decoder.push_datagram(&zero_transfer),
            Err(DatagramFecError::InvalidTransferLength(0))
        ));
        assert!(matches!(
            decoder.push_datagram(&zero_symbol_size),
            Err(DatagramFecError::InvalidSymbolSize(0))
        ));
    }

    #[test]
    fn decoder_rejects_nonzero_source_block_number() {
        let datagram = unchecked_datagram(64, 1, 64, 1);
        let mut decoder = DatagramFecDecoder::new();

        assert!(matches!(
            decoder.push_datagram(&datagram),
            Err(DatagramFecError::UnsupportedSourceBlockNumber(1))
        ));
    }

    #[test]
    fn encoded_datagrams_have_v2_payload_metadata() {
        let payload = b"fec-protected-media-payload".repeat(2);
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(64)
            .with_repair_symbols(1);
        let datagrams = encoder.encode_block(&payload).expect("encode block");
        let datagram = &datagrams[0];
        let header = decode_header(datagram).expect("header");

        assert_eq!(header.packet_kind, DATAGRAM_KIND_RAPTORQ);
        assert_eq!(header.packet_flags, DATAGRAM_FLAG_PACKET_CRC32);
        assert_eq!(header.payload_len as usize, datagram.len() - HEADER_LEN);
        assert_eq!(
            header
                .compute_packet_crc32(&datagram[HEADER_LEN..])
                .expect("packet crc"),
            header.packet_crc32
        );
    }

    #[test]
    fn configured_symbol_size_is_not_rounded_by_raptorq_defaults() {
        let payload = vec![0x31; DatagramFecConfig::default().max_payload_len()];
        let repair_symbols = 2;
        let mut encoder = DatagramFecEncoder::new().with_repair_symbols(repair_symbols);
        let datagrams = encoder.encode_block(&payload).expect("encode block");

        assert_eq!(
            datagrams.len(),
            usize::from(DEFAULT_SOURCE_SYMBOLS) + repair_symbols as usize
        );

        for datagram in datagrams {
            let header = decode_header(&datagram).expect("header");
            assert_eq!(header.symbol_size, DEFAULT_SYMBOL_SIZE);
            assert_eq!(header.source_symbols, DEFAULT_SOURCE_SYMBOLS);
            assert_eq!(
                header.payload_len as usize,
                ENCODING_PACKET_HEADER_LEN + usize::from(DEFAULT_SYMBOL_SIZE)
            );
            assert_eq!(
                datagram.len(),
                datagram_size_for_symbol_size(DEFAULT_SYMBOL_SIZE)
            );
        }
    }

    #[test]
    fn default_periodic_loss_recovers_many_full_blocks() {
        let block_len = DatagramFecConfig::default().max_payload_len();
        let payload = (0..(block_len * 96))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let mut encoder = DatagramFecEncoder::new().with_repair_symbols(2);
        let datagrams = encoder.encode_payload(&payload).expect("encode payload");
        let mut decoder = DatagramFecDecoder::new();
        let mut recovered = Vec::with_capacity(payload.len());
        let mut dropped = 0usize;

        for (index, datagram) in datagrams.iter().enumerate() {
            if (index + 1) % 5 == 0 {
                dropped += 1;
                continue;
            }
            if let Some(decoded) = decoder.push_datagram(datagram).expect("decode datagram") {
                recovered.extend_from_slice(&decoded);
            }
        }

        assert!(dropped > 0);
        assert_eq!(recovered, payload);
    }

    #[test]
    fn decoder_rejects_payload_length_mismatch() {
        let payload = b"fec-protected-media-payload";
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(64)
            .with_repair_symbols(1);
        let mut datagram = encoder
            .encode_block(payload)
            .expect("encode block")
            .remove(0);
        datagram.push(0);

        let mut decoder = DatagramFecDecoder::new();
        assert!(matches!(
            decoder.push_datagram(&datagram),
            Err(DatagramFecError::PayloadLengthMismatch { .. })
        ));
    }

    #[test]
    fn decoder_rejects_packet_crc_mismatch() {
        let payload = b"fec-protected-media-payload";
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(64)
            .with_repair_symbols(1);
        let mut datagram = encoder
            .encode_block(payload)
            .expect("encode block")
            .remove(0);
        let last = datagram.last_mut().expect("payload byte");
        *last ^= 0x01;

        let mut decoder = DatagramFecDecoder::new();
        assert!(matches!(
            decoder.push_datagram(&datagram),
            Err(DatagramFecError::PacketCrc32Mismatch { .. })
        ));
    }

    #[test]
    fn decoder_rejects_reused_block_id_with_different_geometry() {
        let mut first_encoder = DatagramFecEncoder::new()
            .with_source_symbols(4)
            .with_symbol_size(64)
            .with_repair_symbols(1);
        let mut second_encoder = DatagramFecEncoder::new()
            .with_source_symbols(4)
            .with_symbol_size(80)
            .with_repair_symbols(1);
        let first = first_encoder
            .encode_block(&[0x11; 200])
            .expect("encode first flow");
        let second = second_encoder
            .encode_block(&[0x22; 240])
            .expect("encode second flow");
        assert_eq!(decode_header(&first[0]).unwrap().block_id, 0);
        assert_eq!(decode_header(&second[0]).unwrap().block_id, 0);

        let mut decoder = DatagramFecDecoder::new();
        assert_eq!(decoder.push_datagram(&first[0]).unwrap(), None);
        assert!(matches!(
            decoder.push_datagram(&second[0]),
            Err(DatagramFecError::InconsistentBlockGeometry { block_id: 0 })
        ));
    }

    #[test]
    fn raptorq_roundtrips_with_one_missing_source_packet() {
        let payload = b"fec-protected-media-payload".repeat(16);
        let mut encoder = DatagramFecEncoder::new()
            .with_source_symbols(16)
            .with_symbol_size(64)
            .with_repair_symbols(2);
        let datagrams = encoder.encode_block(&payload).expect("encode block");
        assert!(datagrams.len() > 2);

        let mut decoder = DatagramFecDecoder::new();
        let mut decoded = None;
        for (index, datagram) in datagrams.iter().enumerate() {
            if index == 1 {
                continue;
            }
            decoded = decoder.push_datagram(datagram).expect("decode datagram");
            if decoded.is_some() {
                break;
            }
        }

        assert_eq!(decoded, Some(payload));
    }

    #[test]
    fn encode_payload_splits_into_configured_blocks() {
        let payload = vec![42; 100];
        let mut encoder = DatagramFecEncoder::new()
            .with_source_symbols(2)
            .with_symbol_size(16)
            .with_repair_symbols(1);
        let datagrams = encoder.encode_payload(&payload).expect("encode payload");
        let block_ids = datagrams
            .iter()
            .map(|datagram| decode_header(datagram).expect("header").block_id)
            .collect::<HashSet<_>>();

        assert_eq!(block_ids.len(), 4);
    }

    #[test]
    fn reusable_buffer_pool_recycles_datagram_storage() {
        let payload = vec![42; 96];
        let mut encoder = DatagramFecEncoder::new()
            .with_source_symbols(2)
            .with_symbol_size(48)
            .with_repair_symbols(1);
        let mut pool = DatagramBufferPool::new();
        let datagrams = encoder
            .encode_block_reusing(&payload, &mut pool)
            .expect("encode with pool");
        let first_capacity = datagrams[0].capacity();
        let first_ptr = datagrams[0].as_ptr();
        pool.recycle_many(datagrams);

        let datagrams = encoder
            .encode_block_reusing(&payload, &mut pool)
            .expect("encode with recycled pool");

        assert_eq!(pool.available(), 0);
        assert_eq!(datagrams[0].capacity(), first_capacity);
        assert_eq!(datagrams[0].as_ptr(), first_ptr);
    }

    #[test]
    fn ignores_duplicate_completed_block_packets() {
        let payload = b"single-block";
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(32)
            .with_repair_symbols(1);
        let datagrams = encoder.encode_block(payload).expect("encode block");
        let mut decoder = DatagramFecDecoder::new();
        let decoded = decoder
            .push_datagram(&datagrams[0])
            .expect("decode first datagram");
        assert_eq!(decoded.as_deref(), Some(payload.as_slice()));
        let duplicate = decoder
            .push_datagram(&datagrams[1])
            .expect("ignore duplicate block packet");
        assert!(duplicate.is_none());
    }

    #[test]
    fn expired_block_releases_state_and_ignores_late_packets() {
        let payload = vec![0x2a; 96];
        let mut encoder = DatagramFecEncoder::new()
            .with_source_symbols(2)
            .with_symbol_size(48)
            .with_repair_symbols(0);
        let datagrams = encoder.encode_block(&payload).expect("encode block");
        let block_id = decode_header(&datagrams[0]).expect("header").block_id;
        let mut decoder = DatagramFecDecoder::new();

        assert!(decoder
            .push_datagram(&datagrams[0])
            .expect("decode first source")
            .is_none());
        assert_eq!(decoder.in_flight_block_count(), 1);

        decoder.expire_block(block_id);
        assert_eq!(decoder.in_flight_block_count(), 0);
        assert!(decoder
            .push_datagram(&datagrams[1])
            .expect("ignore late source")
            .is_none());
        assert_eq!(decoder.in_flight_block_count(), 0);
    }

    #[test]
    fn header_sequences_are_monotonic() {
        let mut encoder = DatagramFecEncoder::new()
            .with_symbol_size(16)
            .with_repair_symbols(1);
        let datagrams = encoder.encode_block(&[7; 48]).expect("encode block");
        let sequences = datagrams
            .iter()
            .map(|datagram| decode_header(datagram).expect("header").packet_sequence)
            .collect::<Vec<_>>();

        assert_eq!(sequences, vec![0, 1, 2, 3]);
    }

    #[test]
    fn object_encoding_keeps_large_payloads_in_one_decoded_block() {
        let payload = vec![0x35; 4096];
        let mut encoder = DatagramFecEncoder::new()
            .with_source_symbols(2)
            .with_symbol_size(64)
            .with_repair_symbols(1)
            .with_initial_block_id(100);
        let datagrams = encoder.encode_object(&payload).expect("encode object");
        let block_ids = datagrams
            .iter()
            .map(|datagram| decode_header(datagram).expect("header").block_id)
            .collect::<HashSet<_>>();

        assert_eq!(block_ids, HashSet::from([100]));

        let mut decoder = DatagramFecDecoder::new();
        let mut decoded = None;
        for datagram in datagrams {
            decoded = decoder.push_datagram(&datagram).expect("decode datagram");
            if decoded.is_some() {
                break;
            }
        }

        assert_eq!(decoded, Some(payload));
    }

    #[test]
    fn secondary_additional_repair_combines_with_primary_source_symbols() {
        let payload = (0..6001)
            .map(|index| ((index * 37 + 11) % 251) as u8)
            .collect::<Vec<_>>();
        let mut primary = DatagramFecEncoder::new()
            .with_symbol_size(512)
            .with_repair_symbols(1)
            .with_initial_block_id(77);
        let primary_batch = primary.encode_object(&payload).expect("primary encode");
        let profile = RaptorQBlockProfile::from_datagram(&primary_batch[0]).expect("profile");
        let source_symbols = usize::from(profile.source_symbols());

        assert_eq!(primary_batch.len(), source_symbols + 1);
        assert_eq!(
            encoding_symbol_id(&primary_batch[source_symbols]),
            source_symbols as u32
        );

        let mut secondary =
            RaptorQRepairEncoder::new(&payload, profile, 1, primary.packet_sequence())
                .expect("secondary repair encoder");
        let additional = secondary.encode_additional(6).expect("additional repair");
        assert_eq!(
            encoding_symbol_id(&additional[0]),
            source_symbols as u32 + 1
        );
        assert!(additional.iter().all(|datagram| {
            RaptorQBlockProfile::from_datagram(datagram).expect("repair profile") == profile
        }));

        let mut decoder = DatagramFecDecoder::new();
        let mut decoded = None;
        for (index, datagram) in primary_batch[..source_symbols].iter().enumerate() {
            if matches!(index, 1 | 5 | 9) {
                continue;
            }
            decoded = decoder.push_datagram(datagram).expect("primary source");
            assert!(decoded.is_none());
        }
        for datagram in additional.iter().rev() {
            decoded = decoder.push_datagram(datagram).expect("secondary repair");
            if decoded.is_some() {
                break;
            }
        }

        assert_eq!(decoded, Some(payload));
    }

    #[test]
    fn additional_repair_cursor_never_reuses_an_encoding_symbol_id() {
        let payload = vec![0x5a; 700];
        let profile =
            RaptorQBlockProfile::new(9, payload.len() as u32, 3, 256).expect("valid profile");
        let mut repair =
            RaptorQRepairEncoder::new(&payload, profile, 2, 400).expect("repair encoder");

        let first = repair.encode_additional(2).expect("first response");
        let second = repair.encode_additional(3).expect("second response");
        let first_ids = first
            .iter()
            .map(|packet| encoding_symbol_id(packet))
            .collect::<Vec<_>>();
        let second_ids = second
            .iter()
            .map(|packet| encoding_symbol_id(packet))
            .collect::<Vec<_>>();

        assert_eq!(first_ids, vec![5, 6]);
        assert_eq!(second_ids, vec![7, 8, 9]);
        assert!(first_ids.iter().all(|id| !second_ids.contains(id)));
        assert_eq!(repair.next_repair_symbol(), 7);
        assert_eq!(repair.next_encoding_symbol_id(), Some(10));
        assert_eq!(repair.next_packet_sequence(), 405);
    }

    #[test]
    fn additional_repair_rejects_geometry_work_and_esi_overflow() {
        assert!(matches!(
            RaptorQBlockProfile::new(1, 513, 2, 256),
            Err(DatagramFecError::SourceSymbolCountMismatch {
                declared: 2,
                required: 3,
            })
        ));

        let payload = vec![0x42; 512];
        let profile = RaptorQBlockProfile::new(1, 512, 2, 256).expect("profile");
        assert!(matches!(
            RaptorQRepairEncoder::new(&payload[..511], profile, 0, 0),
            Err(DatagramFecError::TransferLengthMismatch {
                expected: 512,
                actual: 511,
            })
        ));

        let oversized_length = HARD_MAX_REPAIR_SOURCE_BYTES as u32 + 1;
        let oversized_symbols = oversized_length.div_ceil(1024) as u16;
        let oversized_profile =
            RaptorQBlockProfile::new(1, oversized_length, oversized_symbols, 1024)
                .expect("valid but operationally excessive profile");
        assert!(matches!(
            RaptorQRepairEncoder::new(&[], oversized_profile, 0, 0),
            Err(DatagramFecError::RepairSourceTooLarge { .. })
        ));

        let mut repair = RaptorQRepairEncoder::new(&payload, profile, 0, 0).expect("repair");
        assert!(matches!(
            repair.encode_additional(0),
            Err(DatagramFecError::AdditionalRepairSymbolCount { actual: 0, .. })
        ));
        assert!(matches!(
            repair.encode_additional(HARD_MAX_EXTRA_REPAIR_SYMBOLS + 1),
            Err(DatagramFecError::AdditionalRepairSymbolCount { .. })
        ));
        assert_eq!(repair.next_repair_symbol(), 0);

        let namespace_capacity = RAPTORQ_ENCODING_SYMBOL_ID_LIMIT - 2;
        let mut exhausted = RaptorQRepairEncoder::new(&payload, profile, namespace_capacity - 1, 0)
            .expect("last available ESI");
        assert!(matches!(
            exhausted.encode_additional(2),
            Err(DatagramFecError::RepairSymbolIdExhausted { .. })
        ));
        assert_eq!(exhausted.next_repair_symbol(), namespace_capacity - 1);

        let wide_payload = vec![0x33; u16::MAX as usize];
        let wide_profile =
            RaptorQBlockProfile::new(2, u16::MAX as u32, 1, u16::MAX).expect("wide profile");
        let mut wide =
            RaptorQRepairEncoder::new(&wide_payload, wide_profile, 0, 0).expect("wide repair");
        assert!(matches!(
            wide.encode_additional(257),
            Err(DatagramFecError::AdditionalRepairOutputTooLarge { .. })
        ));
    }
}
