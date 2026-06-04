//! RaptorQ forward-error-correction framing for low-latency datagrams.
//!
//! The crate keeps the wire protocol intentionally small: every datagram starts
//! with a self-identifying v2 header followed by a serialized RaptorQ
//! `EncodingPacket`.

mod adaptive;
mod backfill;
mod media;
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
pub use schedule::{
    plan_media_datagrams, MediaDatagramClass, MediaDropReason, MediaDroppedDatagram,
    MediaQueueState, MediaScheduledDatagram, MediaSendPlan, MediaSendPolicy,
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
const SUPPORTED_PACKET_FLAGS: u8 = DATAGRAM_FLAG_PACKET_CRC32;
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

        let expected = self.payload_len as usize;
        let actual = datagram.len() - HEADER_LEN;
        if actual != expected {
            return Err(DatagramFecError::PayloadLengthMismatch { expected, actual });
        }

        let payload = &datagram[HEADER_LEN..];
        if self.packet_flags & DATAGRAM_FLAG_PACKET_CRC32 != 0 {
            let actual_crc32 = self.compute_packet_crc32(payload)?;
            if actual_crc32 != self.packet_crc32 {
                return Err(DatagramFecError::PacketCrc32Mismatch {
                    expected: self.packet_crc32,
                    actual: actual_crc32,
                });
            }
        }

        Ok(payload)
    }

    pub fn compute_packet_crc32(&self, payload: &[u8]) -> Result<u32, DatagramFecError> {
        let expected_len = self.payload_len as usize;
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
        datagram_size_for_symbol_size(self.symbol_size)
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
        if self.source_symbols == 0 {
            return Err(DatagramFecError::InvalidSourceSymbols(self.source_symbols));
        }
        if self.symbol_size == 0 {
            return Err(DatagramFecError::InvalidSymbolSize(self.symbol_size));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatagramFecError {
    HeaderTooShort { actual: usize },
    PacketTooShort { actual: usize },
    InvalidMagic { actual: [u8; 4] },
    UnsupportedVersion(u8),
    UnsupportedHeaderLength(u8),
    UnsupportedPacketKind(u8),
    UnsupportedPacketFlags(u8),
    InvalidSourceSymbols(u16),
    InvalidSymbolSize(u16),
    PayloadLengthMismatch { expected: usize, actual: usize },
    PacketCrc32Mismatch { expected: u32, actual: u32 },
    PayloadTooLong { actual: usize },
    PayloadTooLargeForBlock { actual: usize, max: usize },
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
            Self::PayloadLengthMismatch { expected, actual } => {
                write!(
                    formatter,
                    "datagram FEC payload length mismatch: expected {expected} bytes, got {actual}"
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
        }
    }
}

impl std::error::Error for DatagramFecError {}

pub fn crc32_ieee(bytes: &[u8]) -> u32 {
    crc32_ieee_update(0, bytes)
}

pub fn crc32_ieee_update(previous: u32, bytes: &[u8]) -> u32 {
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

pub fn packet_crc32(header_without_crc: &[u8], payload: &[u8]) -> u32 {
    crc32_ieee_update(crc32_ieee(header_without_crc), payload)
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
        let block = self
            .blocks
            .entry(header.block_id)
            .or_insert_with(|| BlockState {
                decoder: Decoder::new(header.oti()),
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
}
