//! RaptorQ forward-error-correction framing for low-latency datagrams.
//!
//! The crate keeps the wire protocol intentionally small: every datagram starts
//! with a 12-byte little-endian header followed by a serialized RaptorQ
//! `EncodingPacket`.

use raptorq::{Decoder, Encoder, EncodingPacket, ObjectTransmissionInformation};
use std::collections::{HashMap, HashSet};
use std::fmt;

#[cfg(feature = "udp")]
use std::net::SocketAddr;

#[cfg(feature = "udp")]
use tokio::net::UdpSocket;

/// Bytes in the per-datagram header.
pub const HEADER_LEN: usize = 12;
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

/// The 12-byte prefix carried by every encoded datagram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DatagramFecHeader {
    pub block_id: u32,
    pub transfer_length: u32,
    pub source_symbols: u16,
    pub symbol_size: u16,
}

impl DatagramFecHeader {
    pub fn encode(&self, bytes: &mut [u8]) -> Result<(), DatagramFecError> {
        if bytes.len() < HEADER_LEN {
            return Err(DatagramFecError::HeaderTooShort {
                actual: bytes.len(),
            });
        }

        bytes[0..4].copy_from_slice(&self.block_id.to_le_bytes());
        bytes[4..8].copy_from_slice(&self.transfer_length.to_le_bytes());
        bytes[8..10].copy_from_slice(&self.source_symbols.to_le_bytes());
        bytes[10..12].copy_from_slice(&self.symbol_size.to_le_bytes());
        Ok(())
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, DatagramFecError> {
        if bytes.len() < HEADER_LEN {
            return Err(DatagramFecError::HeaderTooShort {
                actual: bytes.len(),
            });
        }

        let source_symbols =
            u16::from_le_bytes(bytes[8..10].try_into().expect("header length checked"));
        let symbol_size =
            u16::from_le_bytes(bytes[10..12].try_into().expect("header length checked"));

        if source_symbols == 0 {
            return Err(DatagramFecError::InvalidSourceSymbols(source_symbols));
        }
        if symbol_size == 0 {
            return Err(DatagramFecError::InvalidSymbolSize(symbol_size));
        }

        Ok(Self {
            block_id: u32::from_le_bytes(bytes[0..4].try_into().expect("header length checked")),
            transfer_length: u32::from_le_bytes(
                bytes[4..8].try_into().expect("header length checked"),
            ),
            source_symbols,
            symbol_size,
        })
    }

    pub fn datagram_size(&self) -> usize {
        datagram_size_for_symbol_size(self.symbol_size)
    }

    fn oti(&self) -> ObjectTransmissionInformation {
        ObjectTransmissionInformation::with_defaults(self.transfer_length as u64, self.symbol_size)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DatagramFecError {
    HeaderTooShort { actual: usize },
    PacketTooShort { actual: usize },
    InvalidSourceSymbols(u16),
    InvalidSymbolSize(u16),
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
            Self::InvalidSourceSymbols(value) => {
                write!(
                    formatter,
                    "invalid datagram FEC source symbol count: {value}"
                )
            }
            Self::InvalidSymbolSize(value) => {
                write!(formatter, "invalid datagram FEC symbol size: {value}")
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

/// Stateful RaptorQ encoder that assigns monotonically increasing block ids.
#[derive(Debug, Clone)]
pub struct DatagramFecEncoder {
    block_id: u32,
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

        self.encode_one_block(data)
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

    fn encode_one_block(&mut self, data: &[u8]) -> Result<Vec<Vec<u8>>, DatagramFecError> {
        let encoder = Encoder::with_defaults(data, self.config.symbol_size);
        let raptor_config = encoder.get_config();
        let packets = encoder.get_encoded_packets(self.config.repair_symbols);
        let header = DatagramFecHeader {
            block_id: self.block_id,
            transfer_length: data.len() as u32,
            source_symbols: source_symbol_count(data.len(), raptor_config.symbol_size()),
            symbol_size: raptor_config.symbol_size(),
        };

        let mut datagrams = Vec::with_capacity(packets.len());
        for packet in packets {
            let serialized = packet.serialize();
            let mut datagram = Vec::with_capacity(HEADER_LEN + serialized.len());
            datagram.resize(HEADER_LEN, 0);
            header.encode(&mut datagram[..HEADER_LEN])?;
            datagram.extend_from_slice(&serialized);
            datagrams.push(datagram);
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
        if self.completed.contains(&header.block_id) {
            return Ok(None);
        }

        let packet = EncodingPacket::deserialize(&datagram[HEADER_LEN..]);
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
        let header = DatagramFecHeader {
            block_id: 7,
            transfer_length: 1024,
            source_symbols: 4,
            symbol_size: 256,
        };
        let mut bytes = [0; HEADER_LEN];
        header.encode(&mut bytes).expect("encode header");
        assert_eq!(
            DatagramFecHeader::decode(&bytes).expect("decode header"),
            header
        );
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
}
