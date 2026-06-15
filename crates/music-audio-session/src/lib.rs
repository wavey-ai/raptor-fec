//! Exact-deadline music audio session primitives.
//!
//! This crate is the caller layer above `raptorq-datagram-fec`'s music
//! micro-block API. It deliberately does not open sockets, talk to an audio
//! device, or synthesize missing audio. A transport adapter supplies datagrams,
//! and an audio callback asks the playout buffer for exact chunks by sample PTS.

use bytes::Bytes;
use raptorq_datagram_fec::{
    DecodedMusicAudioFrame, MusicAudioFecError, MusicAudioFrame, MusicAudioMicroBlockConfig,
    MusicAudioMicroBlockDecoder, MusicAudioMicroBlockEncoder,
};
use std::collections::BTreeMap;
use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MusicAudioSessionConfig {
    pub micro_block: MusicAudioMicroBlockConfig,
    pub playout_delay_samples: u32,
    pub max_buffered_frames: usize,
}

impl MusicAudioSessionConfig {
    pub fn pcm48_stereo_2_5ms(stream_id: u64) -> Self {
        Self {
            micro_block: MusicAudioMicroBlockConfig::pcm48_stereo_2_5ms(stream_id),
            playout_delay_samples: 480,
            max_buffered_frames: 256,
        }
    }

    pub fn pcm48_stereo_5ms(stream_id: u64) -> Self {
        Self {
            micro_block: MusicAudioMicroBlockConfig::pcm48_stereo_5ms(stream_id),
            playout_delay_samples: 480,
            max_buffered_frames: 256,
        }
    }

    pub fn normalized(self) -> Self {
        Self {
            micro_block: self.micro_block.normalized(),
            playout_delay_samples: self.playout_delay_samples.max(1),
            max_buffered_frames: self.max_buffered_frames.max(1),
        }
    }
}

impl Default for MusicAudioSessionConfig {
    fn default() -> Self {
        Self::pcm48_stereo_2_5ms(0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapturedAudioChunk {
    pub pts_samples: u64,
    pub payload: Bytes,
}

impl CapturedAudioChunk {
    pub fn new(pts_samples: u64, payload: impl Into<Bytes>) -> Self {
        Self {
            pts_samples,
            payload: payload.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutboundMusicAudioDatagram {
    pub sequence: u64,
    pub first_pts_samples: u64,
    pub datagram_index: usize,
    pub datagram_count: usize,
    pub payload: Bytes,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MusicAudioSessionStats {
    pub chunks_accepted: u64,
    pub fec_blocks_emitted: u64,
    pub datagrams_emitted: u64,
    pub datagrams_received: u64,
    pub fec_blocks_recovered: u64,
    pub frames_buffered: u64,
    pub exact_frames_played: u64,
    pub missing_frames: u64,
    pub stale_frames_dropped: u64,
    pub duplicate_or_late_frames: u64,
}

#[derive(Debug, Clone)]
pub struct MusicAudioSender {
    encoder: MusicAudioMicroBlockEncoder,
    next_sequence: u64,
    stats: MusicAudioSessionStats,
}

impl MusicAudioSender {
    pub fn new(config: MusicAudioSessionConfig) -> Self {
        Self {
            encoder: MusicAudioMicroBlockEncoder::new(config.normalized().micro_block),
            next_sequence: 0,
            stats: MusicAudioSessionStats::default(),
        }
    }

    pub fn next_sequence(&self) -> u64 {
        self.next_sequence
    }

    pub fn pending_frames(&self) -> usize {
        self.encoder.pending_frames()
    }

    pub fn stats(&self) -> MusicAudioSessionStats {
        self.stats
    }

    pub fn push_chunk(
        &mut self,
        chunk: CapturedAudioChunk,
    ) -> Result<Vec<OutboundMusicAudioDatagram>, MusicAudioSessionError> {
        let sequence = self.next_sequence;
        self.next_sequence = self.next_sequence.wrapping_add(1);
        self.stats.chunks_accepted = self.stats.chunks_accepted.saturating_add(1);

        let encoded = self.encoder.push_frame(MusicAudioFrame {
            sequence,
            pts_samples: chunk.pts_samples,
            payload: &chunk.payload,
        })?;

        Ok(self.outbound_datagrams(encoded))
    }

    pub fn flush(&mut self) -> Result<Vec<OutboundMusicAudioDatagram>, MusicAudioSessionError> {
        let encoded = self.encoder.flush()?;
        Ok(self.outbound_datagrams(encoded))
    }

    fn outbound_datagrams(
        &mut self,
        encoded: Option<raptorq_datagram_fec::EncodedMusicAudioMicroBlock>,
    ) -> Vec<OutboundMusicAudioDatagram> {
        let Some(encoded) = encoded else {
            return Vec::new();
        };

        let datagrams = encoded_block_to_outbound_datagrams(encoded);
        self.stats.fec_blocks_emitted = self.stats.fec_blocks_emitted.saturating_add(1);
        self.stats.datagrams_emitted = self
            .stats
            .datagrams_emitted
            .saturating_add(datagrams.len() as u64);
        datagrams
    }
}

fn encoded_block_to_outbound_datagrams(
    encoded: raptorq_datagram_fec::EncodedMusicAudioMicroBlock,
) -> Vec<OutboundMusicAudioDatagram> {
    let datagram_count = encoded.datagrams.len();
    encoded
        .datagrams
        .into_iter()
        .enumerate()
        .map(|(datagram_index, payload)| OutboundMusicAudioDatagram {
            sequence: encoded.first_sequence,
            first_pts_samples: encoded.first_pts_samples,
            datagram_index,
            datagram_count,
            payload: Bytes::from(payload),
        })
        .collect()
}

#[derive(Debug)]
pub struct MusicAudioReceiver {
    decoder: MusicAudioMicroBlockDecoder,
    playout: ExactPlayoutBuffer,
    stats: MusicAudioSessionStats,
}

impl MusicAudioReceiver {
    pub fn new(config: MusicAudioSessionConfig) -> Self {
        Self {
            decoder: MusicAudioMicroBlockDecoder::new(),
            playout: ExactPlayoutBuffer::new(config.normalized().max_buffered_frames),
            stats: MusicAudioSessionStats::default(),
        }
    }

    pub fn stats(&self) -> MusicAudioSessionStats {
        self.stats
    }

    pub fn playout(&self) -> &ExactPlayoutBuffer {
        &self.playout
    }

    pub fn playout_mut(&mut self) -> &mut ExactPlayoutBuffer {
        &mut self.playout
    }

    pub fn push_datagram(
        &mut self,
        datagram: &[u8],
    ) -> Result<Option<Vec<DecodedMusicAudioFrame>>, MusicAudioSessionError> {
        self.stats.datagrams_received = self.stats.datagrams_received.saturating_add(1);
        let Some(block) = self.decoder.push_datagram(datagram)? else {
            return Ok(None);
        };

        self.stats.fec_blocks_recovered = self.stats.fec_blocks_recovered.saturating_add(1);
        let mut frames = Vec::with_capacity(block.frames.len());
        for frame in block.frames {
            match self.playout.insert(frame.clone()) {
                InsertFrameOutcome::Inserted => {
                    self.stats.frames_buffered = self.stats.frames_buffered.saturating_add(1);
                }
                InsertFrameOutcome::DuplicateOrLate => {
                    self.stats.duplicate_or_late_frames =
                        self.stats.duplicate_or_late_frames.saturating_add(1);
                }
                InsertFrameOutcome::EvictedStale(count) => {
                    self.stats.frames_buffered = self.stats.frames_buffered.saturating_add(1);
                    self.stats.stale_frames_dropped =
                        self.stats.stale_frames_dropped.saturating_add(count as u64);
                }
            }
            frames.push(frame);
        }

        Ok(Some(frames))
    }

    pub fn take_for_playout(&mut self, pts_samples: u64) -> PlayoutRead {
        match self.playout.take_for_playout(pts_samples) {
            PlayoutRead::Exact(frame) => {
                self.stats.exact_frames_played = self.stats.exact_frames_played.saturating_add(1);
                PlayoutRead::Exact(frame)
            }
            PlayoutRead::Missing { pts_samples } => {
                self.stats.missing_frames = self.stats.missing_frames.saturating_add(1);
                PlayoutRead::Missing { pts_samples }
            }
        }
    }

    pub fn expire_before(&mut self, pts_samples: u64) -> usize {
        let dropped = self.playout.expire_before(pts_samples);
        self.stats.stale_frames_dropped = self
            .stats
            .stale_frames_dropped
            .saturating_add(dropped as u64);
        dropped
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PlayoutRead {
    Exact(DecodedMusicAudioFrame),
    Missing { pts_samples: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertFrameOutcome {
    Inserted,
    DuplicateOrLate,
    EvictedStale(usize),
}

#[derive(Debug, Default)]
pub struct ExactPlayoutBuffer {
    frames: BTreeMap<u64, DecodedMusicAudioFrame>,
    max_buffered_frames: usize,
    last_played_pts: Option<u64>,
}

impl ExactPlayoutBuffer {
    pub fn new(max_buffered_frames: usize) -> Self {
        Self {
            frames: BTreeMap::new(),
            max_buffered_frames: max_buffered_frames.max(1),
            last_played_pts: None,
        }
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn contains_pts(&self, pts_samples: u64) -> bool {
        self.frames.contains_key(&pts_samples)
    }

    pub fn insert(&mut self, frame: DecodedMusicAudioFrame) -> InsertFrameOutcome {
        if self
            .last_played_pts
            .map(|last| frame.pts_samples <= last)
            .unwrap_or(false)
            || self.frames.contains_key(&frame.pts_samples)
        {
            return InsertFrameOutcome::DuplicateOrLate;
        }

        self.frames.insert(frame.pts_samples, frame);
        if self.frames.len() <= self.max_buffered_frames {
            return InsertFrameOutcome::Inserted;
        }

        let overflow = self.frames.len() - self.max_buffered_frames;
        for pts in self
            .frames
            .keys()
            .copied()
            .take(overflow)
            .collect::<Vec<_>>()
        {
            self.frames.remove(&pts);
        }
        InsertFrameOutcome::EvictedStale(overflow)
    }

    pub fn take_for_playout(&mut self, pts_samples: u64) -> PlayoutRead {
        self.last_played_pts = Some(pts_samples);
        self.expire_before(pts_samples);

        match self.frames.remove(&pts_samples) {
            Some(frame) => PlayoutRead::Exact(frame),
            None => PlayoutRead::Missing { pts_samples },
        }
    }

    pub fn expire_before(&mut self, pts_samples: u64) -> usize {
        let stale: Vec<_> = self
            .frames
            .keys()
            .copied()
            .take_while(|pts| *pts < pts_samples)
            .collect();
        let count = stale.len();
        for pts in stale {
            self.frames.remove(&pts);
        }
        count
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MusicAudioSessionError {
    Fec(MusicAudioFecError),
}

impl From<MusicAudioFecError> for MusicAudioSessionError {
    fn from(error: MusicAudioFecError) -> Self {
        Self::Fec(error)
    }
}

impl fmt::Display for MusicAudioSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fec(error) => write!(formatter, "{error}"),
        }
    }
}

impl std::error::Error for MusicAudioSessionError {}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn session_recovers_exact_music_chunks_into_playout_buffer() {
        let config = MusicAudioSessionConfig::pcm48_stereo_2_5ms(9);
        let mut sender = MusicAudioSender::new(config);
        let mut datagrams = Vec::new();

        for index in 0..4 {
            let emitted = sender
                .push_chunk(CapturedAudioChunk::new(
                    index * 120,
                    test_payload(index as u8, 960),
                ))
                .unwrap();
            datagrams.extend(emitted);
        }

        assert_eq!(datagrams.len(), 5);
        assert_eq!(sender.stats().chunks_accepted, 4);
        assert_eq!(sender.stats().fec_blocks_emitted, 1);
        assert_eq!(sender.stats().datagrams_emitted, 5);

        let mut receiver = MusicAudioReceiver::new(config);
        for datagram in datagrams
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != 1)
            .map(|(_, datagram)| datagram)
        {
            receiver.push_datagram(&datagram.payload).unwrap();
        }

        for index in 0..4 {
            match receiver.take_for_playout(index * 120) {
                PlayoutRead::Exact(frame) => {
                    assert_eq!(frame.sequence, index);
                    assert_eq!(frame.pts_samples, index * 120);
                    assert_eq!(frame.payload.as_ref(), test_payload(index as u8, 960));
                }
                PlayoutRead::Missing { pts_samples } => {
                    panic!("expected exact frame at {pts_samples}");
                }
            }
        }
        assert_eq!(receiver.stats().fec_blocks_recovered, 1);
        assert_eq!(receiver.stats().exact_frames_played, 4);
        assert_eq!(receiver.stats().missing_frames, 0);
    }

    #[test]
    fn receiver_reports_missing_instead_of_faking_audio() {
        let config = MusicAudioSessionConfig::pcm48_stereo_2_5ms(1);
        let mut sender = MusicAudioSender::new(config);
        let mut datagrams = Vec::new();
        for index in 0..4 {
            datagrams.extend(
                sender
                    .push_chunk(CapturedAudioChunk::new(
                        index * 120,
                        test_payload(index as u8, 960),
                    ))
                    .unwrap(),
            );
        }

        let mut receiver = MusicAudioReceiver::new(config);
        for datagram in datagrams
            .iter()
            .enumerate()
            .filter(|(index, _)| *index >= 2)
            .map(|(_, datagram)| datagram)
        {
            receiver.push_datagram(&datagram.payload).unwrap();
        }

        assert_eq!(
            receiver.take_for_playout(0),
            PlayoutRead::Missing { pts_samples: 0 }
        );
        assert_eq!(receiver.stats().missing_frames, 1);
        assert_eq!(receiver.stats().exact_frames_played, 0);
    }

    #[test]
    fn playout_buffer_expires_stale_frames_and_rejects_late_insert() {
        let mut buffer = ExactPlayoutBuffer::new(2);
        assert_eq!(
            buffer.insert(decoded_frame(0)),
            InsertFrameOutcome::Inserted
        );
        assert_eq!(
            buffer.insert(decoded_frame(120)),
            InsertFrameOutcome::Inserted
        );
        assert_eq!(
            buffer.insert(decoded_frame(240)),
            InsertFrameOutcome::EvictedStale(1)
        );
        assert!(!buffer.contains_pts(0));

        assert_eq!(buffer.expire_before(240), 1);
        assert!(!buffer.contains_pts(120));
        assert_eq!(
            buffer.take_for_playout(240),
            PlayoutRead::Exact(decoded_frame(240))
        );
        assert_eq!(
            buffer.insert(decoded_frame(120)),
            InsertFrameOutcome::DuplicateOrLate
        );
    }

    fn decoded_frame(pts_samples: u64) -> DecodedMusicAudioFrame {
        DecodedMusicAudioFrame {
            sequence: pts_samples / 120,
            pts_samples,
            payload: Bytes::from(test_payload((pts_samples / 120) as u8, 16)),
        }
    }

    fn test_payload(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| seed.wrapping_add(index as u8))
            .collect()
    }
}
