//! Same-epoch multichannel audio session primitives.
//!
//! A sender encodes every channel/stem group for one playout timestamp as one
//! RaptorQ object. A receiver exposes complete systematic groups immediately,
//! uses repair symbols only for missing shards from that timestamp, and returns
//! exact-or-missing epochs at playout. No API in this crate batches audio across
//! timestamps or synthesizes replacement samples.

use bytes::Bytes;
use raptorq_datagram_fec::{
    AudioPayloadKind, AudioSampleFormat, DecodedMultichannelAudioShard,
    EncodedMultichannelAudioEpoch, MultichannelAudioEpoch, MultichannelAudioFecConfig,
    MultichannelAudioFecDecoder, MultichannelAudioFecEncoder, MultichannelAudioFecError,
    MultichannelAudioRecovery, MultichannelAudioShardHeader,
};
use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::fmt;

const DEFAULT_MAX_INFLIGHT_EPOCHS: usize = 64;
const DEFAULT_MAX_BUFFERED_EPOCHS: usize = 256;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MultichannelAudioSessionConfig {
    pub fec: MultichannelAudioFecConfig,
    pub max_inflight_epochs: usize,
    pub max_buffered_epochs: usize,
}

impl Default for MultichannelAudioSessionConfig {
    fn default() -> Self {
        Self {
            fec: MultichannelAudioFecConfig::default(),
            max_inflight_epochs: DEFAULT_MAX_INFLIGHT_EPOCHS,
            max_buffered_epochs: DEFAULT_MAX_BUFFERED_EPOCHS,
        }
    }
}

impl MultichannelAudioSessionConfig {
    pub fn normalized(self) -> Self {
        Self {
            fec: self.fec,
            max_inflight_epochs: self.max_inflight_epochs.max(1),
            max_buffered_epochs: self.max_buffered_epochs.max(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MultichannelAudioSessionStats {
    pub epochs_encoded: u64,
    pub source_datagrams_emitted: u64,
    pub repair_datagrams_emitted: u64,
    pub datagrams_received: u64,
    pub systematic_shards_received: u64,
    pub raptorq_shards_recovered: u64,
    pub groups_completed: u64,
    pub epochs_completed: u64,
    pub exact_epochs_played: u64,
    pub missing_epochs: u64,
    pub incomplete_epochs_dropped: u64,
    pub stale_epochs_dropped: u64,
    pub duplicate_or_late_epochs: u64,
}

#[derive(Debug, Clone)]
pub struct MultichannelAudioSender {
    encoder: MultichannelAudioFecEncoder,
    stats: MultichannelAudioSessionStats,
}

impl MultichannelAudioSender {
    pub fn new(config: MultichannelAudioSessionConfig) -> Self {
        Self {
            encoder: MultichannelAudioFecEncoder::new(config.normalized().fec),
            stats: MultichannelAudioSessionStats::default(),
        }
    }

    /// Select the first FEC block identifier used by this sender.
    pub fn with_initial_block_id(mut self, block_id: u32) -> Self {
        self.encoder.set_block_id(block_id);
        self
    }

    /// Set the FEC block identifier used by the next encoded epoch.
    pub fn set_block_id(&mut self, block_id: u32) {
        self.encoder.set_block_id(block_id);
    }

    /// Return the FEC block identifier that will be used by the next epoch.
    pub fn block_id(&self) -> u32 {
        self.encoder.block_id()
    }

    pub fn fec_config(&self) -> MultichannelAudioFecConfig {
        self.encoder.config()
    }

    pub fn fec_config_mut(&mut self) -> &mut MultichannelAudioFecConfig {
        self.encoder.config_mut()
    }

    pub fn stats(&self) -> MultichannelAudioSessionStats {
        self.stats
    }

    pub fn encode_epoch(
        &mut self,
        epoch: MultichannelAudioEpoch<'_>,
    ) -> Result<EncodedMultichannelAudioEpoch, MultichannelAudioSessionError> {
        let encoded = self.encoder.encode_epoch(epoch)?;
        self.stats.epochs_encoded = self.stats.epochs_encoded.saturating_add(1);
        self.stats.source_datagrams_emitted = self
            .stats
            .source_datagrams_emitted
            .saturating_add(encoded.source_datagram_count() as u64);
        self.stats.repair_datagrams_emitted = self
            .stats
            .repair_datagrams_emitted
            .saturating_add(encoded.repair_datagram_count() as u64);
        Ok(encoded)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedMultichannelAudioGroup {
    pub session_id: u64,
    pub config_generation: u32,
    pub epoch_id: u64,
    pub pts_samples: u64,
    pub sample_rate: u32,
    pub frame_count: u32,
    pub group_count: u16,
    pub group_id: u16,
    pub group_index: u16,
    pub channel_start: u16,
    pub channel_count: u16,
    pub payload_kind: AudioPayloadKind,
    pub sample_format: AudioSampleFormat,
    pub flags: u8,
    pub payload: Bytes,
    pub raptorq_recovered_fragments: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedMultichannelAudioEpoch {
    pub session_id: u64,
    pub config_generation: u32,
    pub epoch_id: u64,
    pub pts_samples: u64,
    pub sample_rate: u32,
    pub frame_count: u32,
    pub groups: Vec<DecodedMultichannelAudioGroup>,
}

impl DecodedMultichannelAudioEpoch {
    pub fn raptorq_recovered_fragments(&self) -> u32 {
        self.groups
            .iter()
            .map(|group| u32::from(group.raptorq_recovered_fragments))
            .sum()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MultichannelAudioReceiveOutcome {
    /// Groups that became complete while processing this datagram.
    pub completed_groups: Vec<DecodedMultichannelAudioGroup>,
    /// Set once every group from this FEC block is exact. This completion event
    /// is independent of the optional one-epoch-per-PTS playout buffer, so two
    /// representations (for example Opus and PCM) may share a PTS without one
    /// suppressing the other's live completion.
    pub completed_epoch: Option<DecodedMultichannelAudioEpoch>,
}

#[derive(Debug)]
pub struct MultichannelAudioReceiver {
    decoder: MultichannelAudioFecDecoder,
    inflight: HashMap<u32, EpochAssembly>,
    inflight_order: VecDeque<u32>,
    ignored_blocks: HashSet<u32>,
    ignored_order: VecDeque<u32>,
    max_inflight_epochs: usize,
    max_ignored_blocks: usize,
    playout: ExactMultichannelAudioPlayoutBuffer,
    stats: MultichannelAudioSessionStats,
}

impl MultichannelAudioReceiver {
    pub fn new(config: MultichannelAudioSessionConfig) -> Self {
        let config = config.normalized();
        Self {
            decoder: MultichannelAudioFecDecoder::new(),
            inflight: HashMap::new(),
            inflight_order: VecDeque::new(),
            ignored_blocks: HashSet::new(),
            ignored_order: VecDeque::new(),
            max_inflight_epochs: config.max_inflight_epochs,
            max_ignored_blocks: config.max_inflight_epochs.saturating_mul(4).max(64),
            playout: ExactMultichannelAudioPlayoutBuffer::new(config.max_buffered_epochs),
            stats: MultichannelAudioSessionStats::default(),
        }
    }

    pub fn stats(&self) -> MultichannelAudioSessionStats {
        self.stats
    }

    pub fn sequence_stats(&self) -> raptorq_datagram_fec::SequenceStats {
        self.decoder.sequence_stats()
    }

    pub fn playout(&self) -> &ExactMultichannelAudioPlayoutBuffer {
        &self.playout
    }

    pub fn push_datagram(
        &mut self,
        datagram: &[u8],
    ) -> Result<MultichannelAudioReceiveOutcome, MultichannelAudioSessionError> {
        self.stats.datagrams_received = self.stats.datagrams_received.saturating_add(1);
        let shards = self.decoder.push_datagram(datagram)?;
        let mut outcome = MultichannelAudioReceiveOutcome::default();

        for shard in shards {
            match shard.recovery {
                MultichannelAudioRecovery::Systematic => {
                    self.stats.systematic_shards_received =
                        self.stats.systematic_shards_received.saturating_add(1);
                }
                MultichannelAudioRecovery::RaptorQ => {
                    self.stats.raptorq_shards_recovered =
                        self.stats.raptorq_shards_recovered.saturating_add(1);
                }
            }

            let block_id = shard.block_id;
            if self.ignored_blocks.contains(&block_id) || self.is_late(shard.header.pts_samples) {
                continue;
            }
            self.ensure_inflight_slot(block_id);

            let assembly = self.inflight.entry(block_id).or_insert_with(|| {
                self.inflight_order.push_back(block_id);
                EpochAssembly::new(block_id, shard.header)
            });
            let completed_group = assembly.push(shard)?;
            if let Some(group) = completed_group {
                self.stats.groups_completed = self.stats.groups_completed.saturating_add(1);
                outcome.completed_groups.push(group);
            }

            if self
                .inflight
                .get(&block_id)
                .is_some_and(EpochAssembly::is_complete)
            {
                let assembly = self
                    .inflight
                    .remove(&block_id)
                    .expect("completed assembly exists");
                self.inflight_order
                    .retain(|candidate| *candidate != block_id);
                let epoch = assembly.finish()?;
                self.stats.epochs_completed = self.stats.epochs_completed.saturating_add(1);
                match self.playout.insert(epoch.clone()) {
                    InsertEpochOutcome::Inserted => {}
                    InsertEpochOutcome::DuplicateOrLate => {
                        self.stats.duplicate_or_late_epochs =
                            self.stats.duplicate_or_late_epochs.saturating_add(1);
                    }
                    InsertEpochOutcome::EvictedStale(count) => {
                        self.stats.stale_epochs_dropped =
                            self.stats.stale_epochs_dropped.saturating_add(count as u64);
                    }
                }
                outcome.completed_epoch = Some(epoch);
            }
        }

        Ok(outcome)
    }

    pub fn take_for_playout(&mut self, pts_samples: u64) -> MultichannelAudioPlayoutRead {
        self.abandon_inflight_through(pts_samples);
        match self.playout.take_for_playout(pts_samples) {
            MultichannelAudioPlayoutRead::Exact(epoch) => {
                self.stats.exact_epochs_played = self.stats.exact_epochs_played.saturating_add(1);
                MultichannelAudioPlayoutRead::Exact(epoch)
            }
            MultichannelAudioPlayoutRead::Missing { pts_samples } => {
                self.stats.missing_epochs = self.stats.missing_epochs.saturating_add(1);
                MultichannelAudioPlayoutRead::Missing { pts_samples }
            }
        }
    }

    pub fn expire_before(&mut self, pts_samples: u64) -> usize {
        let stale_buffered = self.playout.expire_before(pts_samples);
        let stale_blocks: Vec<_> = self
            .inflight
            .iter()
            .filter_map(|(block_id, epoch)| (epoch.pts_samples < pts_samples).then_some(*block_id))
            .collect();
        for block_id in stale_blocks.iter().copied() {
            self.drop_inflight(block_id);
        }
        let dropped = stale_buffered + stale_blocks.len();
        self.stats.stale_epochs_dropped = self
            .stats
            .stale_epochs_dropped
            .saturating_add(dropped as u64);
        dropped
    }

    fn is_late(&self, pts_samples: u64) -> bool {
        self.playout
            .last_played_pts()
            .is_some_and(|last| pts_samples <= last)
    }

    fn ensure_inflight_slot(&mut self, block_id: u32) {
        if self.inflight.contains_key(&block_id) {
            return;
        }
        while self.inflight.len() >= self.max_inflight_epochs {
            let Some(oldest) = self.inflight_order.pop_front() else {
                break;
            };
            if self.inflight.remove(&oldest).is_some() {
                self.stats.incomplete_epochs_dropped =
                    self.stats.incomplete_epochs_dropped.saturating_add(1);
                self.remember_ignored(oldest);
            }
        }
    }

    fn abandon_inflight_through(&mut self, pts_samples: u64) {
        let blocks: Vec<_> = self
            .inflight
            .iter()
            .filter_map(|(block_id, epoch)| (epoch.pts_samples <= pts_samples).then_some(*block_id))
            .collect();
        for block_id in blocks {
            self.drop_inflight(block_id);
            self.stats.incomplete_epochs_dropped =
                self.stats.incomplete_epochs_dropped.saturating_add(1);
        }
    }

    fn drop_inflight(&mut self, block_id: u32) {
        self.inflight.remove(&block_id);
        self.inflight_order
            .retain(|candidate| *candidate != block_id);
        self.remember_ignored(block_id);
    }

    fn remember_ignored(&mut self, block_id: u32) {
        if self.ignored_blocks.insert(block_id) {
            self.ignored_order.push_back(block_id);
        }
        while self.ignored_order.len() > self.max_ignored_blocks {
            if let Some(expired) = self.ignored_order.pop_front() {
                self.ignored_blocks.remove(&expired);
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultichannelAudioPlayoutRead {
    Exact(DecodedMultichannelAudioEpoch),
    Missing { pts_samples: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertEpochOutcome {
    Inserted,
    DuplicateOrLate,
    EvictedStale(usize),
}

#[derive(Debug, Default)]
pub struct ExactMultichannelAudioPlayoutBuffer {
    epochs: BTreeMap<u64, DecodedMultichannelAudioEpoch>,
    max_buffered_epochs: usize,
    last_played_pts: Option<u64>,
}

impl ExactMultichannelAudioPlayoutBuffer {
    pub fn new(max_buffered_epochs: usize) -> Self {
        Self {
            epochs: BTreeMap::new(),
            max_buffered_epochs: max_buffered_epochs.max(1),
            last_played_pts: None,
        }
    }

    pub fn len(&self) -> usize {
        self.epochs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.epochs.is_empty()
    }

    pub fn contains_pts(&self, pts_samples: u64) -> bool {
        self.epochs.contains_key(&pts_samples)
    }

    pub fn last_played_pts(&self) -> Option<u64> {
        self.last_played_pts
    }

    pub fn insert(&mut self, epoch: DecodedMultichannelAudioEpoch) -> InsertEpochOutcome {
        if self
            .last_played_pts
            .is_some_and(|last| epoch.pts_samples <= last)
            || self.epochs.contains_key(&epoch.pts_samples)
        {
            return InsertEpochOutcome::DuplicateOrLate;
        }

        self.epochs.insert(epoch.pts_samples, epoch);
        if self.epochs.len() <= self.max_buffered_epochs {
            return InsertEpochOutcome::Inserted;
        }

        let overflow = self.epochs.len() - self.max_buffered_epochs;
        let stale: Vec<_> = self.epochs.keys().copied().take(overflow).collect();
        for pts_samples in stale {
            self.epochs.remove(&pts_samples);
        }
        InsertEpochOutcome::EvictedStale(overflow)
    }

    pub fn take_for_playout(&mut self, pts_samples: u64) -> MultichannelAudioPlayoutRead {
        self.last_played_pts = Some(pts_samples);
        self.expire_before(pts_samples);
        match self.epochs.remove(&pts_samples) {
            Some(epoch) => MultichannelAudioPlayoutRead::Exact(epoch),
            None => MultichannelAudioPlayoutRead::Missing { pts_samples },
        }
    }

    pub fn expire_before(&mut self, pts_samples: u64) -> usize {
        let stale: Vec<_> = self
            .epochs
            .keys()
            .copied()
            .take_while(|pts| *pts < pts_samples)
            .collect();
        for pts in &stale {
            self.epochs.remove(pts);
        }
        stale.len()
    }
}

#[derive(Debug)]
struct EpochAssembly {
    block_id: u32,
    session_id: u64,
    config_generation: u32,
    epoch_id: u64,
    pts_samples: u64,
    sample_rate: u32,
    frame_count: u32,
    group_count: u16,
    source_count: u16,
    groups: Vec<Option<DecodedMultichannelAudioGroup>>,
    partial_groups: HashMap<u16, GroupAssembly>,
}

impl EpochAssembly {
    fn new(block_id: u32, header: MultichannelAudioShardHeader) -> Self {
        Self {
            block_id,
            session_id: header.session_id,
            config_generation: header.config_generation,
            epoch_id: header.epoch_id,
            pts_samples: header.pts_samples,
            sample_rate: header.sample_rate,
            frame_count: header.frame_count,
            group_count: header.group_count,
            source_count: header.source_count,
            groups: vec![None; usize::from(header.group_count)],
            partial_groups: HashMap::new(),
        }
    }

    fn push(
        &mut self,
        shard: DecodedMultichannelAudioShard,
    ) -> Result<Option<DecodedMultichannelAudioGroup>, MultichannelAudioSessionError> {
        self.validate_header(shard.header)?;
        let group_index = shard.header.group_index;
        if self.groups[usize::from(group_index)].is_some() {
            return Ok(None);
        }

        let group = self
            .partial_groups
            .entry(group_index)
            .or_insert_with(|| GroupAssembly::new(self.block_id, shard.header));
        let Some(completed) = group.push(shard)? else {
            return Ok(None);
        };
        self.partial_groups.remove(&group_index);
        self.groups[usize::from(group_index)] = Some(completed.clone());
        Ok(Some(completed))
    }

    fn validate_header(
        &self,
        header: MultichannelAudioShardHeader,
    ) -> Result<(), MultichannelAudioSessionError> {
        macro_rules! require_equal {
            ($field:ident) => {
                if self.$field != header.$field {
                    return Err(MultichannelAudioSessionError::EpochMetadataMismatch {
                        block_id: self.block_id,
                        field: stringify!($field),
                    });
                }
            };
        }
        require_equal!(session_id);
        require_equal!(config_generation);
        require_equal!(epoch_id);
        require_equal!(pts_samples);
        require_equal!(sample_rate);
        require_equal!(frame_count);
        require_equal!(group_count);
        require_equal!(source_count);
        Ok(())
    }

    fn is_complete(&self) -> bool {
        self.groups.iter().all(Option::is_some)
    }

    fn finish(self) -> Result<DecodedMultichannelAudioEpoch, MultichannelAudioSessionError> {
        let groups = self.groups.into_iter().collect::<Option<Vec<_>>>().ok_or(
            MultichannelAudioSessionError::IncompleteEpoch {
                block_id: self.block_id,
            },
        )?;
        Ok(DecodedMultichannelAudioEpoch {
            session_id: self.session_id,
            config_generation: self.config_generation,
            epoch_id: self.epoch_id,
            pts_samples: self.pts_samples,
            sample_rate: self.sample_rate,
            frame_count: self.frame_count,
            groups,
        })
    }
}

#[derive(Debug)]
struct GroupAssembly {
    block_id: u32,
    header: MultichannelAudioShardHeader,
    fragments: Vec<Option<Fragment>>,
    recovered_fragments: u16,
}

impl GroupAssembly {
    fn new(block_id: u32, header: MultichannelAudioShardHeader) -> Self {
        Self {
            block_id,
            header,
            fragments: vec![None; usize::from(header.fragment_count)],
            recovered_fragments: 0,
        }
    }

    fn push(
        &mut self,
        shard: DecodedMultichannelAudioShard,
    ) -> Result<Option<DecodedMultichannelAudioGroup>, MultichannelAudioSessionError> {
        self.validate_header(shard.header)?;
        let fragment_index = usize::from(shard.header.fragment_index);
        if self.fragments[fragment_index].is_some() {
            return Ok(None);
        }
        if matches!(shard.recovery, MultichannelAudioRecovery::RaptorQ) {
            self.recovered_fragments = self.recovered_fragments.saturating_add(1);
        }
        self.fragments[fragment_index] = Some(Fragment {
            offset: shard.header.payload_offset,
            payload: shard.payload,
        });
        if self.fragments.iter().any(Option::is_none) {
            return Ok(None);
        }

        let mut fragments = self.fragments.iter().flatten().cloned().collect::<Vec<_>>();
        fragments.sort_by_key(|fragment| fragment.offset);
        let mut payload = Vec::with_capacity(self.header.group_payload_len as usize);
        for fragment in fragments {
            if fragment.offset as usize != payload.len() {
                return Err(MultichannelAudioSessionError::NonContiguousGroupPayload {
                    block_id: shard.block_id,
                    group_index: self.header.group_index,
                });
            }
            payload.extend_from_slice(&fragment.payload);
        }
        if payload.len() != self.header.group_payload_len as usize {
            return Err(MultichannelAudioSessionError::NonContiguousGroupPayload {
                block_id: shard.block_id,
                group_index: self.header.group_index,
            });
        }

        Ok(Some(DecodedMultichannelAudioGroup {
            session_id: self.header.session_id,
            config_generation: self.header.config_generation,
            epoch_id: self.header.epoch_id,
            pts_samples: self.header.pts_samples,
            sample_rate: self.header.sample_rate,
            frame_count: self.header.frame_count,
            group_count: self.header.group_count,
            group_id: self.header.group_id,
            group_index: self.header.group_index,
            channel_start: self.header.channel_start,
            channel_count: self.header.channel_count,
            payload_kind: self.header.payload_kind,
            sample_format: self.header.sample_format,
            flags: self.header.flags,
            payload: Bytes::from(payload),
            raptorq_recovered_fragments: self.recovered_fragments,
        }))
    }

    fn validate_header(
        &self,
        header: MultichannelAudioShardHeader,
    ) -> Result<(), MultichannelAudioSessionError> {
        macro_rules! require_equal {
            ($field:ident) => {
                if self.header.$field != header.$field {
                    return Err(MultichannelAudioSessionError::GroupMetadataMismatch {
                        block_id: self.block_id,
                        group_index: self.header.group_index,
                        field: stringify!($field),
                    });
                }
            };
        }
        require_equal!(group_id);
        require_equal!(group_index);
        require_equal!(channel_start);
        require_equal!(channel_count);
        require_equal!(payload_kind);
        require_equal!(sample_format);
        require_equal!(flags);
        require_equal!(fragment_count);
        require_equal!(group_payload_len);
        Ok(())
    }
}

#[derive(Debug, Clone)]
struct Fragment {
    offset: u32,
    payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MultichannelAudioSessionError {
    Fec(MultichannelAudioFecError),
    EpochMetadataMismatch {
        block_id: u32,
        field: &'static str,
    },
    GroupMetadataMismatch {
        block_id: u32,
        group_index: u16,
        field: &'static str,
    },
    NonContiguousGroupPayload {
        block_id: u32,
        group_index: u16,
    },
    IncompleteEpoch {
        block_id: u32,
    },
}

impl From<MultichannelAudioFecError> for MultichannelAudioSessionError {
    fn from(error: MultichannelAudioFecError) -> Self {
        Self::Fec(error)
    }
}

impl fmt::Display for MultichannelAudioSessionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Fec(error) => write!(formatter, "{error}"),
            Self::EpochMetadataMismatch { block_id, field } => {
                write!(
                    formatter,
                    "audio epoch block {block_id} has inconsistent {field}"
                )
            }
            Self::GroupMetadataMismatch {
                block_id,
                group_index,
                field,
            } => write!(
                formatter,
                "audio epoch block {block_id} group {group_index} has inconsistent {field}"
            ),
            Self::NonContiguousGroupPayload {
                block_id,
                group_index,
            } => write!(
                formatter,
                "audio epoch block {block_id} group {group_index} payload is not contiguous"
            ),
            Self::IncompleteEpoch { block_id } => {
                write!(formatter, "audio epoch block {block_id} is incomplete")
            }
        }
    }
}

impl std::error::Error for MultichannelAudioSessionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use raptorq_datagram_fec::MultichannelAudioGroup;

    #[test]
    fn sender_encodes_each_timestamp_immediately_and_source_first() {
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig::default());
        let payload = test_payload(7, 2_000);
        let groups = [pcm_group(0, 0, 2, &payload)];
        let encoded = sender.encode_epoch(epoch(1, 0, &groups)).unwrap();

        assert!(encoded.source_datagram_count() > 1);
        assert_eq!(encoded.repair_datagram_count(), 4);
        assert!(encoded.datagrams[..encoded.source_datagram_count()]
            .iter()
            .all(|packet| matches!(
                packet.role,
                raptorq_datagram_fec::MultichannelAudioDatagramRole::Source { .. }
            )));
        assert_eq!(sender.stats().epochs_encoded, 1);
    }

    #[test]
    fn receiver_emits_complete_systematic_groups_before_epoch_completion() {
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig::default());
        let first = test_payload(1, 128);
        let second = test_payload(2, 128);
        let groups = [pcm_group(10, 0, 2, &first), pcm_group(11, 2, 2, &second)];
        let encoded = sender.encode_epoch(epoch(2, 240, &groups)).unwrap();
        let mut receiver =
            MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default());

        let first_outcome = receiver
            .push_datagram(&encoded.datagrams[0].payload)
            .unwrap();
        assert_eq!(first_outcome.completed_groups.len(), 1);
        assert_eq!(first_outcome.completed_groups[0].payload.as_ref(), first);
        assert!(first_outcome.completed_epoch.is_none());

        let second_outcome = receiver
            .push_datagram(&encoded.datagrams[1].payload)
            .unwrap();
        assert_eq!(second_outcome.completed_groups.len(), 1);
        assert!(second_outcome.completed_epoch.is_some());
    }

    #[test]
    fn same_epoch_raptorq_loss_recovery_reaches_exact_playout() {
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig::default());
        let payloads = (0..8)
            .map(|group| test_payload(group as u8, 1_700))
            .collect::<Vec<_>>();
        let groups = payloads
            .iter()
            .enumerate()
            .map(|(index, payload)| pcm_group(index as u16, index as u16 * 2, 2, payload))
            .collect::<Vec<_>>();
        let encoded = sender.encode_epoch(epoch(3, 480, &groups)).unwrap();
        let mut receiver =
            MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default());

        let dropped_sources = [1usize, 7, 11];
        for (index, datagram) in encoded.datagrams.iter().enumerate() {
            if !dropped_sources.contains(&index) {
                receiver.push_datagram(&datagram.payload).unwrap();
            }
        }

        let exact = match receiver.take_for_playout(480) {
            MultichannelAudioPlayoutRead::Exact(epoch) => epoch,
            MultichannelAudioPlayoutRead::Missing { .. } => panic!("expected recovered epoch"),
        };
        assert_eq!(exact.groups.len(), groups.len());
        for (decoded, expected) in exact.groups.iter().zip(payloads.iter()) {
            assert_eq!(decoded.payload.as_ref(), expected.as_slice());
        }
        assert_eq!(exact.raptorq_recovered_fragments(), 3);
        assert_eq!(receiver.stats().raptorq_shards_recovered, 3);
    }

    #[test]
    fn playout_deadline_abandons_incomplete_epoch_and_rejects_late_repair() {
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig::default());
        let payload = test_payload(9, 2_500);
        let groups = [pcm_group(0, 0, 2, &payload)];
        let encoded = sender.encode_epoch(epoch(4, 720, &groups)).unwrap();
        let mut receiver =
            MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default());

        receiver
            .push_datagram(&encoded.datagrams[0].payload)
            .unwrap();
        assert_eq!(
            receiver.take_for_playout(720),
            MultichannelAudioPlayoutRead::Missing { pts_samples: 720 }
        );
        for datagram in &encoded.datagrams[1..] {
            let outcome = receiver.push_datagram(&datagram.payload).unwrap();
            assert!(outcome.completed_epoch.is_none());
        }
        assert_eq!(receiver.stats().missing_epochs, 1);
        assert_eq!(receiver.stats().incomplete_epochs_dropped, 1);
    }

    #[test]
    fn live_completion_reports_distinct_blocks_that_share_a_pts() {
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig::default());
        let first_payload = test_payload(1, 200);
        let second_payload = test_payload(2, 200);
        let first_groups = [pcm_group(0, 0, 2, &first_payload)];
        let second_groups = [pcm_group(1, 2, 2, &second_payload)];
        let first = sender.encode_epoch(epoch(20, 960, &first_groups)).unwrap();
        let second = sender.encode_epoch(epoch(21, 960, &second_groups)).unwrap();
        let mut receiver =
            MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default());

        let mut first_completion = None;
        for datagram in &first.datagrams {
            first_completion = receiver
                .push_datagram(&datagram.payload)
                .unwrap()
                .completed_epoch
                .or(first_completion);
        }
        let mut second_completion = None;
        for datagram in &second.datagrams {
            second_completion = receiver
                .push_datagram(&datagram.payload)
                .unwrap()
                .completed_epoch
                .or(second_completion);
        }

        assert_eq!(first_completion.unwrap().epoch_id, 20);
        assert_eq!(second_completion.unwrap().epoch_id, 21);
        assert_eq!(receiver.stats().epochs_completed, 2);
        assert_eq!(receiver.stats().duplicate_or_late_epochs, 1);
    }

    fn epoch<'a>(
        epoch_id: u64,
        pts_samples: u64,
        groups: &'a [MultichannelAudioGroup<'a>],
    ) -> MultichannelAudioEpoch<'a> {
        MultichannelAudioEpoch {
            session_id: 42,
            config_generation: 1,
            epoch_id,
            pts_samples,
            sample_rate: 48_000,
            frame_count: 240,
            groups,
        }
    }

    fn pcm_group<'a>(
        group_id: u16,
        channel_start: u16,
        channel_count: u16,
        payload: &'a [u8],
    ) -> MultichannelAudioGroup<'a> {
        MultichannelAudioGroup {
            group_id,
            channel_start,
            channel_count,
            payload_kind: AudioPayloadKind::Pcm,
            sample_format: AudioSampleFormat::S24Le,
            flags: 0,
            payload,
        }
    }

    fn test_payload(seed: u8, len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| seed.wrapping_add(index as u8))
            .collect()
    }
}
