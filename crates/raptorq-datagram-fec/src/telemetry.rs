use crate::{
    EncodedMediaFrame, MediaDatagramRole, MediaDropReason, MediaFecFrameStats, MediaSendPlan,
};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MediaFecLossOutcome {
    pub lost_source_datagrams: usize,
    pub lost_repair_datagrams: usize,
    pub repaired_source_datagrams: usize,
    pub unused_repair_datagrams: usize,
    pub failed_blocks: usize,
    pub decoded: bool,
    pub decode_deadline_missed: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MediaFecRepairCounters {
    pub encoded_frames: u64,
    pub decoded_frames: u64,
    pub failed_frames: u64,
    pub source_symbols: u64,
    pub repair_symbols: u64,
    pub wire_datagrams: u64,
    pub protected_bytes: u64,
    pub wire_bytes: u64,
    pub fec_overhead_bytes: u64,
    pub lost_source_datagrams: u64,
    pub lost_repair_datagrams: u64,
    pub repaired_source_datagrams: u64,
    pub unused_repair_datagrams: u64,
    pub failed_blocks: u64,
    pub decode_deadline_misses: u64,
    pub dropped_stale_repair_datagrams: u64,
    pub dropped_in_flight_limit_datagrams: u64,
    pub backfill_requests: u64,
    pub backfill_hits: u64,
    pub backfill_misses: u64,
}

impl MediaFecRepairCounters {
    pub fn record_encoded_frame(&mut self, frame: &EncodedMediaFrame) {
        self.record_frame_stats(frame.stats());
    }

    pub fn record_frame_stats(&mut self, stats: MediaFecFrameStats) {
        self.encoded_frames = self.encoded_frames.saturating_add(1);
        self.source_symbols = self
            .source_symbols
            .saturating_add(stats.source_datagrams as u64);
        self.repair_symbols = self
            .repair_symbols
            .saturating_add(stats.repair_datagrams as u64);
        self.wire_datagrams = self
            .wire_datagrams
            .saturating_add(stats.wire_datagrams as u64);
        self.protected_bytes = self
            .protected_bytes
            .saturating_add(stats.protected_bytes as u64);
        self.wire_bytes = self.wire_bytes.saturating_add(stats.wire_bytes as u64);
        self.fec_overhead_bytes = self
            .fec_overhead_bytes
            .saturating_add(stats.overhead_bytes() as u64);
    }

    pub fn record_loss_outcome(&mut self, outcome: MediaFecLossOutcome) {
        if outcome.decoded {
            self.decoded_frames = self.decoded_frames.saturating_add(1);
        } else {
            self.failed_frames = self.failed_frames.saturating_add(1);
        }
        self.lost_source_datagrams = self
            .lost_source_datagrams
            .saturating_add(outcome.lost_source_datagrams as u64);
        self.lost_repair_datagrams = self
            .lost_repair_datagrams
            .saturating_add(outcome.lost_repair_datagrams as u64);
        self.repaired_source_datagrams = self
            .repaired_source_datagrams
            .saturating_add(outcome.repaired_source_datagrams as u64);
        self.unused_repair_datagrams = self
            .unused_repair_datagrams
            .saturating_add(outcome.unused_repair_datagrams as u64);
        self.failed_blocks = self
            .failed_blocks
            .saturating_add(outcome.failed_blocks as u64);
        if outcome.decode_deadline_missed {
            self.decode_deadline_misses = self.decode_deadline_misses.saturating_add(1);
        }
    }

    pub fn record_send_plan(&mut self, plan: &MediaSendPlan) {
        for dropped in &plan.dropped {
            match dropped.reason {
                MediaDropReason::StaleDeltaRepair => {
                    self.dropped_stale_repair_datagrams =
                        self.dropped_stale_repair_datagrams.saturating_add(1);
                }
                MediaDropReason::InFlightLimit => {
                    self.dropped_in_flight_limit_datagrams =
                        self.dropped_in_flight_limit_datagrams.saturating_add(1);
                }
            }
        }
    }

    pub fn record_backfill_result(&mut self, hit: bool) {
        self.backfill_requests = self.backfill_requests.saturating_add(1);
        if hit {
            self.backfill_hits = self.backfill_hits.saturating_add(1);
        } else {
            self.backfill_misses = self.backfill_misses.saturating_add(1);
        }
    }
}

impl EncodedMediaFrame {
    pub fn loss_outcome<I>(
        &self,
        dropped_datagram_indices: I,
        decoded: bool,
        decode_deadline_missed: bool,
    ) -> MediaFecLossOutcome
    where
        I: IntoIterator<Item = usize>,
    {
        let dropped = dropped_datagram_indices.into_iter().collect::<HashSet<_>>();
        let mut outcome = MediaFecLossOutcome {
            decoded,
            decode_deadline_missed,
            ..MediaFecLossOutcome::default()
        };

        for block in &self.blocks {
            let lost_source = block
                .source_datagram_indices()
                .filter(|index| dropped.contains(index))
                .count();
            let lost_repair = block
                .repair_datagram_indices()
                .filter(|index| dropped.contains(index))
                .count();
            let repair_symbols = block.repair_symbols as usize;
            let available_repair = repair_symbols.saturating_sub(lost_repair);

            outcome.lost_source_datagrams += lost_source;
            outcome.lost_repair_datagrams += lost_repair;
            if lost_source > available_repair {
                outcome.failed_blocks += 1;
            }
            if decoded {
                outcome.repaired_source_datagrams += lost_source;
                outcome.unused_repair_datagrams += available_repair.saturating_sub(lost_source);
            }
        }

        outcome
    }

    pub fn datagram_loss_outcome<I>(
        &self,
        dropped_datagram_indices: I,
        decoded: bool,
    ) -> MediaFecLossOutcome
    where
        I: IntoIterator<Item = usize>,
    {
        self.loss_outcome(dropped_datagram_indices, decoded, false)
    }

    pub fn lost_datagram_roles<I>(&self, dropped_datagram_indices: I) -> (usize, usize)
    where
        I: IntoIterator<Item = usize>,
    {
        dropped_datagram_indices
            .into_iter()
            .fold((0, 0), |(source, repair), index| {
                match self.datagram_role(index) {
                    Some(MediaDatagramRole::Source) => (source + 1, repair),
                    Some(MediaDatagramRole::Repair) => (source, repair + 1),
                    None => (source, repair),
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MediaCodec, MediaFecEncoder, MediaFrame, MediaFrameMetadata};

    #[test]
    fn counters_record_repaired_loss_unused_repair_and_plan_drops() {
        let mut encoder = MediaFecEncoder::default();
        let payload = vec![0x55; 18_000];
        let metadata =
            MediaFrameMetadata::new(5, encoder.allocate_sequence(), 1_000, MediaCodec::H264);
        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode");
        let dropped_source = encoded.blocks[0].source_datagram_indices().start;
        let outcome = encoded.loss_outcome([dropped_source], true, false);

        assert_eq!(outcome.lost_source_datagrams, 1);
        assert_eq!(outcome.repaired_source_datagrams, 1);
        assert_eq!(outcome.failed_blocks, 0);

        let mut counters = MediaFecRepairCounters::default();
        counters.record_encoded_frame(&encoded);
        counters.record_loss_outcome(outcome);
        assert_eq!(counters.encoded_frames, 1);
        assert_eq!(counters.decoded_frames, 1);
        assert_eq!(counters.repaired_source_datagrams, 1);
        assert!(counters.repair_symbols > 0);
        assert!(counters.fec_overhead_bytes > 0);
    }
}
