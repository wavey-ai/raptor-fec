use crate::{
    EncodedMediaFrame, MediaDatagramRole, MediaDeadlineOutcome, MediaDropReason,
    MediaFecFrameStats, MediaRecoveryAction, MediaRecoveryDecision, MediaSendPlan,
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
    pub dropped_expired_datagrams: u64,
    pub dropped_stale_repair_datagrams: u64,
    pub dropped_in_flight_limit_datagrams: u64,
    pub recovery_noop_decisions: u64,
    pub raptorq_recovery_decisions: u64,
    pub raptorq_extra_repair_symbols: u64,
    pub reliable_fetch_recovery_decisions: u64,
    pub expired_recovery_decisions: u64,
    pub delivery_deadline_hits: u64,
    pub delivery_deadline_misses: u64,
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
        self.dropped_in_flight_limit_datagrams = self
            .dropped_in_flight_limit_datagrams
            .saturating_add(plan.blocked_by_in_flight as u64);
        for dropped in &plan.dropped {
            match dropped.reason {
                MediaDropReason::Expired => {
                    self.dropped_expired_datagrams =
                        self.dropped_expired_datagrams.saturating_add(1);
                }
                MediaDropReason::StaleDeltaRepair => {
                    self.dropped_stale_repair_datagrams =
                        self.dropped_stale_repair_datagrams.saturating_add(1);
                }
                MediaDropReason::InFlightLimit => {
                    // Compatibility for externally constructed legacy plans.
                    if plan.blocked_by_in_flight == 0 {
                        self.dropped_in_flight_limit_datagrams =
                            self.dropped_in_flight_limit_datagrams.saturating_add(1);
                    }
                }
            }
        }
    }

    pub fn record_recovery_decision(&mut self, decision: MediaRecoveryDecision) {
        match decision.action {
            MediaRecoveryAction::NoRecoveryNeeded => {
                self.recovery_noop_decisions = self.recovery_noop_decisions.saturating_add(1);
            }
            MediaRecoveryAction::SendRaptorQRepair { repair_symbols, .. } => {
                self.raptorq_recovery_decisions = self.raptorq_recovery_decisions.saturating_add(1);
                self.raptorq_extra_repair_symbols = self
                    .raptorq_extra_repair_symbols
                    .saturating_add(u64::from(repair_symbols));
            }
            MediaRecoveryAction::ReliableFetch { .. } => {
                self.reliable_fetch_recovery_decisions =
                    self.reliable_fetch_recovery_decisions.saturating_add(1);
            }
            MediaRecoveryAction::Expire => {
                self.expired_recovery_decisions = self.expired_recovery_decisions.saturating_add(1);
            }
        }
    }

    pub fn record_deadline_outcome(&mut self, outcome: MediaDeadlineOutcome) {
        if outcome.deadline_hit {
            self.delivery_deadline_hits = self.delivery_deadline_hits.saturating_add(1);
        } else {
            self.delivery_deadline_misses = self.delivery_deadline_misses.saturating_add(1);
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
    use crate::{
        MediaCodec, MediaDeadline, MediaFecEncoder, MediaFrame, MediaFrameMetadata,
        MediaQueueState, MediaRecoveryInput, MediaRecoveryPolicy, MediaSendPolicy,
    };

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
        let expired_plan = encoded.scheduled_datagram_send_plan(
            MediaSendPolicy::default(),
            MediaQueueState {
                now_ms: 2_000,
                ..MediaQueueState::default()
            },
        );
        counters.record_send_plan(&expired_plan);
        let recovery = MediaRecoveryPolicy::default().decide(MediaRecoveryInput {
            now_us: 100_000,
            deadline: MediaDeadline::from_micros(130_000),
            uncovered_source_symbols: 1,
            secondary_rtt_us: 8_000,
            secondary_queue_delay_us: 0,
            repair_symbol_spacing_us: 250,
            reliable_fetch_estimate_us: Some(10_000),
        });
        counters.record_recovery_decision(recovery);
        counters.record_recovery_decision(MediaRecoveryPolicy::default().decide(
            MediaRecoveryInput {
                now_us: 100_000,
                deadline: MediaDeadline::from_micros(130_000),
                uncovered_source_symbols: 1,
                secondary_rtt_us: 40_000,
                secondary_queue_delay_us: 0,
                repair_symbol_spacing_us: 250,
                reliable_fetch_estimate_us: Some(20_000),
            },
        ));
        counters.record_recovery_decision(MediaRecoveryPolicy::default().decide(
            MediaRecoveryInput {
                now_us: 100_000,
                deadline: MediaDeadline::from_micros(130_000),
                uncovered_source_symbols: 1,
                secondary_rtt_us: 40_000,
                secondary_queue_delay_us: 0,
                repair_symbol_spacing_us: 250,
                reliable_fetch_estimate_us: Some(30_000),
            },
        ));
        counters.record_deadline_outcome(
            MediaDeadline::from_micros(130_000).observe_completion(100_000, 125_000),
        );
        counters.record_deadline_outcome(
            MediaDeadline::from_micros(130_000).observe_completion(100_000, 131_000),
        );
        assert_eq!(counters.encoded_frames, 1);
        assert_eq!(counters.decoded_frames, 1);
        assert_eq!(counters.repaired_source_datagrams, 1);
        assert!(counters.repair_symbols > 0);
        assert!(counters.fec_overhead_bytes > 0);
        assert_eq!(
            counters.dropped_expired_datagrams,
            encoded.datagrams.len() as u64
        );
        assert_eq!(counters.raptorq_recovery_decisions, 1);
        assert_eq!(counters.raptorq_extra_repair_symbols, 2);
        assert_eq!(counters.reliable_fetch_recovery_decisions, 1);
        assert_eq!(counters.expired_recovery_decisions, 1);
        assert_eq!(counters.delivery_deadline_hits, 1);
        assert_eq!(counters.delivery_deadline_misses, 1);
    }
}
