use std::collections::{HashSet, VecDeque};

/// How far behind the newest sequence number a missing one is still awaited.
/// A sequence number that is further behind is counted as lost and forgotten.
const MAX_TRACKED_GAP: u32 = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SequenceObservation {
    pub sequence: u32,
    pub expected: u32,
    pub missing_before: u32,
    pub duplicate_or_reordered: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SequenceStats {
    pub received: u64,
    pub missing: u64,
    pub duplicate_or_reordered: u64,
    pub highest_seen: Option<u32>,
}

impl SequenceStats {
    pub fn loss_fraction(self) -> f32 {
        let total = self.received.saturating_add(self.missing);
        if total == 0 {
            0.0
        } else {
            self.missing as f32 / total as f32
        }
    }
}

/// Counts the datagrams of one flow that arrived, that are missing, and that
/// arrived twice or out of order.
///
/// The tracker remembers the sequence numbers that are missing within
/// `MAX_TRACKED_GAP` of the newest one, and nothing else, so its memory does
/// not grow with the life of the flow. A sequence number that falls out of
/// that window while it is missing is lost for good: it stays in
/// `SequenceStats::missing`.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SequenceTracker {
    highest: Option<u32>,
    /// Sequence numbers below `highest` that have not arrived and are still
    /// awaited.
    missing: HashSet<u32>,
    /// The same numbers, oldest first. A number that has arrived since is
    /// dropped when it reaches the front.
    missing_order: VecDeque<u32>,
    /// Sequence numbers that were given up.
    lost: u64,
    stats: SequenceStats,
}

impl SequenceTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, sequence: u32) -> SequenceObservation {
        self.stats.received = self.stats.received.saturating_add(1);
        let Some(highest) = self.highest else {
            self.highest = Some(sequence);
            self.stats.highest_seen = Some(sequence);
            return SequenceObservation {
                sequence,
                expected: sequence,
                missing_before: 0,
                duplicate_or_reordered: false,
            };
        };

        // The oldest sequence number that is still awaited, or the one after
        // the newest when nothing is missing.
        let expected = self.oldest_missing().unwrap_or(highest.wrapping_add(1));
        let from_expected = sequence.wrapping_sub(expected);
        let at_or_after_expected = from_expected <= i32::MAX as u32;
        let missing_before = if at_or_after_expected {
            from_expected
        } else {
            0
        };

        let ahead = sequence.wrapping_sub(highest);
        let duplicate_or_reordered = if ahead != 0 && ahead <= i32::MAX as u32 {
            // The newest so far. The numbers between the last newest and
            // this one are missing. Only the nearest are awaited.
            let gap = ahead - 1;
            let awaited = gap.min(MAX_TRACKED_GAP);
            self.lost = self.lost.saturating_add(u64::from(gap - awaited));
            let mut missing = sequence.wrapping_sub(awaited);
            for _ in 0..awaited {
                if self.missing.insert(missing) {
                    self.missing_order.push_back(missing);
                }
                missing = missing.wrapping_add(1);
            }
            self.highest = Some(sequence);
            self.stats.highest_seen = Some(sequence);
            false
        } else {
            // An awaited number that arrives late, or a number that has
            // arrived before or was given up.
            self.missing.remove(&sequence);
            true
        };
        self.give_up_old();

        self.stats.missing = self.lost.saturating_add(self.missing.len() as u64);
        if duplicate_or_reordered {
            self.stats.duplicate_or_reordered = self.stats.duplicate_or_reordered.saturating_add(1);
        }

        SequenceObservation {
            sequence,
            expected,
            missing_before,
            duplicate_or_reordered,
        }
    }

    pub fn stats(&self) -> SequenceStats {
        self.stats
    }

    /// Sequence numbers that this tracker holds in memory.
    pub fn tracked(&self) -> usize {
        self.missing.len().max(self.missing_order.len())
    }

    fn oldest_missing(&mut self) -> Option<u32> {
        while let Some(front) = self.missing_order.front().copied() {
            if self.missing.contains(&front) {
                return Some(front);
            }
            self.missing_order.pop_front();
        }
        None
    }

    /// Count as lost the numbers that are missing and too far behind the
    /// newest to be awaited.
    fn give_up_old(&mut self) {
        let Some(highest) = self.highest else {
            return;
        };
        while let Some(front) = self.missing_order.front().copied() {
            if !self.missing.contains(&front) {
                self.missing_order.pop_front();
            } else if highest.wrapping_sub(front) > MAX_TRACKED_GAP {
                self.missing.remove(&front);
                self.missing_order.pop_front();
                self.lost = self.lost.saturating_add(1);
            } else {
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequence_tracker_counts_gaps_and_reordering() {
        let mut tracker = SequenceTracker::new();
        assert_eq!(tracker.observe(10).missing_before, 0);
        assert_eq!(tracker.observe(13).missing_before, 2);
        assert!(tracker.observe(12).duplicate_or_reordered);

        let stats = tracker.stats();
        assert_eq!(stats.received, 3);
        assert_eq!(stats.missing, 1);
        assert_eq!(stats.duplicate_or_reordered, 1);

        assert!(tracker.observe(11).duplicate_or_reordered);
        assert_eq!(tracker.stats().missing, 0);
    }

    #[test]
    fn sequence_tracker_handles_wraparound() {
        let mut tracker = SequenceTracker::new();
        tracker.observe(u32::MAX - 1);
        tracker.observe(u32::MAX);
        let wrapped = tracker.observe(0);

        assert_eq!(wrapped.missing_before, 0);
        assert!(!wrapped.duplicate_or_reordered);
        assert_eq!(tracker.stats().highest_seen, Some(0));
    }

    #[test]
    fn a_lost_datagram_does_not_make_the_tracker_remember_every_later_one() {
        let mut tracker = SequenceTracker::new();
        tracker.observe(0);
        // Sequence number 1 never arrives.
        for sequence in 2..1_000_000_u32 {
            let observed = tracker.observe(sequence);
            assert!(!observed.duplicate_or_reordered);
        }
        assert!(tracker.tracked() <= 1, "{} tracked", tracker.tracked());
        let stats = tracker.stats();
        assert_eq!(stats.received, 999_999);
        assert_eq!(stats.missing, 1);
        assert_eq!(stats.duplicate_or_reordered, 0);
        // The lost one is counted once, however long the flow lives.
        assert!(stats.loss_fraction() < 0.000_002);
    }

    #[test]
    fn datagrams_that_arrive_out_of_order_leave_nothing_behind() {
        let mut tracker = SequenceTracker::new();
        // 10, 14, 12, 11, 13: every number arrives, none in its place after
        // the first. The tracker of 0.1.6 waited for 12 for ever after this.
        for sequence in [10, 14, 12, 11, 13] {
            tracker.observe(sequence);
        }
        assert_eq!(tracker.stats().missing, 0);
        assert_eq!(tracker.stats().duplicate_or_reordered, 3);
        for sequence in 15..200_000_u32 {
            tracker.observe(sequence);
        }
        assert_eq!(tracker.tracked(), 0);
        assert_eq!(tracker.stats().missing, 0);
    }

    #[test]
    fn a_long_outage_counts_what_was_lost_and_tracks_only_the_newest_gap() {
        let mut tracker = SequenceTracker::new();
        tracker.observe(0);
        let observed = tracker.observe(1_000_001);
        assert_eq!(observed.expected, 1);
        assert_eq!(observed.missing_before, 1_000_000);
        assert_eq!(tracker.stats().missing, 1_000_000);
        assert!(tracker.tracked() <= MAX_TRACKED_GAP as usize);
        // One of the newest missing numbers arrives late and is no longer
        // missing. An old one that was given up is only a late datagram.
        assert!(tracker.observe(1_000_000).duplicate_or_reordered);
        assert_eq!(tracker.stats().missing, 999_999);
        assert!(tracker.observe(5).duplicate_or_reordered);
        assert_eq!(tracker.stats().missing, 999_999);
    }
}
