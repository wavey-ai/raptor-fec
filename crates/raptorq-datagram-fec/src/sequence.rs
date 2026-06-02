use std::collections::HashSet;

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

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SequenceTracker {
    next_expected: Option<u32>,
    missing: HashSet<u32>,
    future_received: HashSet<u32>,
    stats: SequenceStats,
}

impl SequenceTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn observe(&mut self, sequence: u32) -> SequenceObservation {
        let expected = self.next_expected.unwrap_or(sequence);
        let diff = sequence.wrapping_sub(expected);
        let forward = self.next_expected.is_none() || diff <= i32::MAX as u32;
        let mut duplicate_or_reordered = !forward;
        let missing_before = if forward { diff } else { 0 };

        self.stats.received = self.stats.received.saturating_add(1);
        self.stats.highest_seen = Some(self.stats.highest_seen.map_or(sequence, |highest| {
            let diff = sequence.wrapping_sub(highest);
            if diff <= i32::MAX as u32 {
                sequence
            } else {
                highest
            }
        }));

        if self.next_expected.is_none() {
            self.next_expected = Some(sequence.wrapping_add(1));
        } else if self.missing.remove(&sequence) {
            duplicate_or_reordered = true;
            if sequence == expected {
                self.advance_expected(sequence);
            }
        } else if forward {
            if diff == 0 {
                self.advance_expected(sequence);
            } else {
                let mut missing = expected;
                for _ in 0..diff.min(MAX_TRACKED_GAP) {
                    self.missing.insert(missing);
                    missing = missing.wrapping_add(1);
                }
                if !self.future_received.insert(sequence) {
                    duplicate_or_reordered = true;
                }
            }
        }

        self.stats.missing = self.missing.len() as u64;
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

    fn advance_expected(&mut self, sequence: u32) {
        self.missing.remove(&sequence);
        let mut next = sequence.wrapping_add(1);
        while self.future_received.remove(&next) {
            self.missing.remove(&next);
            next = next.wrapping_add(1);
        }
        self.next_expected = Some(next);
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
}
