use crate::{
    sequence::SequenceStats, source_symbol_count, DatagramFecConfig, DEFAULT_SOURCE_SYMBOLS,
    DEFAULT_SYMBOL_SIZE,
};

const DEFAULT_MIN_REPAIR_RATIO: f32 = 0.05;
const DEFAULT_MAX_REPAIR_RATIO: f32 = 0.35;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NetworkMetrics {
    pub loss_fraction: f32,
    pub rtt_ms: f32,
    pub jitter_ms: f32,
    pub queue_delay_ms: f32,
    pub available_bitrate_bps: Option<u64>,
}

impl Default for NetworkMetrics {
    fn default() -> Self {
        Self {
            loss_fraction: 0.0,
            rtt_ms: 0.0,
            jitter_ms: 0.0,
            queue_delay_ms: 0.0,
            available_bitrate_bps: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NetworkMetricsObservation {
    pub sequence_stats: SequenceStats,
    pub rtt_ms: f32,
    pub jitter_ms: f32,
    pub queue_delay_ms: f32,
    pub available_bitrate_bps: Option<u64>,
    pub loss_fraction_override: Option<f32>,
}

impl NetworkMetricsObservation {
    pub fn new(sequence_stats: SequenceStats) -> Self {
        Self {
            sequence_stats,
            rtt_ms: 0.0,
            jitter_ms: 0.0,
            queue_delay_ms: 0.0,
            available_bitrate_bps: None,
            loss_fraction_override: None,
        }
    }

    pub fn into_metrics(self) -> NetworkMetrics {
        NetworkMetrics {
            loss_fraction: self
                .loss_fraction_override
                .unwrap_or_else(|| self.sequence_stats.loss_fraction())
                .clamp(0.0, 1.0),
            rtt_ms: self.rtt_ms.max(0.0),
            jitter_ms: self.jitter_ms.max(0.0),
            queue_delay_ms: self.queue_delay_ms.max(0.0),
            available_bitrate_bps: self.available_bitrate_bps,
        }
    }
}

impl From<NetworkMetricsObservation> for NetworkMetrics {
    fn from(observation: NetworkMetricsObservation) -> Self {
        observation.into_metrics()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaPriority {
    Audio,
    VideoKey,
    VideoDelta,
    Data,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AdaptiveFecPolicy {
    pub min_source_symbols: u16,
    pub max_source_symbols: u16,
    pub min_repair_symbols: u32,
    pub max_repair_symbols: u32,
    pub delta_repair_floor_source_symbols: u16,
    pub delta_repair_floor_symbols: u32,
    pub min_repair_ratio: f32,
    pub max_repair_ratio: f32,
    pub keyframe_repair_boost: f32,
    pub audio_repair_boost: f32,
    pub symbol_size: u16,
}

impl Default for AdaptiveFecPolicy {
    fn default() -> Self {
        Self {
            min_source_symbols: DEFAULT_SOURCE_SYMBOLS,
            max_source_symbols: 48,
            min_repair_symbols: 0,
            max_repair_symbols: 16,
            delta_repair_floor_source_symbols: 8,
            delta_repair_floor_symbols: 1,
            min_repair_ratio: DEFAULT_MIN_REPAIR_RATIO,
            max_repair_ratio: DEFAULT_MAX_REPAIR_RATIO,
            keyframe_repair_boost: 0.10,
            audio_repair_boost: 0.08,
            symbol_size: DEFAULT_SYMBOL_SIZE,
        }
    }
}

impl AdaptiveFecPolicy {
    pub fn normalized(self) -> Self {
        let min_source_symbols = self.min_source_symbols.max(1);
        let max_source_symbols = self.max_source_symbols.max(min_source_symbols);
        let min_repair_ratio = self.min_repair_ratio.max(0.0);
        let max_repair_ratio = self.max_repair_ratio.max(min_repair_ratio);
        let min_repair_symbols = self.min_repair_symbols.min(self.max_repair_symbols);
        let delta_repair_floor_symbols =
            self.delta_repair_floor_symbols.min(self.max_repair_symbols);

        Self {
            min_source_symbols,
            max_source_symbols,
            min_repair_symbols,
            max_repair_symbols: self.max_repair_symbols,
            delta_repair_floor_source_symbols: self.delta_repair_floor_source_symbols.max(1),
            delta_repair_floor_symbols,
            min_repair_ratio,
            max_repair_ratio,
            keyframe_repair_boost: self.keyframe_repair_boost.max(0.0),
            audio_repair_boost: self.audio_repair_boost.max(0.0),
            symbol_size: self.symbol_size.max(1),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FecDecision {
    pub config: DatagramFecConfig,
    pub repair_ratio: f32,
    pub source_symbols_for_payload: u16,
    pub expected_datagrams: u32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CongestionConfig {
    pub min_bitrate_bps: u64,
    pub max_bitrate_bps: u64,
    pub initial_bitrate_bps: u64,
    pub additive_increase_bps: u64,
    pub multiplicative_decrease: f32,
    pub high_loss_threshold: f32,
    pub high_queue_delay_ms: f32,
    pub high_rtt_ms: f32,
}

impl Default for CongestionConfig {
    fn default() -> Self {
        Self {
            min_bitrate_bps: 256_000,
            max_bitrate_bps: 25_000_000,
            initial_bitrate_bps: 4_000_000,
            additive_increase_bps: 250_000,
            multiplicative_decrease: 0.80,
            high_loss_threshold: 0.08,
            high_queue_delay_ms: 80.0,
            high_rtt_ms: 250.0,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CongestionDecision {
    pub target_bitrate_bps: u64,
    pub congestion_limited: bool,
    pub should_drop_delta_frame: bool,
    pub request_keyframe: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AdaptiveFecController {
    policy: AdaptiveFecPolicy,
    congestion: CongestionConfig,
    metrics: NetworkMetrics,
    target_bitrate_bps: u64,
}

impl Default for AdaptiveFecController {
    fn default() -> Self {
        Self::new(AdaptiveFecPolicy::default(), CongestionConfig::default())
    }
}

impl AdaptiveFecController {
    pub fn new(policy: AdaptiveFecPolicy, congestion: CongestionConfig) -> Self {
        let congestion = congestion_config_normalized(congestion);
        Self {
            policy: policy.normalized(),
            target_bitrate_bps: congestion
                .initial_bitrate_bps
                .clamp(congestion.min_bitrate_bps, congestion.max_bitrate_bps),
            congestion,
            metrics: NetworkMetrics::default(),
        }
    }

    pub fn policy(&self) -> AdaptiveFecPolicy {
        self.policy
    }

    pub fn metrics(&self) -> NetworkMetrics {
        self.metrics
    }

    pub fn target_bitrate_bps(&self) -> u64 {
        self.target_bitrate_bps
    }

    pub fn update_network_metrics(&mut self, metrics: NetworkMetrics) -> CongestionDecision {
        let loss = metrics.loss_fraction.clamp(0.0, 1.0);
        let congested = loss >= self.congestion.high_loss_threshold
            || metrics.queue_delay_ms >= self.congestion.high_queue_delay_ms
            || metrics.rtt_ms >= self.congestion.high_rtt_ms;

        if congested {
            let decreased =
                (self.target_bitrate_bps as f32 * self.congestion.multiplicative_decrease) as u64;
            self.target_bitrate_bps = decreased.clamp(
                self.congestion.min_bitrate_bps,
                self.congestion.max_bitrate_bps,
            );
        } else {
            self.target_bitrate_bps = self
                .target_bitrate_bps
                .saturating_add(self.congestion.additive_increase_bps)
                .clamp(
                    self.congestion.min_bitrate_bps,
                    self.congestion.max_bitrate_bps,
                );
        }

        if let Some(available) = metrics.available_bitrate_bps {
            self.target_bitrate_bps = self.target_bitrate_bps.min(available).clamp(
                self.congestion.min_bitrate_bps,
                self.congestion.max_bitrate_bps,
            );
        }

        self.metrics = NetworkMetrics {
            loss_fraction: loss,
            ..metrics
        };

        CongestionDecision {
            target_bitrate_bps: self.target_bitrate_bps,
            congestion_limited: congested,
            should_drop_delta_frame: congested && loss >= self.congestion.high_loss_threshold,
            request_keyframe: loss >= 0.20,
        }
    }

    pub fn update_from_observation(
        &mut self,
        observation: NetworkMetricsObservation,
    ) -> CongestionDecision {
        self.update_network_metrics(observation.into_metrics())
    }

    pub fn decide(&self, payload_len: usize, priority: MediaPriority) -> FecDecision {
        let symbol_size = self.policy.symbol_size.max(1);
        let source_symbols_for_payload = source_symbol_count(payload_len, symbol_size);
        let source_symbols = source_symbols_for_payload
            .max(self.policy.min_source_symbols)
            .min(self.policy.max_source_symbols);
        let repair_ratio = self.repair_ratio(priority);
        let repair_symbols = self.repair_symbols_for(source_symbols_for_payload, priority);
        let expected_datagrams = u32::from(source_symbols_for_payload) + repair_symbols;

        FecDecision {
            config: DatagramFecConfig {
                source_symbols,
                repair_symbols,
                symbol_size,
            },
            repair_ratio,
            source_symbols_for_payload,
            expected_datagrams,
        }
    }

    pub fn repair_symbols_for(&self, source_symbols: u16, priority: MediaPriority) -> u32 {
        let source_symbols = source_symbols.max(1);
        let repair_ratio = self.repair_ratio(priority);
        let raw_repair = f32::from(source_symbols) * repair_ratio;
        let repair = if raw_repair < 1.0 {
            match priority {
                MediaPriority::Audio | MediaPriority::VideoKey
                    if repair_ratio > self.policy.min_repair_ratio =>
                {
                    1
                }
                _ => 0,
            }
        } else {
            raw_repair.ceil() as u32
        };
        let media_floor = match priority {
            MediaPriority::VideoDelta
                if source_symbols >= self.policy.delta_repair_floor_source_symbols =>
            {
                self.policy.delta_repair_floor_symbols
            }
            _ => 0,
        };
        repair
            .max(self.policy.min_repair_symbols)
            .max(media_floor)
            .min(self.policy.max_repair_symbols)
    }

    pub fn repair_ratio(&self, priority: MediaPriority) -> f32 {
        let loss = self.metrics.loss_fraction.clamp(0.0, 1.0);
        let jitter_pressure = if self.metrics.jitter_ms <= 0.0 {
            0.0
        } else {
            (self.metrics.jitter_ms / 80.0).clamp(0.0, 1.0) * 0.04
        };
        let queue_pressure = if self.metrics.queue_delay_ms <= 0.0 {
            0.0
        } else {
            (self.metrics.queue_delay_ms / 200.0).clamp(0.0, 1.0) * 0.04
        };
        let priority_boost = match priority {
            MediaPriority::Audio => self.policy.audio_repair_boost,
            MediaPriority::VideoKey => self.policy.keyframe_repair_boost,
            MediaPriority::VideoDelta | MediaPriority::Data => 0.0,
        };

        (self.policy.min_repair_ratio
            + (loss * 1.5)
            + jitter_pressure
            + queue_pressure
            + priority_boost)
            .clamp(self.policy.min_repair_ratio, self.policy.max_repair_ratio)
    }
}

fn congestion_config_normalized(config: CongestionConfig) -> CongestionConfig {
    let min_bitrate_bps = config.min_bitrate_bps.max(1);
    let max_bitrate_bps = config.max_bitrate_bps.max(min_bitrate_bps);
    CongestionConfig {
        min_bitrate_bps,
        max_bitrate_bps,
        initial_bitrate_bps: config
            .initial_bitrate_bps
            .clamp(min_bitrate_bps, max_bitrate_bps),
        additive_increase_bps: config.additive_increase_bps.max(1),
        multiplicative_decrease: config.multiplicative_decrease.clamp(0.1, 0.99),
        high_loss_threshold: config.high_loss_threshold.clamp(0.0, 1.0),
        high_queue_delay_ms: config.high_queue_delay_ms.max(1.0),
        high_rtt_ms: config.high_rtt_ms.max(1.0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn low_loss_large_frame_avoids_one_for_one_repair() {
        let controller = AdaptiveFecController::default();
        let decision = controller.decide(
            32 * usize::from(DEFAULT_SYMBOL_SIZE),
            MediaPriority::VideoDelta,
        );

        assert!(decision.config.source_symbols >= DEFAULT_SOURCE_SYMBOLS);
        assert!(decision.config.repair_symbols < u32::from(decision.source_symbols_for_payload));
        assert!(decision.config.repair_symbols >= 1);
        assert!(decision.config.repair_symbols <= 4);
    }

    #[test]
    fn low_loss_large_delta_gets_repair_floor() {
        let controller = AdaptiveFecController::default();
        let decision = controller.decide(18_000, MediaPriority::VideoDelta);

        assert!(decision.source_symbols_for_payload >= 8);
        assert_eq!(decision.config.repair_symbols, 1);
    }

    #[test]
    fn low_loss_single_symbol_delta_avoids_full_repair_overhead() {
        let controller = AdaptiveFecController::default();
        let decision = controller.decide(512, MediaPriority::VideoDelta);

        assert_eq!(decision.source_symbols_for_payload, 1);
        assert_eq!(decision.config.repair_symbols, 0);
        assert_eq!(decision.expected_datagrams, 1);
    }

    #[test]
    fn keyframes_receive_more_repair_than_delta_frames() {
        let mut controller = AdaptiveFecController::default();
        controller.update_network_metrics(NetworkMetrics {
            loss_fraction: 0.04,
            ..NetworkMetrics::default()
        });

        let keyframe = controller.decide(16_000, MediaPriority::VideoKey);
        let delta = controller.decide(16_000, MediaPriority::VideoDelta);

        assert!(keyframe.config.repair_symbols >= delta.config.repair_symbols);
        assert!(keyframe.repair_ratio > delta.repair_ratio);
    }

    #[test]
    fn congestion_controller_reduces_and_recovers_target_bitrate() {
        let mut controller = AdaptiveFecController::default();
        let initial = controller.target_bitrate_bps();

        let congested = controller.update_network_metrics(NetworkMetrics {
            loss_fraction: 0.2,
            rtt_ms: 300.0,
            ..NetworkMetrics::default()
        });
        assert!(congested.congestion_limited);
        assert!(controller.target_bitrate_bps() < initial);

        let reduced = controller.target_bitrate_bps();
        let healthy = controller.update_network_metrics(NetworkMetrics::default());
        assert!(!healthy.congestion_limited);
        assert!(controller.target_bitrate_bps() > reduced);
    }

    #[test]
    fn observation_updates_metrics_from_sequence_stats_and_queue_state() {
        let mut controller = AdaptiveFecController::default();
        let observation = NetworkMetricsObservation {
            sequence_stats: SequenceStats {
                received: 90,
                missing: 10,
                duplicate_or_reordered: 0,
                highest_seen: Some(99),
            },
            rtt_ms: 42.0,
            jitter_ms: 7.0,
            queue_delay_ms: 11.0,
            available_bitrate_bps: Some(2_000_000),
            loss_fraction_override: None,
        };

        controller.update_from_observation(observation);
        let metrics = controller.metrics();

        assert_eq!(metrics.loss_fraction, 0.10);
        assert_eq!(metrics.rtt_ms, 42.0);
        assert_eq!(metrics.jitter_ms, 7.0);
        assert_eq!(metrics.queue_delay_ms, 11.0);
        assert_eq!(metrics.available_bitrate_bps, Some(2_000_000));
    }
}
