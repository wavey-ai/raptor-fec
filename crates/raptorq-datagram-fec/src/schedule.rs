use crate::{
    EncodedMediaFrame, MediaDatagramOrder, MediaDatagramRole, MediaFrameFlags, MediaPriority,
};

/// Default upper bound for work admitted into one scheduling pass.
pub const DEFAULT_MAX_DATAGRAMS_PER_PLAN: usize = 4_096;
/// Absolute allocation/work bound for one scheduling pass.
pub const HARD_MAX_DATAGRAMS_PER_PLAN: usize = 65_536;
/// Absolute number of frame schedules inspected in one scheduling pass.
///
/// The planner performs one additional iterator read to detect truncation.
pub const HARD_MAX_FRAMES_SCANNED_PER_PLAN: usize = 4_096;
/// Absolute number of frame-role block visits in one scheduling pass.
pub const HARD_MAX_BLOCK_VISITS_PER_PLAN: usize = 262_144;
/// Repairs for dependency-bearing media inside this deadline window may pass
/// far-deadline delta/data source traffic. Important source traffic stays first.
pub const URGENT_REPAIR_WINDOW_US: u64 = 5_000;
/// Absolute bound on additional RaptorQ symbols selected by one recovery decision.
pub const HARD_MAX_EXTRA_REPAIR_SYMBOLS: u32 = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaSendPolicy {
    /// Retained for API compatibility. Scheduling is always source-first.
    pub order: MediaDatagramOrder,
    pub max_in_flight_datagrams: usize,
    pub max_datagrams_per_plan: usize,
    pub min_datagram_spacing_us: u64,
    pub playout_latency_ms: u32,
    pub stale_delta_repair_queue_delay_ms: u32,
}

impl Default for MediaSendPolicy {
    fn default() -> Self {
        Self {
            order: MediaDatagramOrder::SourceFirst,
            max_in_flight_datagrams: DEFAULT_MAX_DATAGRAMS_PER_PLAN,
            max_datagrams_per_plan: DEFAULT_MAX_DATAGRAMS_PER_PLAN,
            min_datagram_spacing_us: 0,
            playout_latency_ms: 33,
            stale_delta_repair_queue_delay_ms: 20,
        }
    }
}

impl MediaSendPolicy {
    fn normalized(self) -> Self {
        let max_datagrams_per_plan = self
            .max_datagrams_per_plan
            .clamp(1, HARD_MAX_DATAGRAMS_PER_PLAN);
        Self {
            order: MediaDatagramOrder::SourceFirst,
            max_in_flight_datagrams: self.max_in_flight_datagrams.min(max_datagrams_per_plan),
            max_datagrams_per_plan,
            ..self
        }
    }
}

/// Millisecond compatibility state for the original scheduling API.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MediaQueueState {
    pub now_ms: u64,
    pub queue_delay_ms: u32,
    pub in_flight_datagrams: usize,
}

/// Precise scheduler state in the same monotonic clock domain as `MediaDeadline`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MediaScheduleState {
    pub now_us: u64,
    pub queue_delay_us: u64,
    pub in_flight_datagrams: usize,
}

impl From<MediaQueueState> for MediaScheduleState {
    fn from(state: MediaQueueState) -> Self {
        Self {
            now_us: state.now_ms.saturating_mul(1_000),
            queue_delay_us: u64::from(state.queue_delay_ms).saturating_mul(1_000),
            in_flight_datagrams: state.in_flight_datagrams,
        }
    }
}

/// Absolute expiry in a caller-selected monotonic microsecond clock domain.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct MediaDeadline {
    pub expires_at_us: u64,
}

impl MediaDeadline {
    pub const fn from_micros(expires_at_us: u64) -> Self {
        Self { expires_at_us }
    }

    pub fn from_millis(expires_at_ms: u64) -> Self {
        Self::from_micros(expires_at_ms.saturating_mul(1_000))
    }

    pub fn remaining_us_at(self, now_us: u64) -> Option<u64> {
        self.expires_at_us
            .checked_sub(now_us)
            .filter(|value| *value > 0)
    }

    pub fn is_expired_at(self, now_us: u64) -> bool {
        self.expires_at_us <= now_us
    }

    /// Observe actual completion for latency histograms and deadline SLOs.
    pub fn observe_completion(
        self,
        started_at_us: u64,
        completed_at_us: u64,
    ) -> MediaDeadlineOutcome {
        let deadline_hit = completed_at_us < self.expires_at_us;
        MediaDeadlineOutcome {
            deadline: self,
            started_at_us,
            completed_at_us,
            elapsed_us: completed_at_us.saturating_sub(started_at_us),
            deadline_hit,
            headroom_us: if deadline_hit {
                self.expires_at_us.saturating_sub(completed_at_us)
            } else {
                0
            },
            lateness_us: if deadline_hit {
                0
            } else {
                completed_at_us.saturating_sub(self.expires_at_us)
            },
        }
    }
}

/// Actual timing observation ready for counters and latency/headroom histograms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaDeadlineOutcome {
    pub deadline: MediaDeadline,
    pub started_at_us: u64,
    pub completed_at_us: u64,
    pub elapsed_us: u64,
    pub deadline_hit: bool,
    pub headroom_us: u64,
    pub lateness_us: u64,
}

/// Dependency/latency importance independent of the packet carrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MediaObjectKind {
    Initialization,
    CodecConfig,
    VideoKeyframe,
    Audio,
    VideoDelta,
    Data,
}

/// Intended route class. The transport maps this hint onto its own sessions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaPathIntent {
    PrimarySource,
    SecondaryRepair,
    ReliableObjectFetch,
}

#[derive(Debug, Clone, Copy)]
pub struct MediaFrameSchedule<'a> {
    pub frame: &'a EncodedMediaFrame,
    pub deadline: MediaDeadline,
}

impl<'a> MediaFrameSchedule<'a> {
    pub const fn new(frame: &'a EncodedMediaFrame, deadline: MediaDeadline) -> Self {
        Self { frame, deadline }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MediaDatagramClass {
    InitializationSource,
    CodecConfigSource,
    VideoKeySource,
    AudioSource,
    VideoDeltaSource,
    DataSource,
    InitializationRepair,
    CodecConfigRepair,
    VideoKeyRepair,
    AudioRepair,
    VideoDeltaRepair,
    DataRepair,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaDropReason {
    Expired,
    StaleDeltaRepair,
    InFlightLimit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaScheduledDatagram {
    pub frame_sequence: u64,
    pub stream_id: u64,
    pub datagram_index: usize,
    pub block_id: u32,
    pub fragment_index: u16,
    pub role: MediaDatagramRole,
    pub object_kind: MediaObjectKind,
    pub class: MediaDatagramClass,
    pub path_intent: MediaPathIntent,
    /// Compatibility projection. Prefer `deadline` for scheduling decisions.
    pub deadline_ms: u64,
    pub deadline: MediaDeadline,
    pub send_after_us: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaDroppedDatagram {
    pub frame_sequence: u64,
    pub stream_id: u64,
    pub datagram_index: usize,
    pub block_id: u32,
    pub fragment_index: u16,
    pub role: MediaDatagramRole,
    pub object_kind: MediaObjectKind,
    pub class: MediaDatagramClass,
    pub path_intent: MediaPathIntent,
    pub deadline: MediaDeadline,
    pub reason: MediaDropReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MediaSendPlan {
    pub scheduled: Vec<MediaScheduledDatagram>,
    pub dropped: Vec<MediaDroppedDatagram>,
    pub blocked_by_in_flight: usize,
    pub plan_limit_reached: bool,
}

impl MediaSendPlan {
    pub fn scheduled_datagram_indices(&self) -> Vec<usize> {
        self.scheduled
            .iter()
            .map(|entry| entry.datagram_index)
            .collect()
    }

    pub fn dropped_stale_delta_repair(&self) -> usize {
        self.dropped
            .iter()
            .filter(|entry| entry.reason == MediaDropReason::StaleDeltaRepair)
            .count()
    }

    pub fn dropped_expired(&self) -> usize {
        self.dropped
            .iter()
            .filter(|entry| entry.reason == MediaDropReason::Expired)
            .count()
    }
}

impl EncodedMediaFrame {
    /// Compatibility planner that derives a deadline from PTS and policy latency.
    /// Use `plan_media_datagrams_with_deadlines` when the absolute expiry is known.
    pub fn scheduled_datagram_send_plan(
        &self,
        policy: MediaSendPolicy,
        queue: MediaQueueState,
    ) -> MediaSendPlan {
        plan_media_datagrams([self], policy, queue)
    }
}

/// Compatibility planner for callers whose PTS uses the scheduler clock domain.
pub fn plan_media_datagrams<'a, I>(
    frames: I,
    policy: MediaSendPolicy,
    queue: MediaQueueState,
) -> MediaSendPlan
where
    I: IntoIterator<Item = &'a EncodedMediaFrame>,
{
    let scheduled_frames = frames.into_iter().map(|frame| {
        MediaFrameSchedule::new(
            frame,
            MediaDeadline::from_millis(frame_deadline_ms(frame, policy)),
        )
    });
    plan_media_datagrams_with_deadlines(scheduled_frames, policy, queue.into())
}

/// Build a bounded, carrier-neutral send plan.
///
/// Normal traffic is source-first. Initialization, configuration, keyframe, and
/// audio repair within [`URGENT_REPAIR_WINDOW_US`] may pass far-deadline
/// delta/data source traffic so useful recovery does not expire in the queue.
pub fn plan_media_datagrams_with_deadlines<'a, I>(
    frames: I,
    policy: MediaSendPolicy,
    state: MediaScheduleState,
) -> MediaSendPlan
where
    I: IntoIterator<Item = MediaFrameSchedule<'a>>,
{
    let policy = policy.normalized();
    let ready_at_us = state.now_us.saturating_add(state.queue_delay_us);
    let mut groups = Vec::with_capacity(HARD_MAX_FRAMES_SCANNED_PER_PLAN.min(256) * 2);
    let mut frames = frames.into_iter();
    let mut frames_exhausted = false;
    for _ in 0..HARD_MAX_FRAMES_SCANNED_PER_PLAN {
        let Some(scheduled_frame) = frames.next() else {
            frames_exhausted = true;
            break;
        };
        let object_kind = media_object_kind(scheduled_frame.frame);
        for role in [MediaDatagramRole::Source, MediaDatagramRole::Repair] {
            groups.push(MediaCandidateGroup {
                frame: scheduled_frame.frame,
                role,
                object_kind,
                deadline: scheduled_frame.deadline,
            });
        }
    }
    let frame_scan_limit_reached = !frames_exhausted && frames.next().is_some();

    // Sort compact frame-role groups before expanding datagrams. This lets a
    // later important frame compete with an early large delta frame without
    // allocating every candidate or consuming an unbounded iterator.
    groups.sort_by_key(|entry| {
        (
            scheduling_rank(entry.role, entry.object_kind, entry.deadline, ready_at_us),
            entry.object_kind,
            entry.deadline,
            entry.frame.metadata.stream_id,
            entry.frame.sequence,
            role_rank(entry.role),
        )
    });

    let capacity = policy
        .max_in_flight_datagrams
        .saturating_sub(state.in_flight_datagrams);
    let mut plan = MediaSendPlan {
        scheduled: Vec::with_capacity(policy.max_datagrams_per_plan.min(capacity)),
        dropped: Vec::new(),
        blocked_by_in_flight: 0,
        plan_limit_reached: frame_scan_limit_reached,
    };

    let mut admitted = 0usize;
    let mut block_visits = 0usize;
    'groups: for group in groups {
        for block in &group.frame.blocks {
            if block_visits == HARD_MAX_BLOCK_VISITS_PER_PLAN {
                plan.plan_limit_reached = true;
                break 'groups;
            }
            block_visits += 1;
            let indices = match group.role {
                MediaDatagramRole::Source => block.source_datagram_indices(),
                MediaDatagramRole::Repair => block.repair_datagram_indices(),
            };
            for datagram_index in indices {
                if admitted == policy.max_datagrams_per_plan {
                    plan.plan_limit_reached = true;
                    break 'groups;
                }
                admitted += 1;
                let candidate = group.candidate(block, datagram_index);
                if candidate.deadline.is_expired_at(ready_at_us) {
                    plan.dropped
                        .push(candidate.dropped(MediaDropReason::Expired));
                    continue;
                }
                if should_drop_stale_delta_repair(candidate, policy, state) {
                    plan.dropped
                        .push(candidate.dropped(MediaDropReason::StaleDeltaRepair));
                    continue;
                }
                if plan.scheduled.len() == capacity {
                    // Carrier saturation defers a live symbol; it does not make
                    // that symbol obsolete and therefore is not a drop record.
                    plan.blocked_by_in_flight = plan.blocked_by_in_flight.saturating_add(1);
                    continue;
                }

                let send_after_us =
                    (plan.scheduled.len() as u64).saturating_mul(policy.min_datagram_spacing_us);
                let send_at_us = ready_at_us.saturating_add(send_after_us);
                if candidate.deadline.is_expired_at(send_at_us) {
                    plan.dropped
                        .push(candidate.dropped(MediaDropReason::Expired));
                    continue;
                }
                plan.scheduled.push(candidate.scheduled(send_after_us));
            }
        }
    }

    plan
}

#[derive(Debug, Clone, Copy)]
struct MediaCandidateGroup<'a> {
    frame: &'a EncodedMediaFrame,
    role: MediaDatagramRole,
    object_kind: MediaObjectKind,
    deadline: MediaDeadline,
}

impl MediaCandidateGroup<'_> {
    fn candidate(self, block: &crate::EncodedMediaBlock, datagram_index: usize) -> MediaCandidate {
        MediaCandidate {
            frame_sequence: self.frame.sequence,
            stream_id: self.frame.metadata.stream_id,
            datagram_index,
            block_id: block.block_id,
            fragment_index: block.fragment_index,
            role: self.role,
            object_kind: self.object_kind,
            class: datagram_class(self.object_kind, self.role),
            path_intent: path_intent(self.role),
            deadline: self.deadline,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct MediaCandidate {
    frame_sequence: u64,
    stream_id: u64,
    datagram_index: usize,
    block_id: u32,
    fragment_index: u16,
    role: MediaDatagramRole,
    object_kind: MediaObjectKind,
    class: MediaDatagramClass,
    path_intent: MediaPathIntent,
    deadline: MediaDeadline,
}

impl MediaCandidate {
    fn scheduled(self, send_after_us: u64) -> MediaScheduledDatagram {
        MediaScheduledDatagram {
            frame_sequence: self.frame_sequence,
            stream_id: self.stream_id,
            datagram_index: self.datagram_index,
            block_id: self.block_id,
            fragment_index: self.fragment_index,
            role: self.role,
            object_kind: self.object_kind,
            class: self.class,
            path_intent: self.path_intent,
            deadline_ms: self.deadline.expires_at_us / 1_000,
            deadline: self.deadline,
            send_after_us,
        }
    }

    fn dropped(self, reason: MediaDropReason) -> MediaDroppedDatagram {
        MediaDroppedDatagram {
            frame_sequence: self.frame_sequence,
            stream_id: self.stream_id,
            datagram_index: self.datagram_index,
            block_id: self.block_id,
            fragment_index: self.fragment_index,
            role: self.role,
            object_kind: self.object_kind,
            class: self.class,
            path_intent: self.path_intent,
            deadline: self.deadline,
            reason,
        }
    }
}

fn should_drop_stale_delta_repair(
    candidate: MediaCandidate,
    policy: MediaSendPolicy,
    state: MediaScheduleState,
) -> bool {
    candidate.object_kind == MediaObjectKind::VideoDelta
        && candidate.role == MediaDatagramRole::Repair
        && state.queue_delay_us
            >= u64::from(policy.stale_delta_repair_queue_delay_ms).saturating_mul(1_000)
}

fn media_object_kind(frame: &EncodedMediaFrame) -> MediaObjectKind {
    if frame
        .metadata
        .flags
        .contains(MediaFrameFlags::INITIALIZATION)
    {
        return MediaObjectKind::Initialization;
    }
    if frame.metadata.flags.contains(MediaFrameFlags::CODEC_CONFIG) {
        return MediaObjectKind::CodecConfig;
    }
    match frame.priority {
        MediaPriority::VideoKey => MediaObjectKind::VideoKeyframe,
        MediaPriority::Audio => MediaObjectKind::Audio,
        MediaPriority::VideoDelta => MediaObjectKind::VideoDelta,
        MediaPriority::Data => MediaObjectKind::Data,
    }
}

fn datagram_class(object_kind: MediaObjectKind, role: MediaDatagramRole) -> MediaDatagramClass {
    match (object_kind, role) {
        (MediaObjectKind::Initialization, MediaDatagramRole::Source) => {
            MediaDatagramClass::InitializationSource
        }
        (MediaObjectKind::CodecConfig, MediaDatagramRole::Source) => {
            MediaDatagramClass::CodecConfigSource
        }
        (MediaObjectKind::VideoKeyframe, MediaDatagramRole::Source) => {
            MediaDatagramClass::VideoKeySource
        }
        (MediaObjectKind::Audio, MediaDatagramRole::Source) => MediaDatagramClass::AudioSource,
        (MediaObjectKind::VideoDelta, MediaDatagramRole::Source) => {
            MediaDatagramClass::VideoDeltaSource
        }
        (MediaObjectKind::Data, MediaDatagramRole::Source) => MediaDatagramClass::DataSource,
        (MediaObjectKind::Initialization, MediaDatagramRole::Repair) => {
            MediaDatagramClass::InitializationRepair
        }
        (MediaObjectKind::CodecConfig, MediaDatagramRole::Repair) => {
            MediaDatagramClass::CodecConfigRepair
        }
        (MediaObjectKind::VideoKeyframe, MediaDatagramRole::Repair) => {
            MediaDatagramClass::VideoKeyRepair
        }
        (MediaObjectKind::Audio, MediaDatagramRole::Repair) => MediaDatagramClass::AudioRepair,
        (MediaObjectKind::VideoDelta, MediaDatagramRole::Repair) => {
            MediaDatagramClass::VideoDeltaRepair
        }
        (MediaObjectKind::Data, MediaDatagramRole::Repair) => MediaDatagramClass::DataRepair,
    }
}

fn role_rank(role: MediaDatagramRole) -> u8 {
    match role {
        MediaDatagramRole::Source => 0,
        MediaDatagramRole::Repair => 1,
    }
}

fn scheduling_rank(
    role: MediaDatagramRole,
    object_kind: MediaObjectKind,
    deadline: MediaDeadline,
    ready_at_us: u64,
) -> u8 {
    let important = matches!(
        object_kind,
        MediaObjectKind::Initialization
            | MediaObjectKind::CodecConfig
            | MediaObjectKind::VideoKeyframe
            | MediaObjectKind::Audio
    );
    match role {
        MediaDatagramRole::Source if important => 0,
        MediaDatagramRole::Repair
            if important
                && deadline
                    .remaining_us_at(ready_at_us)
                    .is_some_and(|remaining| remaining <= URGENT_REPAIR_WINDOW_US) =>
        {
            1
        }
        MediaDatagramRole::Source => 2,
        MediaDatagramRole::Repair => 3,
    }
}

fn path_intent(role: MediaDatagramRole) -> MediaPathIntent {
    match role {
        MediaDatagramRole::Source => MediaPathIntent::PrimarySource,
        MediaDatagramRole::Repair => MediaPathIntent::SecondaryRepair,
    }
}

fn frame_deadline_ms(frame: &EncodedMediaFrame, policy: MediaSendPolicy) -> u64 {
    frame
        .metadata
        .pts_ms
        .saturating_add(u64::from(frame.metadata.duration_ms))
        .saturating_add(u64::from(policy.playout_latency_ms))
}

/// Bounded policy for choosing the next recovery mechanism for an incomplete object.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaRecoveryPolicy {
    pub max_extra_repair_symbols: u32,
    pub repair_safety_symbols: u32,
    pub deadline_safety_margin_us: u64,
}

impl Default for MediaRecoveryPolicy {
    fn default() -> Self {
        Self {
            max_extra_repair_symbols: 64,
            repair_safety_symbols: 1,
            deadline_safety_margin_us: 1_000,
        }
    }
}

impl MediaRecoveryPolicy {
    pub fn normalized(self) -> Self {
        Self {
            max_extra_repair_symbols: self
                .max_extra_repair_symbols
                .min(HARD_MAX_EXTRA_REPAIR_SYMBOLS),
            repair_safety_symbols: self
                .repair_safety_symbols
                .min(HARD_MAX_EXTRA_REPAIR_SYMBOLS),
            ..self
        }
    }

    pub fn decide(self, input: MediaRecoveryInput) -> MediaRecoveryDecision {
        decide_media_recovery(self, input)
    }
}

/// Inputs are estimates in microseconds from the same decision instant.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaRecoveryInput {
    pub now_us: u64,
    pub deadline: MediaDeadline,
    /// Source-symbol deficit after accounting for repair already available/in flight.
    pub uncovered_source_symbols: u32,
    /// Request/response RTT to the selected independent repair parent.
    pub secondary_rtt_us: u64,
    pub secondary_queue_delay_us: u64,
    pub repair_symbol_spacing_us: u64,
    /// Full request-to-object-complete estimate for the reliable path.
    pub reliable_fetch_estimate_us: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaRecoveryAction {
    NoRecoveryNeeded,
    SendRaptorQRepair {
        repair_symbols: u32,
        path_intent: MediaPathIntent,
    },
    ReliableFetch {
        path_intent: MediaPathIntent,
    },
    Expire,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaRecoveryDecision {
    pub action: MediaRecoveryAction,
    pub remaining_us: u64,
    pub estimated_arrival_at_us: Option<u64>,
    /// Estimated time left before expiry after recovery completes.
    pub estimated_headroom_us: Option<u64>,
}

/// Prefer timely RaptorQ repair, fall back to timely reliable fetch, then expire.
pub fn decide_media_recovery(
    policy: MediaRecoveryPolicy,
    input: MediaRecoveryInput,
) -> MediaRecoveryDecision {
    if input.uncovered_source_symbols == 0 {
        return MediaRecoveryDecision {
            action: MediaRecoveryAction::NoRecoveryNeeded,
            remaining_us: input.deadline.expires_at_us.saturating_sub(input.now_us),
            estimated_arrival_at_us: Some(input.now_us),
            estimated_headroom_us: input.deadline.remaining_us_at(input.now_us),
        };
    }

    let Some(remaining_us) = input.deadline.remaining_us_at(input.now_us) else {
        return expired_recovery_decision();
    };
    let policy = policy.normalized();
    let repair_symbols = input
        .uncovered_source_symbols
        .saturating_add(policy.repair_safety_symbols);

    if repair_symbols <= policy.max_extra_repair_symbols {
        let repair_burst_us = u64::from(repair_symbols.saturating_sub(1))
            .saturating_mul(input.repair_symbol_spacing_us);
        let fec_estimate_us = input
            .secondary_rtt_us
            .saturating_add(input.secondary_queue_delay_us)
            .saturating_add(repair_burst_us);
        if estimate_fits(
            fec_estimate_us,
            remaining_us,
            policy.deadline_safety_margin_us,
        ) {
            let arrival_at_us = input.now_us.saturating_add(fec_estimate_us);
            return MediaRecoveryDecision {
                action: MediaRecoveryAction::SendRaptorQRepair {
                    repair_symbols,
                    path_intent: MediaPathIntent::SecondaryRepair,
                },
                remaining_us,
                estimated_arrival_at_us: Some(arrival_at_us),
                estimated_headroom_us: input
                    .deadline
                    .expires_at_us
                    .checked_sub(arrival_at_us)
                    .filter(|headroom| *headroom > 0),
            };
        }
    }

    if let Some(fetch_estimate_us) = input.reliable_fetch_estimate_us {
        if estimate_fits(
            fetch_estimate_us,
            remaining_us,
            policy.deadline_safety_margin_us,
        ) {
            let arrival_at_us = input.now_us.saturating_add(fetch_estimate_us);
            return MediaRecoveryDecision {
                action: MediaRecoveryAction::ReliableFetch {
                    path_intent: MediaPathIntent::ReliableObjectFetch,
                },
                remaining_us,
                estimated_arrival_at_us: Some(arrival_at_us),
                estimated_headroom_us: input
                    .deadline
                    .expires_at_us
                    .checked_sub(arrival_at_us)
                    .filter(|headroom| *headroom > 0),
            };
        }
    }

    MediaRecoveryDecision {
        remaining_us,
        ..expired_recovery_decision()
    }
}

fn estimate_fits(estimate_us: u64, remaining_us: u64, safety_margin_us: u64) -> bool {
    estimate_us.saturating_add(safety_margin_us.max(1)) <= remaining_us
}

fn expired_recovery_decision() -> MediaRecoveryDecision {
    MediaRecoveryDecision {
        action: MediaRecoveryAction::Expire,
        remaining_us: 0,
        estimated_arrival_at_us: None,
        estimated_headroom_us: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig, MediaCodec, MediaFecEncoder,
        MediaFrame, MediaFrameMetadata, NetworkMetrics,
    };

    #[test]
    fn source_first_plan_has_explicit_importance_and_path_order() {
        let mut encoder = encoder_with_repairs();
        let init = encode_frame(
            &mut encoder,
            1,
            MediaCodec::Data,
            400,
            MediaFrameFlags::new(MediaFrameFlags::INITIALIZATION),
        );
        let config = encode_frame(
            &mut encoder,
            2,
            MediaCodec::H264,
            400,
            MediaFrameFlags::new(MediaFrameFlags::CODEC_CONFIG),
        );
        let key = encode_frame(
            &mut encoder,
            3,
            MediaCodec::H264,
            8_000,
            MediaFrameFlags::keyframe(),
        );
        let audio = encode_frame(
            &mut encoder,
            4,
            MediaCodec::Opus,
            400,
            MediaFrameFlags::default(),
        );
        let delta = encode_frame(
            &mut encoder,
            5,
            MediaCodec::H264,
            8_000,
            MediaFrameFlags::default(),
        );
        let deadline = MediaDeadline::from_micros(2_000_000);
        let frames = [&delta, &audio, &key, &config, &init]
            .map(|frame| MediaFrameSchedule::new(frame, deadline));

        let plan = plan_media_datagrams_with_deadlines(
            frames,
            MediaSendPolicy::default(),
            MediaScheduleState::default(),
        );

        assert!(!plan.scheduled.is_empty());
        assert!(plan.scheduled.windows(2).all(|window| {
            (role_rank(window[0].role), window[0].object_kind)
                <= (role_rank(window[1].role), window[1].object_kind)
        }));
        assert_eq!(
            plan.scheduled
                .iter()
                .filter(|entry| entry.role == MediaDatagramRole::Source)
                .map(|entry| entry.object_kind)
                .collect::<Vec<_>>()
                .first(),
            Some(&MediaObjectKind::Initialization)
        );
        assert!(plan.scheduled.iter().all(|entry| match entry.role {
            MediaDatagramRole::Source => entry.path_intent == MediaPathIntent::PrimarySource,
            MediaDatagramRole::Repair => entry.path_intent == MediaPathIntent::SecondaryRepair,
        }));
        let first_repair = plan
            .scheduled
            .iter()
            .position(|entry| entry.role == MediaDatagramRole::Repair)
            .expect("repair traffic");
        assert!(plan.scheduled[..first_repair]
            .iter()
            .all(|entry| entry.role == MediaDatagramRole::Source));
        assert!(plan.scheduled[first_repair..]
            .iter()
            .all(|entry| entry.role == MediaDatagramRole::Repair));
    }

    #[test]
    fn bounded_admission_considers_later_important_frames_before_plan_limit() {
        let mut encoder = encoder_with_repairs();
        let delta = encode_frame(
            &mut encoder,
            1,
            MediaCodec::H264,
            64_000,
            MediaFrameFlags::default(),
        );
        let init = encode_frame(
            &mut encoder,
            2,
            MediaCodec::Data,
            400,
            MediaFrameFlags::initialization(),
        );
        let key = encode_frame(
            &mut encoder,
            3,
            MediaCodec::H264,
            400,
            MediaFrameFlags::keyframe(),
        );
        let audio = encode_frame(
            &mut encoder,
            4,
            MediaCodec::Opus,
            400,
            MediaFrameFlags::default(),
        );
        let deadline = MediaDeadline::from_micros(1_000_000);
        let plan = plan_media_datagrams_with_deadlines(
            [&delta, &init, &key, &audio].map(|frame| MediaFrameSchedule::new(frame, deadline)),
            MediaSendPolicy {
                max_in_flight_datagrams: 3,
                max_datagrams_per_plan: 3,
                ..MediaSendPolicy::default()
            },
            MediaScheduleState::default(),
        );

        assert_eq!(plan.scheduled.len(), 3);
        assert_eq!(
            plan.scheduled
                .iter()
                .map(|entry| (entry.object_kind, entry.role))
                .collect::<Vec<_>>(),
            vec![
                (MediaObjectKind::Initialization, MediaDatagramRole::Source),
                (MediaObjectKind::VideoKeyframe, MediaDatagramRole::Source),
                (MediaObjectKind::Audio, MediaDatagramRole::Source),
            ]
        );
        assert!(plan.plan_limit_reached);
    }

    #[test]
    fn urgent_important_repair_precedes_far_deadline_low_priority_source() {
        let mut encoder = MediaFecEncoder::new(AdaptiveFecController::new(
            AdaptiveFecPolicy {
                min_repair_symbols: 1,
                ..AdaptiveFecPolicy::default()
            },
            CongestionConfig::default(),
        ));
        let init = encode_frame(
            &mut encoder,
            1,
            MediaCodec::Data,
            400,
            MediaFrameFlags::initialization(),
        );
        let config = encode_frame(
            &mut encoder,
            2,
            MediaCodec::H264,
            400,
            MediaFrameFlags::new(MediaFrameFlags::CODEC_CONFIG),
        );
        let key = encode_frame(
            &mut encoder,
            3,
            MediaCodec::H264,
            400,
            MediaFrameFlags::keyframe(),
        );
        let audio = encode_frame(
            &mut encoder,
            4,
            MediaCodec::Opus,
            400,
            MediaFrameFlags::default(),
        );
        let delta = encode_frame(
            &mut encoder,
            5,
            MediaCodec::H264,
            8_000,
            MediaFrameFlags::default(),
        );
        let urgent = MediaDeadline::from_micros(URGENT_REPAIR_WINDOW_US);
        let far = MediaDeadline::from_micros(1_000_000);
        let plan = plan_media_datagrams_with_deadlines(
            [
                MediaFrameSchedule::new(&delta, far),
                MediaFrameSchedule::new(&audio, urgent),
                MediaFrameSchedule::new(&key, urgent),
                MediaFrameSchedule::new(&config, urgent),
                MediaFrameSchedule::new(&init, urgent),
            ],
            MediaSendPolicy::default(),
            MediaScheduleState::default(),
        );

        let first_delta_source = plan
            .scheduled
            .iter()
            .position(|entry| {
                entry.object_kind == MediaObjectKind::VideoDelta
                    && entry.role == MediaDatagramRole::Source
            })
            .expect("delta source");
        for kind in [
            MediaObjectKind::Initialization,
            MediaObjectKind::CodecConfig,
            MediaObjectKind::VideoKeyframe,
            MediaObjectKind::Audio,
        ] {
            let repair = plan
                .scheduled
                .iter()
                .position(|entry| {
                    entry.object_kind == kind && entry.role == MediaDatagramRole::Repair
                })
                .expect("important repair");
            assert!(repair < first_delta_source, "{kind:?} repair stayed urgent");
        }
    }

    #[test]
    fn absolute_expiry_accounts_for_queueing_and_pacing() {
        let mut encoder = encoder_with_repairs();
        let frame = encode_frame(
            &mut encoder,
            1,
            MediaCodec::Opus,
            400,
            MediaFrameFlags::default(),
        );
        let deadline = MediaDeadline::from_micros(10_250);
        let plan = plan_media_datagrams_with_deadlines(
            [MediaFrameSchedule::new(&frame, deadline)],
            MediaSendPolicy {
                min_datagram_spacing_us: 200,
                ..MediaSendPolicy::default()
            },
            MediaScheduleState {
                now_us: 10_000,
                queue_delay_us: 100,
                in_flight_datagrams: 0,
            },
        );

        assert_eq!(plan.scheduled.len(), 1);
        assert_eq!(plan.scheduled[0].send_after_us, 0);
        assert_eq!(plan.dropped_expired(), frame.datagrams.len() - 1);
        assert!(plan.dropped.iter().all(|entry| entry.deadline == deadline));
    }

    #[test]
    fn stale_delta_repair_is_dropped_without_suppressing_sources() {
        let mut encoder = encoder_with_repairs();
        let delta = encode_frame(
            &mut encoder,
            1,
            MediaCodec::H264,
            8_000,
            MediaFrameFlags::default(),
        );
        let plan = plan_media_datagrams_with_deadlines(
            [MediaFrameSchedule::new(
                &delta,
                MediaDeadline::from_micros(1_000_000),
            )],
            MediaSendPolicy {
                stale_delta_repair_queue_delay_ms: 2,
                ..MediaSendPolicy::default()
            },
            MediaScheduleState {
                queue_delay_us: 2_000,
                ..MediaScheduleState::default()
            },
        );

        assert_eq!(
            plan.scheduled.len(),
            delta.stats().source_datagrams,
            "all systematic source symbols remain live"
        );
        assert_eq!(
            plan.dropped_stale_delta_repair(),
            delta.stats().repair_datagrams
        );
    }

    #[test]
    fn in_flight_and_plan_limits_keep_allocations_bounded() {
        let mut encoder = encoder_with_repairs();
        let frame = encode_frame(
            &mut encoder,
            1,
            MediaCodec::H264,
            36_000,
            MediaFrameFlags::default(),
        );
        let mut frames_scanned = 0usize;
        let plan = plan_media_datagrams_with_deadlines(
            std::iter::repeat_n(
                MediaFrameSchedule::new(&frame, MediaDeadline::from_micros(u64::MAX)),
                10_000,
            )
            .inspect(|_| frames_scanned += 1),
            MediaSendPolicy {
                max_in_flight_datagrams: usize::MAX,
                max_datagrams_per_plan: HARD_MAX_DATAGRAMS_PER_PLAN + 1,
                ..MediaSendPolicy::default()
            },
            MediaScheduleState::default(),
        );

        assert!(plan.plan_limit_reached);
        assert_eq!(frames_scanned, HARD_MAX_FRAMES_SCANNED_PER_PLAN + 1);
        assert_eq!(
            plan.scheduled.len() + plan.dropped.len(),
            HARD_MAX_DATAGRAMS_PER_PLAN
        );
        assert!(plan.scheduled.len() <= HARD_MAX_DATAGRAMS_PER_PLAN);
    }

    #[test]
    fn in_flight_capacity_keeps_the_highest_priority_source() {
        let mut encoder = encoder_with_repairs();
        let init = encode_frame(
            &mut encoder,
            1,
            MediaCodec::Data,
            400,
            MediaFrameFlags::initialization(),
        );
        let delta = encode_frame(
            &mut encoder,
            2,
            MediaCodec::H264,
            8_000,
            MediaFrameFlags::default(),
        );
        let deadline = MediaDeadline::from_micros(1_000_000);
        let plan = plan_media_datagrams_with_deadlines(
            [
                MediaFrameSchedule::new(&delta, deadline),
                MediaFrameSchedule::new(&init, deadline),
            ],
            MediaSendPolicy {
                max_in_flight_datagrams: 2,
                ..MediaSendPolicy::default()
            },
            MediaScheduleState {
                in_flight_datagrams: 1,
                ..MediaScheduleState::default()
            },
        );

        assert_eq!(plan.scheduled.len(), 1);
        assert_eq!(
            plan.scheduled[0].class,
            MediaDatagramClass::InitializationSource
        );
        assert!(plan.blocked_by_in_flight > 0);
        assert!(plan
            .dropped
            .iter()
            .all(|entry| entry.reason != MediaDropReason::InFlightLimit));
    }

    #[test]
    fn recovery_prefers_timely_secondary_raptorq_repair_even_when_fetch_is_faster() {
        let input = recovery_input();
        let decision = MediaRecoveryPolicy::default().decide(input);

        assert_eq!(
            decision.action,
            MediaRecoveryAction::SendRaptorQRepair {
                repair_symbols: 3,
                path_intent: MediaPathIntent::SecondaryRepair,
            }
        );
        assert_eq!(decision.remaining_us, 30_000);
        assert_eq!(decision.estimated_arrival_at_us, Some(108_500));
        assert_eq!(decision.estimated_headroom_us, Some(21_500));
    }

    #[test]
    fn recovery_uses_reliable_fetch_when_extra_fec_cannot_make_deadline() {
        let decision = MediaRecoveryPolicy::default().decide(MediaRecoveryInput {
            secondary_rtt_us: 40_000,
            reliable_fetch_estimate_us: Some(20_000),
            ..recovery_input()
        });

        assert_eq!(
            decision.action,
            MediaRecoveryAction::ReliableFetch {
                path_intent: MediaPathIntent::ReliableObjectFetch,
            }
        );
        assert_eq!(decision.estimated_arrival_at_us, Some(120_000));
        assert_eq!(decision.estimated_headroom_us, Some(10_000));
    }

    #[test]
    fn recovery_expires_when_neither_mechanism_can_arrive_safely() {
        let decision = MediaRecoveryPolicy::default().decide(MediaRecoveryInput {
            secondary_rtt_us: 40_000,
            reliable_fetch_estimate_us: Some(30_000),
            ..recovery_input()
        });

        assert_eq!(decision.action, MediaRecoveryAction::Expire);
        assert_eq!(decision.remaining_us, 30_000);
        assert_eq!(decision.estimated_arrival_at_us, None);
        assert_eq!(decision.estimated_headroom_us, None);
    }

    #[test]
    fn recovery_rejects_an_arrival_exactly_at_expiry() {
        let decision = MediaRecoveryPolicy {
            deadline_safety_margin_us: 0,
            ..MediaRecoveryPolicy::default()
        }
        .decide(MediaRecoveryInput {
            deadline: MediaDeadline::from_micros(108_000),
            uncovered_source_symbols: 1,
            secondary_rtt_us: 8_000,
            repair_symbol_spacing_us: 0,
            reliable_fetch_estimate_us: None,
            ..recovery_input()
        });

        assert_eq!(decision.action, MediaRecoveryAction::Expire);
    }

    #[test]
    fn completion_outcome_exposes_latency_headroom_and_lateness() {
        let deadline = MediaDeadline::from_micros(130_000);
        let hit = deadline.observe_completion(100_000, 129_500);
        let exact_expiry = deadline.observe_completion(100_000, 130_000);
        let late = deadline.observe_completion(100_000, 133_000);

        assert_eq!(hit.elapsed_us, 29_500);
        assert!(hit.deadline_hit);
        assert_eq!(hit.headroom_us, 500);
        assert_eq!(hit.lateness_us, 0);
        assert!(!exact_expiry.deadline_hit);
        assert_eq!(exact_expiry.lateness_us, 0);
        assert!(!late.deadline_hit);
        assert_eq!(late.elapsed_us, 33_000);
        assert_eq!(late.lateness_us, 3_000);
    }

    #[test]
    fn recovery_policy_is_deterministic_and_repair_is_hard_bounded() {
        let policy = MediaRecoveryPolicy {
            max_extra_repair_symbols: u32::MAX,
            repair_safety_symbols: 0,
            deadline_safety_margin_us: 0,
        };
        assert_eq!(
            policy.normalized().max_extra_repair_symbols,
            HARD_MAX_EXTRA_REPAIR_SYMBOLS
        );
        for missing in [0, 1, 64, 4_095, 4_096, 4_097, u32::MAX] {
            for remaining_us in [0, 1, 10_000, u64::MAX] {
                let input = MediaRecoveryInput {
                    now_us: 100,
                    deadline: MediaDeadline::from_micros(100_u64.saturating_add(remaining_us)),
                    uncovered_source_symbols: missing,
                    secondary_rtt_us: 1,
                    secondary_queue_delay_us: 0,
                    repair_symbol_spacing_us: 0,
                    reliable_fetch_estimate_us: None,
                };
                let first = policy.decide(input);
                let second = policy.decide(input);
                assert_eq!(first, second);
                if let MediaRecoveryAction::SendRaptorQRepair { repair_symbols, .. } = first.action
                {
                    assert!(repair_symbols <= HARD_MAX_EXTRA_REPAIR_SYMBOLS);
                }
            }
        }
    }

    fn encoder_with_repairs() -> MediaFecEncoder {
        let mut encoder = MediaFecEncoder::default();
        encoder
            .controller_mut()
            .update_network_metrics(NetworkMetrics {
                loss_fraction: 0.03,
                ..NetworkMetrics::default()
            });
        encoder
    }

    fn encode_frame(
        encoder: &mut MediaFecEncoder,
        sequence: u64,
        codec: MediaCodec,
        payload_len: usize,
        flags: MediaFrameFlags,
    ) -> EncodedMediaFrame {
        let mut metadata = MediaFrameMetadata::new(7, sequence, 0, codec);
        metadata.duration_ms = 20;
        metadata.flags = flags;
        let payload = vec![sequence as u8; payload_len];
        encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode frame")
    }

    fn recovery_input() -> MediaRecoveryInput {
        MediaRecoveryInput {
            now_us: 100_000,
            deadline: MediaDeadline::from_micros(130_000),
            uncovered_source_symbols: 2,
            secondary_rtt_us: 8_000,
            secondary_queue_delay_us: 0,
            repair_symbol_spacing_us: 250,
            reliable_fetch_estimate_us: Some(1_000),
        }
    }
}
