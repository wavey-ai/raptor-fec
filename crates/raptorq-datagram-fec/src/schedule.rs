use crate::{
    EncodedMediaFrame, MediaDatagramOrder, MediaDatagramRole, MediaFrameFlags, MediaPriority,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MediaSendPolicy {
    pub order: MediaDatagramOrder,
    pub max_in_flight_datagrams: usize,
    pub min_datagram_spacing_us: u64,
    pub playout_latency_ms: u32,
    pub stale_delta_repair_queue_delay_ms: u32,
}

impl Default for MediaSendPolicy {
    fn default() -> Self {
        Self {
            order: MediaDatagramOrder::SourceFirst,
            max_in_flight_datagrams: usize::MAX,
            min_datagram_spacing_us: 0,
            playout_latency_ms: 33,
            stale_delta_repair_queue_delay_ms: 20,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MediaQueueState {
    pub now_ms: u64,
    pub queue_delay_ms: u32,
    pub in_flight_datagrams: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum MediaDatagramClass {
    AudioSource,
    AudioRepair,
    CodecConfigSource,
    CodecConfigRepair,
    VideoKeySource,
    VideoDeltaSource,
    VideoKeyRepair,
    VideoDeltaRepair,
    DataSource,
    DataRepair,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MediaDropReason {
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
    pub class: MediaDatagramClass,
    pub deadline_ms: u64,
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
    pub class: MediaDatagramClass,
    pub reason: MediaDropReason,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MediaSendPlan {
    pub scheduled: Vec<MediaScheduledDatagram>,
    pub dropped: Vec<MediaDroppedDatagram>,
    pub blocked_by_in_flight: usize,
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
}

impl EncodedMediaFrame {
    pub fn scheduled_datagram_send_plan(
        &self,
        policy: MediaSendPolicy,
        queue: MediaQueueState,
    ) -> MediaSendPlan {
        plan_media_datagrams([self], policy, queue)
    }
}

pub fn plan_media_datagrams<'a, I>(
    frames: I,
    policy: MediaSendPolicy,
    queue: MediaQueueState,
) -> MediaSendPlan
where
    I: IntoIterator<Item = &'a EncodedMediaFrame>,
{
    let mut scheduled = Vec::new();
    let mut dropped = Vec::new();

    for frame in frames {
        let deadline_ms = frame_deadline_ms(frame, policy);
        for entry in frame.datagram_send_plan(policy.order) {
            let class = datagram_class(frame, entry.role);
            if should_drop_stale_delta_repair(frame, entry.role, deadline_ms, policy, queue) {
                dropped.push(MediaDroppedDatagram {
                    frame_sequence: frame.sequence,
                    stream_id: frame.metadata.stream_id,
                    datagram_index: entry.datagram_index,
                    block_id: entry.block_id,
                    fragment_index: entry.fragment_index,
                    role: entry.role,
                    class,
                    reason: MediaDropReason::StaleDeltaRepair,
                });
                continue;
            }

            scheduled.push(MediaScheduledDatagram {
                frame_sequence: frame.sequence,
                stream_id: frame.metadata.stream_id,
                datagram_index: entry.datagram_index,
                block_id: entry.block_id,
                fragment_index: entry.fragment_index,
                role: entry.role,
                class,
                deadline_ms,
                send_after_us: 0,
            });
        }
    }

    scheduled.sort_by_key(|entry| {
        (
            entry.class,
            entry.deadline_ms,
            entry.stream_id,
            entry.frame_sequence,
            entry.fragment_index,
            entry.datagram_index,
        )
    });

    let capacity = policy
        .max_in_flight_datagrams
        .saturating_sub(queue.in_flight_datagrams);
    let blocked_by_in_flight = scheduled.len().saturating_sub(capacity);
    while scheduled.len() > capacity {
        let entry = scheduled.pop().expect("len checked");
        dropped.push(MediaDroppedDatagram {
            frame_sequence: entry.frame_sequence,
            stream_id: entry.stream_id,
            datagram_index: entry.datagram_index,
            block_id: entry.block_id,
            fragment_index: entry.fragment_index,
            role: entry.role,
            class: entry.class,
            reason: MediaDropReason::InFlightLimit,
        });
    }

    for (ordinal, entry) in scheduled.iter_mut().enumerate() {
        entry.send_after_us = ordinal as u64 * policy.min_datagram_spacing_us;
    }

    MediaSendPlan {
        scheduled,
        dropped,
        blocked_by_in_flight,
    }
}

fn should_drop_stale_delta_repair(
    frame: &EncodedMediaFrame,
    role: MediaDatagramRole,
    deadline_ms: u64,
    policy: MediaSendPolicy,
    queue: MediaQueueState,
) -> bool {
    frame.priority == MediaPriority::VideoDelta
        && role == MediaDatagramRole::Repair
        && (queue.queue_delay_ms >= policy.stale_delta_repair_queue_delay_ms
            || deadline_ms <= queue.now_ms.saturating_add(u64::from(queue.queue_delay_ms)))
}

fn datagram_class(frame: &EncodedMediaFrame, role: MediaDatagramRole) -> MediaDatagramClass {
    match (
        frame.priority,
        frame.metadata.flags.contains(MediaFrameFlags::CODEC_CONFIG),
        role,
    ) {
        (MediaPriority::Audio, _, MediaDatagramRole::Source) => MediaDatagramClass::AudioSource,
        (MediaPriority::Audio, _, MediaDatagramRole::Repair) => MediaDatagramClass::AudioRepair,
        (_, true, MediaDatagramRole::Source) => MediaDatagramClass::CodecConfigSource,
        (_, true, MediaDatagramRole::Repair) => MediaDatagramClass::CodecConfigRepair,
        (MediaPriority::VideoKey, _, MediaDatagramRole::Source) => {
            MediaDatagramClass::VideoKeySource
        }
        (MediaPriority::VideoKey, _, MediaDatagramRole::Repair) => {
            MediaDatagramClass::VideoKeyRepair
        }
        (MediaPriority::VideoDelta, _, MediaDatagramRole::Source) => {
            MediaDatagramClass::VideoDeltaSource
        }
        (MediaPriority::VideoDelta, _, MediaDatagramRole::Repair) => {
            MediaDatagramClass::VideoDeltaRepair
        }
        (_, _, MediaDatagramRole::Source) => MediaDatagramClass::DataSource,
        (_, _, MediaDatagramRole::Repair) => MediaDatagramClass::DataRepair,
    }
}

fn frame_deadline_ms(frame: &EncodedMediaFrame, policy: MediaSendPolicy) -> u64 {
    frame
        .metadata
        .pts_ms
        .saturating_add(u64::from(frame.metadata.duration_ms))
        .saturating_add(u64::from(policy.playout_latency_ms))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MediaCodec, MediaFecEncoder, MediaFrame, MediaFrameMetadata};

    #[test]
    fn deadline_aware_plan_prioritizes_audio_key_sources_and_drops_stale_delta_repair() {
        let mut encoder = MediaFecEncoder::default();
        let audio = encode_frame(&mut encoder, 1, 1_000, 20, MediaCodec::Opus, 400, false);
        let key = encode_frame(&mut encoder, 2, 1_010, 33, MediaCodec::H264, 8_000, true);
        let delta = encode_frame(&mut encoder, 3, 1_020, 33, MediaCodec::H264, 18_000, false);

        let plan = plan_media_datagrams(
            [&delta, &key, &audio],
            MediaSendPolicy {
                min_datagram_spacing_us: 250,
                max_in_flight_datagrams: usize::MAX,
                stale_delta_repair_queue_delay_ms: 1,
                ..MediaSendPolicy::default()
            },
            MediaQueueState {
                queue_delay_ms: 5,
                ..MediaQueueState::default()
            },
        );

        assert_eq!(
            plan.scheduled
                .iter()
                .take_while(|entry| entry.class == MediaDatagramClass::AudioSource)
                .count(),
            audio.stats().source_datagrams
        );
        assert_eq!(
            plan.scheduled
                .iter()
                .position(|entry| entry.class == MediaDatagramClass::VideoKeySource),
            Some(audio.datagrams.len())
        );
        assert_eq!(
            plan.dropped_stale_delta_repair(),
            delta.stats().repair_datagrams
        );
        assert!(plan
            .scheduled
            .windows(2)
            .all(|window| window[1].send_after_us >= window[0].send_after_us));
        assert_eq!(plan.scheduled[1].send_after_us, 250);
    }

    #[test]
    fn in_flight_cap_keeps_best_ranked_datagrams() {
        let mut encoder = MediaFecEncoder::default();
        let audio = encode_frame(&mut encoder, 1, 1_000, 20, MediaCodec::Opus, 400, false);
        let delta = encode_frame(&mut encoder, 3, 1_020, 33, MediaCodec::H264, 18_000, false);

        let plan = plan_media_datagrams(
            [&delta, &audio],
            MediaSendPolicy {
                max_in_flight_datagrams: 3,
                ..MediaSendPolicy::default()
            },
            MediaQueueState {
                in_flight_datagrams: 1,
                ..MediaQueueState::default()
            },
        );

        assert_eq!(plan.scheduled.len(), 2);
        assert!(plan.blocked_by_in_flight > 0);
        assert!(plan.scheduled.iter().all(|entry| matches!(
            entry.class,
            MediaDatagramClass::AudioSource | MediaDatagramClass::AudioRepair
        )));
        assert!(plan
            .dropped
            .iter()
            .any(|entry| entry.reason == MediaDropReason::InFlightLimit));
    }

    fn encode_frame(
        encoder: &mut MediaFecEncoder,
        sequence: u64,
        pts_ms: u64,
        duration_ms: u32,
        codec: MediaCodec,
        payload_len: usize,
        keyframe: bool,
    ) -> EncodedMediaFrame {
        let mut metadata = MediaFrameMetadata::new(7, sequence, pts_ms, codec);
        metadata.duration_ms = duration_ms;
        if keyframe {
            metadata.flags = MediaFrameFlags::keyframe();
        }
        let payload = vec![sequence as u8; payload_len];
        encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode frame")
    }
}
