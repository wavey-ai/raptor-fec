use raptorq_datagram_fec::{
    AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig, DecodedMediaFrame,
    EncodedMediaBlock, EncodedMediaFrame, MediaCodec, MediaFecDecoder, MediaFecEncoder, MediaFrame,
    MediaFrameFlags, MediaFrameMetadata, NetworkMetrics, DEFAULT_SYMBOL_SIZE,
};
use rist_core::{
    packet::rtcp::NackMode, time::ntp_from_unix_duration, RtcpIntervals, SimpleReceiverCore,
    SimpleSenderCore,
};
use std::collections::{BTreeMap, BTreeSet};
use std::net::SocketAddr;
use std::time::{Duration, Instant};
use tokio::net::UdpSocket;
use tokio::time::{sleep, timeout};

// Matches the default feedback interval in rist-core's SimpleReceiverCore RTCP scheduler.
const RIST_DEFAULT_FEEDBACK_INTERVAL_MS: u32 = 20;
const VIDEO_TEST_RTT_MS: u32 = 70;
const LOW_LATENCY_PLAYOUT_MS: u32 = 33;
const RIST_CATCHUP_PLAYOUT_MS: u32 = 95;
// Matches upload-response's SRT read size and the common MPEG-TS/SRT payload size.
const SRT_PACKET_PAYLOAD_SIZE: usize = 1316;

#[derive(Debug, Clone, Copy)]
enum LossPattern {
    Burst { start: usize, len: usize },
    Periodic { every: usize, phase: usize },
    RandomExact { seed: u64, count: usize },
}

#[derive(Debug, Clone, Copy)]
struct VideoScenario {
    name: &'static str,
    payload_len: usize,
    flags: MediaFrameFlags,
    pattern: LossPattern,
    max_wire_overhead: f32,
}

#[derive(Debug, Clone, Copy)]
struct StreamFrameScenario {
    payload_len: usize,
    flags: MediaFrameFlags,
    pattern: LossPattern,
}

#[derive(Debug, Default)]
struct StreamSimulation {
    frame_count: usize,
    fec_recovered_frames: usize,
    rist_ready_frames: usize,
    srt_best_case_ready_frames: usize,
    lost_frames: usize,
    source_datagrams: usize,
    wire_datagrams: usize,
}

#[derive(Debug, Clone, Copy)]
struct RandomizedVideoNetwork {
    name: &'static str,
    rtt_ms: u32,
    feedback_interval_ms: u32,
    playout_latency_ms: u32,
    loss_fraction: f32,
    burst_fraction: f32,
    seed: u64,
}

#[derive(Debug, Default)]
struct RandomizedVideoScorecard {
    frame_count: usize,
    source_loss_frames: usize,
    repair_loss_frames: usize,
    fec_recovered_frames: usize,
    fec_recovered_source_loss_frames: usize,
    fec_failed_frames: usize,
    fec_failed_no_source_loss_frames: usize,
    rist_ready_frames: usize,
    srt_best_case_ready_frames: usize,
    source_datagrams: usize,
    wire_datagrams: usize,
    lost_source_datagrams: usize,
    lost_repair_datagrams: usize,
}

#[derive(Debug, Clone, Copy)]
enum BoundedLossShape {
    Front,
    Late,
    Periodic { every: usize, phase: usize },
    Random { seed: u64 },
    Alternating { seed: u64 },
}

#[derive(Debug, Clone, Copy)]
struct BroadVideoImpairmentProfile {
    name: &'static str,
    rtt_ms: u32,
    feedback_interval_ms: u32,
    playout_latency_ms: u32,
    frame_count: usize,
    max_source_loss_per_block: usize,
    repair_noise_every: usize,
    reorder_span: usize,
    shape: BoundedLossShape,
    max_wire_overhead: f32,
}

#[derive(Debug, Default)]
struct BroadVideoScorecard {
    frame_count: usize,
    source_loss_frames: usize,
    repair_loss_frames: usize,
    fec_recovered_frames: usize,
    fec_failed_frames: usize,
    rist_ready_frames: usize,
    srt_best_case_ready_frames: usize,
    source_datagrams: usize,
    wire_datagrams: usize,
    lost_source_datagrams: usize,
    lost_repair_datagrams: usize,
    reordered_frames: usize,
}

#[derive(Debug)]
struct RistFrameRecovery {
    dropped_packets: usize,
    retransmitted_packets: usize,
    retransmission_arrival_ms: u32,
    recovered_payload: Vec<u8>,
}

#[derive(Debug)]
struct RistStreamRecovery {
    frame_count: usize,
    recovered_frames: usize,
    lost_frames: usize,
    feedback_missed_frames: usize,
    dropped_packets: usize,
    retransmitted_packets: usize,
    retransmission_arrival_ms: u32,
}

#[derive(Debug)]
struct SrtStyleFrameRecovery {
    dropped_packets: usize,
    retransmitted_packets: usize,
    retransmission_arrival_ms: u32,
    recovered_payload: Vec<u8>,
}

#[derive(Debug)]
struct LossyUdpProxyStats {
    received: usize,
    forwarded: usize,
    dropped: usize,
    delayed: usize,
}

#[derive(Debug, Clone, Copy)]
struct DatagramImpairment {
    drop: bool,
    delay_ms: u64,
}

#[derive(Debug)]
struct ScheduledDatagram {
    ordinal: usize,
    delay_ms: u64,
    bytes: Vec<u8>,
}

#[derive(Debug, Default)]
struct StreamUdpProxyStats {
    received: usize,
    forwarded: usize,
    dropped: usize,
    delayed: usize,
    reordered: usize,
}

#[derive(Debug)]
struct ExpectedLiveFrame {
    metadata: MediaFrameMetadata,
    payload: Vec<u8>,
}

#[test]
fn video_access_units_recover_from_feedback_free_loss_matrix() {
    let scenarios = [
        VideoScenario {
            name: "keyframe-burst-loss",
            payload_len: 40_000,
            flags: MediaFrameFlags::keyframe(),
            pattern: LossPattern::Burst { start: 3, len: 8 },
            max_wire_overhead: 1.34,
        },
        VideoScenario {
            name: "keyframe-periodic-loss",
            payload_len: 40_000,
            flags: MediaFrameFlags::keyframe(),
            pattern: LossPattern::Periodic { every: 7, phase: 1 },
            max_wire_overhead: 1.34,
        },
        VideoScenario {
            name: "delta-random-loss",
            payload_len: 18_000,
            flags: MediaFrameFlags::default(),
            pattern: LossPattern::RandomExact {
                seed: 0xD317_A11D_E1A5_E5ED,
                count: 3,
            },
            max_wire_overhead: 1.25,
        },
    ];

    for scenario in scenarios {
        let (encoded, decoded, dropped) = run_scenario(scenario);
        let block_layout = encoded_block_layout(&encoded);
        let wire_overhead =
            encoded.datagrams.len() as f32 / total_source_symbols(&block_layout) as f32;

        assert!(
            !dropped.is_empty(),
            "{} must exercise actual packet loss",
            scenario.name
        );
        assert!(
            total_lost_source_symbols(&block_layout, &dropped) > 0,
            "{} must drop at least one source symbol, not only repair packets",
            scenario.name
        );
        assert_loss_within_repair_budget(scenario.name, &block_layout, &dropped);
        assert!(
            wire_overhead <= scenario.max_wire_overhead,
            "{} overhead {wire_overhead:.3} exceeded {}",
            scenario.name,
            scenario.max_wire_overhead
        );
        assert_eq!(
            decoded.payload,
            video_payload(scenario.payload_len),
            "{} decoded payload mismatch",
            scenario.name
        );
    }
}

#[test]
fn video_keyframes_receive_enough_parity_for_single_rtt_burst_loss() {
    let scenario = VideoScenario {
        name: "keyframe-late-burst",
        payload_len: 64_000,
        flags: MediaFrameFlags::keyframe(),
        pattern: LossPattern::Burst { start: 19, len: 10 },
        max_wire_overhead: 1.34,
    };

    let (encoded, decoded, dropped) = run_scenario(scenario);
    let block_layout = encoded_block_layout(&encoded);

    assert_eq!(decoded.payload, video_payload(scenario.payload_len));
    assert_eq!(dropped.len(), 10);
    assert!(
        max_lost_source_symbols_per_block(&block_layout, &dropped) >= 10,
        "scenario should drop a full 10-source-symbol burst in one keyframe block"
    );
    assert_loss_within_repair_budget(scenario.name, &block_layout, &dropped);
}

#[test]
fn video_recovery_sweep_covers_burst_periodic_and_random_bounded_loss() {
    let scenarios = [
        ("tiny-key", 4_000, MediaFrameFlags::keyframe(), 1.75),
        ("tiny-delta", 7_200, MediaFrameFlags::default(), 1.36),
        ("small-delta", 9_000, MediaFrameFlags::default(), 1.30),
        ("delta", 18_000, MediaFrameFlags::default(), 1.25),
        ("small-key", 24_000, MediaFrameFlags::keyframe(), 1.36),
        ("key", 40_000, MediaFrameFlags::keyframe(), 1.34),
        ("large-key", 96_000, MediaFrameFlags::keyframe(), 1.34),
    ];
    let mut recovered_cases = 0usize;
    let mut fail_closed_cases = 0usize;
    let mut feedback_missed_cases = 0usize;

    for (name, payload_len, flags, max_wire_overhead) in scenarios {
        let payload = video_payload(payload_len);
        let encoded = encode_video_frame(payload_len, flags);
        let block_layout = encoded_block_layout(&encoded);
        let total_repair_symbols = block_layout
            .iter()
            .map(|block| block.repair_symbols as usize)
            .sum::<usize>();
        let total_source_symbols = block_layout
            .iter()
            .map(|block| usize::from(block.source_symbols))
            .sum::<usize>();
        let wire_overhead = encoded.datagrams.len() as f32 / total_source_symbols as f32;

        assert!(
            total_repair_symbols > 0,
            "{name} should receive non-zero forward repair in the lossy-video profile"
        );
        assert!(
            wire_overhead <= max_wire_overhead,
            "{name} overhead {wire_overhead:.3} exceeded {max_wire_overhead}"
        );

        let drop_sets = [
            ("front-burst", block_front_source_indices(&block_layout, 4)),
            ("late-burst", block_late_source_indices(&block_layout, 5)),
            (
                "periodic",
                block_periodic_source_indices(&block_layout, 3, 1, 6),
            ),
            (
                "random",
                block_random_source_indices(&block_layout, 0xFEC0_0000 ^ payload_len as u64),
            ),
        ];

        for (loss_name, dropped) in drop_sets {
            if dropped.is_empty() {
                continue;
            }
            let decoded = decode_with_loss(&encoded, &dropped)
                .unwrap_or_else(|| panic!("{name}/{loss_name} did not recover"));

            assert!(
                block_loss_is_within_repair_budget(&block_layout, &dropped),
                "{name}/{loss_name} loss set {:?} exceeds a per-block repair budget",
                dropped
            );
            assert_eq!(
                decoded.payload, payload,
                "{name}/{loss_name} decoded payload mismatch"
            );
            recovered_cases += 1;
            if !feedback_only_arq_can_recover_before_deadline(
                dropped.len(),
                VIDEO_TEST_RTT_MS,
                RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
                LOW_LATENCY_PLAYOUT_MS,
            ) {
                feedback_missed_cases += 1;
            }
        }

        let over_budget = first_block_over_budget_source_indices(&block_layout);
        assert!(
            decode_with_loss(&encoded, &over_budget).is_none(),
            "{name} should fail closed when one more source datagram is lost than the repair budget"
        );
        fail_closed_cases += 1;
    }

    assert!(
        recovered_cases >= 20,
        "deterministic sweep should cover many recoverable video loss shapes"
    );
    assert_eq!(
        feedback_missed_cases, recovered_cases,
        "every recovered sweep case should be a sub-RTT frame where feedback repair misses the low-latency deadline"
    );
    assert_eq!(
        fail_closed_cases,
        scenarios.len(),
        "every sweep scenario should prove the over-budget failure boundary"
    );
}

#[test]
fn fec_recovers_before_feedback_retransmission_deadline_for_low_latency_video() {
    let scenario = VideoScenario {
        name: "keyframe-low-latency-feedback-comparison",
        payload_len: 40_000,
        flags: MediaFrameFlags::keyframe(),
        pattern: LossPattern::Burst { start: 3, len: 8 },
        max_wire_overhead: 1.34,
    };

    let (_encoded, decoded, dropped) = run_scenario(scenario);

    assert_eq!(decoded.payload, video_payload(scenario.payload_len));
    assert!(!dropped.is_empty());
    assert!(
        !feedback_only_arq_can_recover_before_deadline(
            dropped.len(),
            VIDEO_TEST_RTT_MS,
            RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
            LOW_LATENCY_PLAYOUT_MS,
        ),
        "RIST/SRT-style feedback retransmission needs a feedback turn plus network RTT and misses this frame deadline"
    );
}

#[tokio::test]
async fn media_fec_recovers_video_access_units_over_lossy_udp_proxy_matrix() {
    let scenarios = [
        VideoScenario {
            name: "keyframe-live-burst",
            payload_len: 40_000,
            flags: MediaFrameFlags::keyframe(),
            pattern: LossPattern::Burst { start: 3, len: 8 },
            max_wire_overhead: 1.34,
        },
        VideoScenario {
            name: "keyframe-live-late-burst",
            payload_len: 64_000,
            flags: MediaFrameFlags::keyframe(),
            pattern: LossPattern::Burst { start: 19, len: 10 },
            max_wire_overhead: 1.34,
        },
        VideoScenario {
            name: "delta-live-random",
            payload_len: 18_000,
            flags: MediaFrameFlags::default(),
            pattern: LossPattern::RandomExact {
                seed: 0xD317_A11D_E1A5_E5ED,
                count: 3,
            },
            max_wire_overhead: 1.25,
        },
    ];

    for scenario in scenarios {
        let (encoded, decoded, dropped, stats, metadata) =
            run_live_media_fec_udp_scenario(scenario).await;
        let block_layout = encoded_block_layout(&encoded);
        let wire_overhead =
            encoded.datagrams.len() as f32 / total_source_symbols(&block_layout) as f32;

        assert!(
            !dropped.is_empty(),
            "{} must exercise actual packet loss",
            scenario.name
        );
        assert!(
            total_lost_source_symbols(&block_layout, &dropped) > 0,
            "{} must drop at least one source symbol, not only repair packets",
            scenario.name
        );
        assert_loss_within_repair_budget(scenario.name, &block_layout, &dropped);
        assert!(
            wire_overhead <= scenario.max_wire_overhead,
            "{} overhead {wire_overhead:.3} exceeded {}",
            scenario.name,
            scenario.max_wire_overhead
        );
        assert!(
            !feedback_only_arq_can_recover_before_deadline(
                dropped.len(),
                VIDEO_TEST_RTT_MS,
                RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
                LOW_LATENCY_PLAYOUT_MS,
            ),
            "{} should be a sub-RTT case where feedback-only repair misses the frame deadline",
            scenario.name
        );
        assert_eq!(
            stats.received,
            encoded.datagrams.len(),
            "{} proxy receive count mismatch",
            scenario.name
        );
        assert_eq!(
            stats.dropped,
            dropped.len(),
            "{} proxy drop count mismatch",
            scenario.name
        );
        assert_eq!(
            stats.forwarded,
            encoded.datagrams.len().saturating_sub(dropped.len()),
            "{} proxy forward count mismatch",
            scenario.name
        );
        assert!(
            stats.delayed > 0,
            "{} proxy should add loopback jitter",
            scenario.name
        );
        assert_eq!(
            decoded.metadata, metadata,
            "{} metadata mismatch",
            scenario.name
        );
        assert_eq!(
            decoded.payload,
            video_payload(scenario.payload_len),
            "{} decoded payload mismatch",
            scenario.name
        );
    }
}

#[tokio::test]
async fn media_fec_recovers_sustained_video_stream_with_loss_jitter_and_reordering() {
    let frames = stream_scorecard_frames()
        .into_iter()
        .take(30)
        .collect::<Vec<_>>();
    let mut encoder = MediaFecEncoder::new(video_controller());
    let mut expected_frames = BTreeMap::new();
    let mut datagrams = Vec::new();
    let mut impairments = Vec::new();
    let mut source_datagrams = 0usize;
    let mut wire_datagrams = 0usize;
    let mut lost_frames = 0usize;
    let mut feedback_missed_frames = 0usize;

    for (frame_index, frame) in frames.iter().enumerate() {
        let payload = video_payload(frame.payload_len);
        let metadata = MediaFrameMetadata {
            duration_ms: 16,
            flags: frame.flags,
            ..MediaFrameMetadata::new(
                42,
                encoder.allocate_sequence(),
                (frame_index as u64) * 16,
                MediaCodec::H264,
            )
        };
        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode sustained stream access unit");
        let dropped = dropped_indices(frame.pattern, encoded.datagrams.len());
        let block_layout = encoded_block_layout(&encoded);

        assert_loss_within_repair_budget(
            &format!("sustained stream frame {frame_index}"),
            &block_layout,
            &dropped,
        );
        if !dropped.is_empty() {
            assert!(
                total_lost_source_symbols(&block_layout, &dropped) > 0,
                "frame {frame_index} must drop at least one source symbol"
            );
            lost_frames += 1;
            if !feedback_only_arq_can_recover_before_deadline(
                dropped.len(),
                VIDEO_TEST_RTT_MS,
                RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
                LOW_LATENCY_PLAYOUT_MS,
            ) {
                feedback_missed_frames += 1;
            }
        }

        source_datagrams += total_source_symbols(&block_layout);
        wire_datagrams += encoded.datagrams.len();
        for (datagram_index, datagram) in encoded.datagrams.into_iter().enumerate() {
            let ordinal = datagrams.len();
            datagrams.push(datagram);
            impairments.push(DatagramImpairment {
                drop: dropped.contains(&datagram_index),
                delay_ms: stream_reorder_delay_ms(ordinal),
            });
        }
        expected_frames.insert(metadata.sequence, ExpectedLiveFrame { metadata, payload });
    }

    let receiver_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind sustained stream receiver");
    let receiver_addr = receiver_socket
        .local_addr()
        .expect("sustained stream receiver addr");
    let proxy_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind sustained stream proxy");
    let proxy_addr = proxy_socket
        .local_addr()
        .expect("sustained stream proxy addr");
    let sender_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind sustained stream sender");
    let proxy = tokio::spawn(forward_datagrams_with_impairments(
        proxy_socket,
        receiver_addr,
        impairments,
    ));
    let receiver = tokio::spawn(receive_decoded_media_frames(
        receiver_socket,
        expected_frames.len(),
    ));

    for datagram in &datagrams {
        sender_socket
            .send_to(datagram, proxy_addr)
            .await
            .expect("send sustained stream datagram to proxy");
    }

    let stats = timeout(Duration::from_secs(5), proxy)
        .await
        .expect("sustained stream proxy timed out")
        .expect("sustained stream proxy task panicked")
        .expect("sustained stream proxy failed");
    let decoded_frames = timeout(Duration::from_secs(5), receiver)
        .await
        .expect("sustained stream receiver timed out")
        .expect("sustained stream receiver task panicked")
        .expect("sustained stream receiver failed");
    let decoded_by_sequence = decoded_frames
        .into_iter()
        .map(|frame| (frame.metadata.sequence, frame))
        .collect::<BTreeMap<_, _>>();
    let wire_overhead = wire_datagrams as f32 / source_datagrams as f32;

    assert_eq!(stats.received, datagrams.len());
    assert_eq!(stats.forwarded + stats.dropped, datagrams.len());
    assert!(stats.dropped > 0, "stream proxy should drop datagrams");
    assert!(stats.delayed > 0, "stream proxy should delay datagrams");
    assert!(stats.reordered > 0, "stream proxy should reorder datagrams");
    assert!(
        lost_frames >= 8,
        "sustained stream should exercise repeated frame losses"
    );
    assert_eq!(
        feedback_missed_frames, lost_frames,
        "every lost frame in this 70 ms RTT stream should miss a 33 ms feedback-only deadline"
    );
    assert!(
        wire_overhead <= 1.30,
        "sustained stream wire overhead {wire_overhead:.3} exceeded low-latency budget"
    );
    assert_eq!(decoded_by_sequence.len(), expected_frames.len());

    for (sequence, expected) in expected_frames {
        let decoded = decoded_by_sequence
            .get(&sequence)
            .unwrap_or_else(|| panic!("missing decoded frame {sequence}"));
        assert_eq!(decoded.metadata, expected.metadata);
        assert_eq!(decoded.payload, expected.payload);
    }
}

#[test]
fn pure_rist_core_retransmission_misses_deadline_that_forward_fec_meets() {
    let scenario = VideoScenario {
        name: "keyframe-pure-rist-comparison",
        payload_len: 40_000,
        flags: MediaFrameFlags::keyframe(),
        pattern: LossPattern::Burst { start: 3, len: 8 },
        max_wire_overhead: 1.34,
    };
    let payload = video_payload(scenario.payload_len);
    let (_encoded, decoded, dropped) = run_scenario(scenario);
    let rist = run_pure_rist_frame_recovery(
        &payload,
        &dropped,
        VIDEO_TEST_RTT_MS,
        RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
    );

    assert_eq!(decoded.payload, payload);
    assert_eq!(rist.dropped_packets, dropped.len());
    assert_eq!(rist.retransmitted_packets, dropped.len());
    assert_eq!(rist.recovered_payload, payload);
    assert!(
        rist.retransmission_arrival_ms > LOW_LATENCY_PLAYOUT_MS,
        "pure-RIST recovery should miss the low-latency video deadline"
    );
    assert!(
        rist.retransmission_arrival_ms <= RIST_CATCHUP_PLAYOUT_MS,
        "pure-RIST recovery should be usable once playout covers feedback plus RTT"
    );
}

#[tokio::test]
async fn pure_rist_live_udp_feedback_misses_deadline_that_forward_fec_meets() {
    let scenario = VideoScenario {
        name: "keyframe-live-rist-comparison",
        payload_len: 40_000,
        flags: MediaFrameFlags::keyframe(),
        pattern: LossPattern::Burst { start: 3, len: 8 },
        max_wire_overhead: 1.34,
    };
    let payload = video_payload(scenario.payload_len);
    let (_encoded, decoded, dropped) = run_scenario(scenario);
    let rist = run_pure_rist_live_udp_frame_recovery(
        &payload,
        &dropped,
        VIDEO_TEST_RTT_MS,
        RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
    )
    .await
    .expect("live pure-RIST UDP recovery");

    assert_eq!(decoded.payload, payload);
    assert_eq!(rist.dropped_packets, dropped.len());
    assert_eq!(rist.retransmitted_packets, dropped.len());
    assert_eq!(rist.recovered_payload, payload);
    assert!(
        rist.retransmission_arrival_ms > LOW_LATENCY_PLAYOUT_MS,
        "live pure-RIST UDP recovery should miss the low-latency video deadline"
    );
    assert!(
        rist.retransmission_arrival_ms <= RIST_CATCHUP_PLAYOUT_MS,
        "live pure-RIST UDP recovery should be usable once playout covers feedback plus RTT"
    );
}

#[tokio::test]
async fn pure_rist_live_udp_sustained_stream_misses_deadline_for_repeated_video_loss() {
    let frames = stream_scorecard_frames()
        .into_iter()
        .take(30)
        .collect::<Vec<_>>();
    let rist = run_pure_rist_live_udp_stream_recovery(
        &frames,
        VIDEO_TEST_RTT_MS,
        RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
    )
    .await
    .expect("live pure-RIST sustained stream recovery");

    assert_eq!(rist.frame_count, frames.len());
    assert_eq!(
        rist.recovered_frames, rist.frame_count,
        "pure-RIST should eventually recover every frame after retransmission"
    );
    assert!(
        rist.lost_frames >= 8,
        "sustained pure-RIST comparison should exercise repeated video-frame loss"
    );
    assert_eq!(
        rist.feedback_missed_frames, rist.lost_frames,
        "every lost frame should miss the 33 ms low-latency deadline before feedback retransmission arrives"
    );
    assert_eq!(rist.retransmitted_packets, rist.dropped_packets);
    assert!(
        rist.retransmission_arrival_ms > LOW_LATENCY_PLAYOUT_MS,
        "sustained pure-RIST UDP recovery should miss the low-latency video deadline"
    );
    assert!(
        rist.retransmission_arrival_ms <= RIST_CATCHUP_PLAYOUT_MS,
        "sustained pure-RIST UDP recovery should be usable once playout covers feedback plus RTT"
    );
}

#[test]
fn srt_style_best_case_retransmission_misses_deadline_that_forward_fec_meets() {
    let scenario = VideoScenario {
        name: "keyframe-srt-style-comparison",
        payload_len: 40_000,
        flags: MediaFrameFlags::keyframe(),
        pattern: LossPattern::Burst { start: 3, len: 8 },
        max_wire_overhead: 1.34,
    };
    let payload = video_payload(scenario.payload_len);
    let (_encoded, decoded, dropped) = run_scenario(scenario);
    let srt = run_srt_style_best_case_frame_recovery(&payload, &dropped, VIDEO_TEST_RTT_MS);

    assert_eq!(decoded.payload, payload);
    assert_eq!(srt.dropped_packets, dropped.len());
    assert_eq!(srt.retransmitted_packets, dropped.len());
    assert_eq!(srt.recovered_payload, payload);
    assert!(
        srt.retransmission_arrival_ms > LOW_LATENCY_PLAYOUT_MS,
        "even best-case SRT-style ARQ needs at least one RTT and misses the low-latency video deadline"
    );
    assert!(
        srt.retransmission_arrival_ms <= RIST_CATCHUP_PLAYOUT_MS,
        "best-case SRT-style ARQ should be usable once playout covers the RTT"
    );
}

#[test]
fn fec_beats_feedback_only_for_video_stream_under_sub_rtt_latency_budget() {
    let frames = stream_scorecard_frames();
    let simulation = run_stream_simulation(
        &frames,
        VIDEO_TEST_RTT_MS,
        RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
        LOW_LATENCY_PLAYOUT_MS,
    );
    let wire_overhead = simulation.wire_datagrams as f32 / simulation.source_datagrams as f32;

    assert_eq!(simulation.frame_count, 90);
    assert_eq!(
        simulation.fec_recovered_frames, simulation.frame_count,
        "RaptorQ FEC should recover every access unit in this bounded-loss stream"
    );
    assert!(
        simulation.rist_ready_frames < simulation.fec_recovered_frames,
        "pure-RIST feedback retransmission should lose the sub-RTT low-latency frames that FEC repairs"
    );
    assert!(
        simulation.srt_best_case_ready_frames < simulation.fec_recovered_frames,
        "best-case SRT-style ARQ should lose frames once RTT exceeds the low-latency playout budget"
    );
    assert!(
        simulation.lost_frames >= 12,
        "loss schedule should exercise repeated keyframe and delta loss"
    );
    assert!(
        wire_overhead <= 1.30,
        "stream wire overhead {wire_overhead:.3} exceeded low-latency budget"
    );
}

#[test]
fn raptorq_scorecard_is_as_good_or_better_than_feedback_arq_for_low_latency_video() {
    let frames = stream_scorecard_frames();
    let scenarios = [
        (20, "very-low-rtt"),
        (35, "just-over-frame-deadline"),
        (70, "wan-rtt"),
    ];
    let mut strictly_better_than_srt = 0;
    let mut strictly_better_than_rist = 0;

    for (rtt_ms, name) in scenarios {
        let simulation = run_stream_simulation(
            &frames,
            rtt_ms,
            RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
            LOW_LATENCY_PLAYOUT_MS,
        );

        assert_eq!(
            simulation.fec_recovered_frames, simulation.frame_count,
            "{name}: RaptorQ should recover every bounded-loss access unit"
        );
        assert!(
            simulation.fec_recovered_frames >= simulation.srt_best_case_ready_frames,
            "{name}: RaptorQ must not deliver fewer in-deadline frames than the best-case SRT ARQ lower bound"
        );
        assert!(
            simulation.fec_recovered_frames >= simulation.rist_ready_frames,
            "{name}: RaptorQ must not deliver fewer in-deadline frames than pure-RIST feedback"
        );

        strictly_better_than_srt +=
            usize::from(simulation.fec_recovered_frames > simulation.srt_best_case_ready_frames);
        strictly_better_than_rist +=
            usize::from(simulation.fec_recovered_frames > simulation.rist_ready_frames);
    }

    assert!(
        strictly_better_than_srt >= 2,
        "RaptorQ should beat best-case SRT ARQ once RTT exceeds the 33 ms playout budget"
    );
    assert!(
        strictly_better_than_rist >= 3,
        "RaptorQ should beat pure-RIST feedback across this low-latency scorecard"
    );
}

#[test]
fn randomized_video_scorecard_keeps_fec_ahead_of_feedback_arq_under_sub_rtt_playout() {
    let networks = [
        RandomizedVideoNetwork {
            name: "metro-loss",
            rtt_ms: 35,
            feedback_interval_ms: RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
            playout_latency_ms: LOW_LATENCY_PLAYOUT_MS,
            loss_fraction: 0.04,
            burst_fraction: 0.08,
            seed: 0x7A11_D00D_0001,
        },
        RandomizedVideoNetwork {
            name: "wan-loss",
            rtt_ms: 70,
            feedback_interval_ms: RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
            playout_latency_ms: LOW_LATENCY_PLAYOUT_MS,
            loss_fraction: 0.06,
            burst_fraction: 0.10,
            seed: 0x7A11_D00D_0002,
        },
        RandomizedVideoNetwork {
            name: "long-wan-loss",
            rtt_ms: 120,
            feedback_interval_ms: RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
            playout_latency_ms: LOW_LATENCY_PLAYOUT_MS,
            loss_fraction: 0.08,
            burst_fraction: 0.12,
            seed: 0x7A11_D00D_0003,
        },
    ];

    for network in networks {
        assert!(
            network.rtt_ms > network.playout_latency_ms,
            "{} must be a sub-RTT playout scenario for the SRT comparison",
            network.name
        );
        let scorecard = run_randomized_video_scorecard(network, 180);
        let wire_overhead =
            scorecard.wire_datagrams as f32 / scorecard.source_datagrams.max(1) as f32;

        assert_eq!(scorecard.frame_count, 180);
        assert!(
            scorecard.source_loss_frames >= 45,
            "{} should exercise frequent source-packet loss: {:?}",
            network.name,
            scorecard
        );
        assert!(
            scorecard.repair_loss_frames >= scorecard.frame_count / 10,
            "{} should also drop repair datagrams so the test is not source-only: {:?}",
            network.name,
            scorecard
        );
        assert!(
            scorecard.lost_repair_datagrams >= scorecard.frame_count / 10,
            "{} should exercise actual repair-symbol loss, not only repair-loss bookkeeping: {:?}",
            network.name,
            scorecard
        );
        assert_eq!(
            scorecard.fec_failed_no_source_loss_frames, 0,
            "{} FEC must never lose a frame when every source datagram arrives: {:?}",
            network.name, scorecard
        );
        assert!(
            scorecard.fec_recovered_frames >= scorecard.srt_best_case_ready_frames,
            "{} RaptorQ must not deliver fewer in-deadline frames than best-case SRT ARQ: {:?}",
            network.name,
            scorecard
        );
        assert!(
            scorecard.fec_recovered_frames >= scorecard.rist_ready_frames,
            "{} RaptorQ must not deliver fewer in-deadline frames than pure-RIST feedback: {:?}",
            network.name,
            scorecard
        );
        assert!(
            scorecard.fec_recovered_source_loss_frames >= scorecard.source_loss_frames * 3 / 4,
            "{} RaptorQ should recover most frames that feedback ARQ misses under sub-RTT playout: {:?}",
            network.name,
            scorecard
        );
        assert!(
            scorecard.fec_recovered_frames > scorecard.srt_best_case_ready_frames,
            "{} RaptorQ should strictly beat best-case SRT once RTT exceeds the playout budget: {:?}",
            network.name,
            scorecard
        );
        assert!(
            scorecard.fec_recovered_frames > scorecard.rist_ready_frames,
            "{} RaptorQ should strictly beat pure-RIST feedback once RTT exceeds the playout budget: {:?}",
            network.name,
            scorecard
        );
        assert!(
            wire_overhead <= 1.32,
            "{} wire overhead {wire_overhead:.3} exceeded low-latency budget: {:?}",
            network.name,
            scorecard
        );
    }
}

#[test]
fn broad_low_latency_video_impairment_matrix_keeps_fec_ahead_of_feedback_arq() {
    let profiles = [
        BroadVideoImpairmentProfile {
            name: "studio-lan-jitter",
            rtt_ms: 12,
            feedback_interval_ms: RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
            playout_latency_ms: LOW_LATENCY_PLAYOUT_MS,
            frame_count: 120,
            max_source_loss_per_block: 1,
            repair_noise_every: 5,
            reorder_span: 3,
            shape: BoundedLossShape::Alternating {
                seed: 0xB10A_D000_1001,
            },
            max_wire_overhead: 1.30,
        },
        BroadVideoImpairmentProfile {
            name: "metro-sub-rtt-periodic",
            rtt_ms: 45,
            feedback_interval_ms: RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
            playout_latency_ms: LOW_LATENCY_PLAYOUT_MS,
            frame_count: 180,
            max_source_loss_per_block: 1,
            repair_noise_every: 4,
            reorder_span: 4,
            shape: BoundedLossShape::Periodic { every: 3, phase: 1 },
            max_wire_overhead: 1.32,
        },
        BroadVideoImpairmentProfile {
            name: "wan-front-burst",
            rtt_ms: 70,
            feedback_interval_ms: RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
            playout_latency_ms: LOW_LATENCY_PLAYOUT_MS,
            frame_count: 180,
            max_source_loss_per_block: 2,
            repair_noise_every: 3,
            reorder_span: 6,
            shape: BoundedLossShape::Front,
            max_wire_overhead: 1.34,
        },
        BroadVideoImpairmentProfile {
            name: "cellular-late-burst",
            rtt_ms: 95,
            feedback_interval_ms: RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
            playout_latency_ms: 45,
            frame_count: 180,
            max_source_loss_per_block: 2,
            repair_noise_every: 3,
            reorder_span: 7,
            shape: BoundedLossShape::Late,
            max_wire_overhead: 1.34,
        },
        BroadVideoImpairmentProfile {
            name: "long-wan-random-bursty",
            rtt_ms: 140,
            feedback_interval_ms: RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
            playout_latency_ms: 60,
            frame_count: 180,
            max_source_loss_per_block: 2,
            repair_noise_every: 2,
            reorder_span: 8,
            shape: BoundedLossShape::Random {
                seed: 0xB10A_D000_5005,
            },
            max_wire_overhead: 1.34,
        },
    ];

    let mut strict_rist_wins = 0usize;
    let mut strict_srt_wins = 0usize;

    for profile in profiles {
        let scorecard = run_broad_video_impairment_scorecard(profile);
        let wire_overhead =
            scorecard.wire_datagrams as f32 / scorecard.source_datagrams.max(1) as f32;
        let sub_rist_deadline = profile.rtt_ms.saturating_add(profile.feedback_interval_ms)
            > profile.playout_latency_ms;
        let sub_srt_deadline = profile.rtt_ms > profile.playout_latency_ms;

        assert_eq!(scorecard.frame_count, profile.frame_count);
        assert_eq!(
            scorecard.fec_failed_frames, 0,
            "{} FEC should recover every bounded-loss reordered frame: {:?}",
            profile.name, scorecard
        );
        assert!(
            scorecard.source_loss_frames >= profile.frame_count * 3 / 4,
            "{} should exercise source loss on most video frames: {:?}",
            profile.name,
            scorecard
        );
        assert!(
            scorecard.repair_loss_frames >= profile.frame_count / 5,
            "{} should also exercise repair-symbol loss: {:?}",
            profile.name,
            scorecard
        );
        assert!(
            scorecard.reordered_frames >= profile.frame_count / 2,
            "{} should exercise packet reordering, not only loss: {:?}",
            profile.name,
            scorecard
        );
        assert!(
            scorecard.fec_recovered_frames >= scorecard.srt_best_case_ready_frames,
            "{} FEC must not deliver fewer in-deadline frames than best-case SRT ARQ: {:?}",
            profile.name,
            scorecard
        );
        assert!(
            scorecard.fec_recovered_frames >= scorecard.rist_ready_frames,
            "{} FEC must not deliver fewer in-deadline frames than pure RIST feedback: {:?}",
            profile.name,
            scorecard
        );
        if sub_srt_deadline {
            assert!(
                scorecard.fec_recovered_frames > scorecard.srt_best_case_ready_frames,
                "{} FEC should strictly beat best-case SRT under sub-RTT playout: {:?}",
                profile.name,
                scorecard
            );
            strict_srt_wins += 1;
        }
        if sub_rist_deadline {
            assert!(
                scorecard.fec_recovered_frames > scorecard.rist_ready_frames,
                "{} FEC should strictly beat pure RIST under sub-feedback-turn playout: {:?}",
                profile.name,
                scorecard
            );
            strict_rist_wins += 1;
        }
        assert!(
            wire_overhead <= profile.max_wire_overhead,
            "{} wire overhead {wire_overhead:.3} exceeded {}: {:?}",
            profile.name,
            profile.max_wire_overhead,
            scorecard
        );
    }

    assert!(
        strict_srt_wins >= 4,
        "the broad matrix should show strict FEC wins over best-case SRT in every sub-RTT profile"
    );
    assert!(
        strict_rist_wins >= 4,
        "the broad matrix should show strict FEC wins over pure RIST in every sub-feedback profile"
    );
}

#[test]
fn feedback_only_matches_fec_only_when_playout_latency_covers_feedback_turn_and_rtt() {
    let frames = [
        StreamFrameScenario {
            payload_len: 40_000,
            flags: MediaFrameFlags::keyframe(),
            pattern: LossPattern::Burst { start: 3, len: 8 },
        },
        StreamFrameScenario {
            payload_len: 18_000,
            flags: MediaFrameFlags::default(),
            pattern: LossPattern::RandomExact {
                seed: 0xD317_A11D_E1A5_E5ED,
                count: 3,
            },
        },
    ];

    let sub_rtt = run_stream_simulation(
        &frames,
        VIDEO_TEST_RTT_MS,
        RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
        LOW_LATENCY_PLAYOUT_MS,
    );
    let above_rtt = run_stream_simulation(
        &frames,
        VIDEO_TEST_RTT_MS,
        RIST_DEFAULT_FEEDBACK_INTERVAL_MS,
        RIST_CATCHUP_PLAYOUT_MS,
    );

    assert_eq!(sub_rtt.fec_recovered_frames, frames.len());
    assert_eq!(sub_rtt.rist_ready_frames, 0);
    assert_eq!(above_rtt.fec_recovered_frames, frames.len());
    assert_eq!(
        above_rtt.rist_ready_frames,
        frames.len(),
        "feedback-only repair needs a playout buffer covering the feedback turn plus RTT"
    );
}

#[test]
fn video_delta_frames_fail_closed_when_loss_exceeds_parity_budget() {
    let scenario = VideoScenario {
        name: "delta-over-budget-random-loss",
        payload_len: 18_000,
        flags: MediaFrameFlags::default(),
        pattern: LossPattern::RandomExact {
            seed: 0xD317_A11D_E1A5_E5ED,
            count: 4,
        },
        max_wire_overhead: 1.25,
    };
    let payload = video_payload(scenario.payload_len);
    let mut encoder = MediaFecEncoder::new(video_controller());
    let metadata = MediaFrameMetadata {
        duration_ms: 16,
        flags: scenario.flags,
        ..MediaFrameMetadata::new(42, encoder.allocate_sequence(), 1_000, MediaCodec::H264)
    };
    let encoded = encoder
        .encode_frame(MediaFrame {
            metadata,
            payload: &payload,
        })
        .expect("encode video access unit");
    let block_layout = encoded_block_layout(&encoded);
    let dropped = first_block_over_budget_source_indices(&block_layout);

    assert!(
        !block_loss_is_within_repair_budget(&block_layout, &dropped),
        "boundary scenario should exceed a per-block repair budget"
    );
    assert!(
        decode_with_loss(&encoded, &dropped).is_none(),
        "decoder should not synthesize a frame when more symbols are missing than parity can cover"
    );
}

fn feedback_only_arq_can_recover_before_deadline(
    lost_packets: usize,
    rtt_ms: u32,
    feedback_interval_ms: u32,
    frame_deadline_ms: u32,
) -> bool {
    lost_packets == 0 || rtt_ms.saturating_add(feedback_interval_ms) <= frame_deadline_ms
}

fn run_pure_rist_frame_recovery(
    payload: &[u8],
    dropped_indices: &BTreeSet<usize>,
    rtt_ms: u32,
    feedback_interval_ms: u32,
) -> RistFrameRecovery {
    let start = Instant::now();
    let ntp = ntp_from_unix_duration(Duration::from_secs(1));
    let intervals = RtcpIntervals {
        feedback: Duration::from_millis(u64::from(feedback_interval_ms)),
        report: Duration::from_secs(1),
        echo: Duration::from_secs(1),
    };
    let mut sender = SimpleSenderCore::new(0x1122_3344, 1024).with_rtcp_intervals(intervals);
    let mut receiver = SimpleReceiverCore::new(0x5566_7788, "raptor-fec-video", NackMode::Range)
        .with_rtcp_intervals(intervals);
    let packets = payload
        .chunks(usize::from(DEFAULT_SYMBOL_SIZE))
        .map(|chunk| sender.send_payload(chunk, ntp, start))
        .collect::<Vec<_>>();
    let mut received = BTreeMap::new();

    for (index, packet) in packets.iter().enumerate() {
        if dropped_indices.contains(&index) {
            continue;
        }
        let observed = receiver
            .accept_packet(&packet.bytes)
            .expect("pure RIST receiver accepts packet");
        received.insert(observed.sequence, observed.payload);
    }

    let missing = receiver.missing_sequences();
    assert_eq!(
        missing.len(),
        dropped_indices.len(),
        "pure RIST should detect the same dropped source packets in this comparison"
    );
    assert_eq!(
        receiver.poll_rtcp(start, ntp),
        None,
        "first pure RIST RTCP poll arms the feedback scheduler"
    );
    let feedback = receiver
        .poll_rtcp(
            start + Duration::from_millis(u64::from(feedback_interval_ms)),
            ntp,
        )
        .expect("pure RIST receiver emits scheduled NACK feedback");
    let retries = sender
        .handle_feedback_at(&feedback, ntp)
        .expect("pure RIST sender retransmits from NACK feedback");
    assert_eq!(
        retries.len(),
        dropped_indices.len(),
        "pure RIST should retransmit the missing source packets from sender history"
    );

    for retry in &retries {
        let observed = receiver
            .accept_packet(&retry.bytes)
            .expect("pure RIST receiver accepts retransmission");
        assert!(observed.recovered);
        received.insert(observed.sequence, observed.payload);
    }
    assert!(
        receiver.missing_sequences().is_empty(),
        "pure RIST retransmissions should clear receiver missing state"
    );

    let mut recovered_payload = Vec::with_capacity(payload.len());
    for packet in &packets {
        recovered_payload.extend_from_slice(
            received
                .get(&packet.sequence)
                .unwrap_or_else(|| panic!("missing recovered RIST packet {}", packet.sequence)),
        );
    }

    RistFrameRecovery {
        dropped_packets: dropped_indices.len(),
        retransmitted_packets: retries.len(),
        retransmission_arrival_ms: feedback_interval_ms.saturating_add(rtt_ms),
        recovered_payload,
    }
}

fn run_srt_style_best_case_frame_recovery(
    payload: &[u8],
    dropped_indices: &BTreeSet<usize>,
    rtt_ms: u32,
) -> SrtStyleFrameRecovery {
    let mut chunks = payload
        .chunks(SRT_PACKET_PAYLOAD_SIZE)
        .map(Vec::from)
        .collect::<Vec<_>>();
    let original = chunks.clone();
    let mut retransmitted_packets = 0;

    for index in dropped_indices {
        if *index >= chunks.len() {
            continue;
        }
        chunks[*index].clear();
    }
    for index in dropped_indices {
        if *index >= chunks.len() {
            continue;
        }
        chunks[*index] = original[*index].clone();
        retransmitted_packets += 1;
    }

    SrtStyleFrameRecovery {
        dropped_packets: dropped_indices
            .iter()
            .filter(|index| **index < chunks.len())
            .count(),
        retransmitted_packets,
        retransmission_arrival_ms: rtt_ms,
        recovered_payload: chunks.concat(),
    }
}

fn stream_scorecard_frames() -> Vec<StreamFrameScenario> {
    let mut frames = Vec::new();
    for frame_index in 0..90 {
        let is_keyframe = frame_index % 30 == 0;
        let pattern = if is_keyframe {
            LossPattern::Burst { start: 3, len: 8 }
        } else if frame_index % 9 == 0 {
            LossPattern::RandomExact {
                seed: 0x510E_0000 + frame_index as u64,
                count: 3,
            }
        } else if frame_index % 5 == 0 {
            LossPattern::RandomExact {
                seed: 0xD317_0000 + frame_index as u64,
                count: 2,
            }
        } else {
            LossPattern::RandomExact {
                seed: 0xC0DE_0000 + frame_index as u64,
                count: 0,
            }
        };
        frames.push(StreamFrameScenario {
            payload_len: if is_keyframe { 40_000 } else { 18_000 },
            flags: if is_keyframe {
                MediaFrameFlags::keyframe()
            } else {
                MediaFrameFlags::default()
            },
            pattern,
        });
    }
    frames
}

fn run_stream_simulation(
    frames: &[StreamFrameScenario],
    rtt_ms: u32,
    feedback_interval_ms: u32,
    playout_latency_ms: u32,
) -> StreamSimulation {
    let mut encoder = MediaFecEncoder::new(video_controller());
    let mut decoder = MediaFecDecoder::new();
    let mut simulation = StreamSimulation {
        frame_count: frames.len(),
        ..StreamSimulation::default()
    };

    for (frame_index, frame) in frames.iter().enumerate() {
        let payload = video_payload(frame.payload_len);
        let metadata = MediaFrameMetadata {
            duration_ms: 16,
            flags: frame.flags,
            ..MediaFrameMetadata::new(
                42,
                encoder.allocate_sequence(),
                (frame_index as u64) * 16,
                MediaCodec::H264,
            )
        };
        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode video access unit");
        let dropped = dropped_indices(frame.pattern, encoded.datagrams.len());
        let block_layout = encoded_block_layout(&encoded);
        assert_loss_within_repair_budget(
            &format!("stream simulation frame {frame_index}"),
            &block_layout,
            &dropped,
        );
        simulation.source_datagrams += total_source_symbols(&block_layout);
        simulation.wire_datagrams += encoded.datagrams.len();
        if !dropped.is_empty() {
            assert!(
                total_lost_source_symbols(&block_layout, &dropped) > 0,
                "stream simulation frame {frame_index} must drop source symbols"
            );
            simulation.lost_frames += 1;
        }
        if feedback_only_arq_can_recover_before_deadline(
            dropped.len(),
            rtt_ms,
            feedback_interval_ms,
            playout_latency_ms,
        ) {
            simulation.rist_ready_frames += 1;
        }
        if feedback_only_arq_can_recover_before_deadline(
            dropped.len(),
            rtt_ms,
            0,
            playout_latency_ms,
        ) {
            simulation.srt_best_case_ready_frames += 1;
        }

        for (datagram_index, datagram) in encoded.datagrams.iter().enumerate() {
            if dropped.contains(&datagram_index) {
                continue;
            }
            if let Some(decoded) = decoder.push_datagram(datagram).expect("decode datagram") {
                assert_eq!(
                    decoded.payload, payload,
                    "decoded stream payload mismatch at frame {frame_index}"
                );
                simulation.fec_recovered_frames += 1;
            }
        }
    }

    simulation
}

fn run_randomized_video_scorecard(
    network: RandomizedVideoNetwork,
    frame_count: usize,
) -> RandomizedVideoScorecard {
    let mut encoder = MediaFecEncoder::new(video_controller());
    let mut decoder = MediaFecDecoder::new();
    let mut scorecard = RandomizedVideoScorecard {
        frame_count,
        ..RandomizedVideoScorecard::default()
    };
    let mut state = network.seed;

    for frame_index in 0..frame_count {
        let flags = if frame_index % 30 == 0 {
            MediaFrameFlags::keyframe()
        } else {
            MediaFrameFlags::default()
        };
        let payload_len = randomized_scorecard_payload_len(frame_index);
        let payload = video_payload(payload_len);
        let metadata = MediaFrameMetadata {
            duration_ms: 16,
            flags,
            ..MediaFrameMetadata::new(
                77,
                encoder.allocate_sequence(),
                (frame_index as u64) * 16,
                MediaCodec::H264,
            )
        };
        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode randomized video access unit");
        let block_layout = encoded_block_layout(&encoded);
        let dropped = randomized_wire_loss(&block_layout, &mut state, network);
        let lost_source = total_lost_source_symbols(&block_layout, &dropped);
        let lost_repair = total_lost_repair_symbols(&block_layout, &dropped);

        scorecard.source_datagrams += total_source_symbols(&block_layout);
        scorecard.wire_datagrams += encoded.datagrams.len();
        scorecard.lost_source_datagrams += lost_source;
        scorecard.lost_repair_datagrams += lost_repair;

        if lost_source > 0 {
            scorecard.source_loss_frames += 1;
        }
        if lost_repair > 0 {
            scorecard.repair_loss_frames += 1;
        }
        if feedback_only_arq_can_recover_before_deadline(
            lost_source,
            network.rtt_ms,
            network.feedback_interval_ms,
            network.playout_latency_ms,
        ) {
            scorecard.rist_ready_frames += 1;
        }
        if feedback_only_arq_can_recover_before_deadline(
            lost_source,
            network.rtt_ms,
            0,
            network.playout_latency_ms,
        ) {
            scorecard.srt_best_case_ready_frames += 1;
        }

        let mut recovered = false;
        for (datagram_index, datagram) in encoded.datagrams.iter().enumerate() {
            if dropped.contains(&datagram_index) {
                continue;
            }
            if let Some(decoded) = decoder.push_datagram(datagram).expect("decode datagram") {
                assert_eq!(
                    decoded.payload, payload,
                    "{} decoded randomized frame payload mismatch at frame {frame_index}",
                    network.name
                );
                recovered = true;
            }
        }

        if recovered {
            scorecard.fec_recovered_frames += 1;
            if lost_source > 0 {
                scorecard.fec_recovered_source_loss_frames += 1;
            }
        } else {
            scorecard.fec_failed_frames += 1;
            if lost_source == 0 {
                scorecard.fec_failed_no_source_loss_frames += 1;
            }
        }
    }

    scorecard
}

fn run_broad_video_impairment_scorecard(
    profile: BroadVideoImpairmentProfile,
) -> BroadVideoScorecard {
    let mut encoder = MediaFecEncoder::new(video_controller());
    let mut decoder = MediaFecDecoder::new();
    let mut scorecard = BroadVideoScorecard {
        frame_count: profile.frame_count,
        ..BroadVideoScorecard::default()
    };

    for frame_index in 0..profile.frame_count {
        let flags = if frame_index % 30 == 0 {
            MediaFrameFlags::keyframe()
        } else {
            MediaFrameFlags::default()
        };
        let payload_len = randomized_scorecard_payload_len(frame_index);
        let payload = video_payload(payload_len);
        let metadata = MediaFrameMetadata {
            duration_ms: 16,
            flags,
            ..MediaFrameMetadata::new(
                101,
                encoder.allocate_sequence(),
                (frame_index as u64) * 16,
                MediaCodec::H264,
            )
        };
        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode broad video access unit");
        let block_layout = encoded_block_layout(&encoded);
        let dropped = bounded_broad_video_drops(&block_layout, frame_index, profile);
        assert_broad_loss_within_available_repair(profile.name, &block_layout, &dropped);

        let lost_source = total_lost_source_symbols(&block_layout, &dropped);
        let lost_repair = total_lost_repair_symbols(&block_layout, &dropped);
        scorecard.source_datagrams += total_source_symbols(&block_layout);
        scorecard.wire_datagrams += encoded.datagrams.len();
        scorecard.lost_source_datagrams += lost_source;
        scorecard.lost_repair_datagrams += lost_repair;

        if lost_source > 0 {
            scorecard.source_loss_frames += 1;
        }
        if lost_repair > 0 {
            scorecard.repair_loss_frames += 1;
        }
        if feedback_only_arq_can_recover_before_deadline(
            lost_source,
            profile.rtt_ms,
            profile.feedback_interval_ms,
            profile.playout_latency_ms,
        ) {
            scorecard.rist_ready_frames += 1;
        }
        if feedback_only_arq_can_recover_before_deadline(
            lost_source,
            profile.rtt_ms,
            0,
            profile.playout_latency_ms,
        ) {
            scorecard.srt_best_case_ready_frames += 1;
        }

        let delivery_order =
            reordered_delivery_order(encoded.datagrams.len(), profile.reorder_span);
        if delivery_order
            .iter()
            .enumerate()
            .any(|(position, datagram_index)| position != *datagram_index)
        {
            scorecard.reordered_frames += 1;
        }

        let mut recovered = false;
        for datagram_index in delivery_order {
            if dropped.contains(&datagram_index) {
                continue;
            }
            if let Some(decoded) = decoder
                .push_datagram(&encoded.datagrams[datagram_index])
                .expect("decode broad video datagram")
            {
                assert_eq!(
                    decoded.payload, payload,
                    "{} decoded broad video payload mismatch at frame {frame_index}",
                    profile.name
                );
                recovered = true;
            }
        }

        if recovered {
            scorecard.fec_recovered_frames += 1;
        } else {
            scorecard.fec_failed_frames += 1;
        }
    }

    scorecard
}

fn bounded_broad_video_drops(
    blocks: &[EncodedMediaBlock],
    frame_index: usize,
    profile: BroadVideoImpairmentProfile,
) -> BTreeSet<usize> {
    let mut dropped = BTreeSet::new();
    let mut random_state = broad_loss_seed(profile.shape, frame_index);

    for block in blocks {
        let repair_budget = block.repair_symbols as usize;
        if repair_budget == 0 {
            continue;
        }

        let repair_loss = if profile.repair_noise_every > 0
            && repair_budget >= 2
            && (frame_index + block.fragment_index as usize) % profile.repair_noise_every == 0
        {
            1
        } else {
            0
        };
        let source_loss_budget = repair_budget
            .saturating_sub(repair_loss)
            .min(profile.max_source_loss_per_block);

        dropped.extend(select_bounded_source_losses(
            block,
            source_loss_budget,
            profile.shape,
            &mut random_state,
            frame_index,
        ));
        dropped.extend(block.repair_datagram_indices().take(repair_loss));
    }

    dropped
}

fn broad_loss_seed(shape: BoundedLossShape, frame_index: usize) -> u64 {
    match shape {
        BoundedLossShape::Random { seed } | BoundedLossShape::Alternating { seed } => {
            seed ^ (frame_index as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15)
        }
        _ => 0xB10A_D000_0000 ^ frame_index as u64,
    }
}

fn select_bounded_source_losses(
    block: &EncodedMediaBlock,
    count: usize,
    shape: BoundedLossShape,
    random_state: &mut u64,
    frame_index: usize,
) -> BTreeSet<usize> {
    let source_indices = block.source_datagram_indices().collect::<Vec<_>>();
    if source_indices.is_empty() || count == 0 {
        return BTreeSet::new();
    }
    let count = count.min(source_indices.len());

    match shape {
        BoundedLossShape::Front => source_indices.into_iter().take(count).collect(),
        BoundedLossShape::Late => source_indices.into_iter().rev().take(count).collect(),
        BoundedLossShape::Periodic { every, phase } => {
            let every = every.max(1);
            let mut selected = source_indices
                .iter()
                .copied()
                .enumerate()
                .filter(|(index, _)| index % every == phase % every)
                .map(|(_, datagram_index)| datagram_index)
                .take(count)
                .collect::<BTreeSet<_>>();
            if selected.len() < count {
                selected.extend(source_indices.into_iter().take(count - selected.len()));
            }
            selected
        }
        BoundedLossShape::Random { .. } => {
            random_source_losses(&source_indices, count, random_state)
        }
        BoundedLossShape::Alternating { .. } => {
            if frame_index % 3 == 0 {
                source_indices.into_iter().take(count).collect()
            } else if frame_index % 3 == 1 {
                source_indices.into_iter().rev().take(count).collect()
            } else {
                random_source_losses(&source_indices, count, random_state)
            }
        }
    }
}

fn random_source_losses(
    source_indices: &[usize],
    count: usize,
    random_state: &mut u64,
) -> BTreeSet<usize> {
    let mut selected = BTreeSet::new();
    while selected.len() < count {
        *random_state = splitmix64(*random_state);
        let index = (*random_state as usize) % source_indices.len();
        selected.insert(source_indices[index]);
    }
    selected
}

fn reordered_delivery_order(datagram_count: usize, reorder_span: usize) -> Vec<usize> {
    if reorder_span <= 1 {
        return (0..datagram_count).collect();
    }
    let mut order = Vec::with_capacity(datagram_count);
    let span = reorder_span.max(2);
    for chunk_start in (0..datagram_count).step_by(span) {
        let chunk_end = (chunk_start + span).min(datagram_count);
        let mut chunk = (chunk_start..chunk_end).collect::<Vec<_>>();
        chunk.rotate_left(1);
        order.extend(chunk);
    }
    order
}

fn assert_broad_loss_within_available_repair(
    name: &str,
    blocks: &[EncodedMediaBlock],
    dropped: &BTreeSet<usize>,
) {
    for block in blocks {
        let lost_source = lost_source_symbols_for_block(block, dropped);
        let lost_repair = block
            .repair_datagram_indices()
            .filter(|datagram_index| dropped.contains(datagram_index))
            .count();
        assert!(
            lost_source + lost_repair <= block.repair_symbols as usize,
            "{name} broad profile lost {lost_source} source and {lost_repair} repair symbols in block {} with only {} repair symbols; dropped={dropped:?}",
            block.block_id,
            block.repair_symbols
        );
    }
}

fn randomized_scorecard_payload_len(frame_index: usize) -> usize {
    if frame_index % 30 == 0 {
        if frame_index % 60 == 0 {
            96_000
        } else {
            40_000
        }
    } else {
        match frame_index % 11 {
            0 => 24_000,
            3 | 7 => 9_000,
            _ => 18_000,
        }
    }
}

fn randomized_wire_loss(
    blocks: &[EncodedMediaBlock],
    state: &mut u64,
    network: RandomizedVideoNetwork,
) -> BTreeSet<usize> {
    let mut dropped = BTreeSet::new();
    for block in blocks {
        let block_start = block.first_datagram_index;
        let block_end = block.first_datagram_index + block.datagram_count;
        for datagram_index in block_start..block_end {
            if random_unit_interval(state) < network.loss_fraction {
                dropped.insert(datagram_index);
            }
        }

        if block.datagram_count > 1 && random_unit_interval(state) < network.burst_fraction {
            let burst_cap = (block.repair_symbols as usize)
                .saturating_add(1)
                .clamp(1, 5)
                .min(block.datagram_count);
            let burst_len = 1 + random_usize(state, burst_cap);
            let max_start = block.datagram_count - burst_len;
            let start = block_start + random_usize(state, max_start + 1);
            dropped.extend(start..start + burst_len);
        }
    }
    dropped
}

fn run_scenario(
    scenario: VideoScenario,
) -> (EncodedMediaFrame, DecodedMediaFrame, BTreeSet<usize>) {
    let encoded = encode_video_frame(scenario.payload_len, scenario.flags);
    let dropped = dropped_indices(scenario.pattern, encoded.datagrams.len());
    let decoded = decode_with_loss(&encoded, &dropped);

    (
        encoded,
        decoded.unwrap_or_else(|| panic!("{} did not recover", scenario.name)),
        dropped,
    )
}

fn encode_video_frame(payload_len: usize, flags: MediaFrameFlags) -> EncodedMediaFrame {
    let payload = video_payload(payload_len);
    let mut encoder = MediaFecEncoder::new(video_controller());
    let metadata = MediaFrameMetadata {
        duration_ms: 16,
        flags,
        ..MediaFrameMetadata::new(42, encoder.allocate_sequence(), 1_000, MediaCodec::H264)
    };
    encoder
        .encode_frame(MediaFrame {
            metadata,
            payload: &payload,
        })
        .expect("encode video access unit")
}

fn decode_with_loss(
    encoded: &EncodedMediaFrame,
    dropped: &BTreeSet<usize>,
) -> Option<DecodedMediaFrame> {
    let mut decoder = MediaFecDecoder::new();
    for (index, datagram) in encoded.datagrams.iter().enumerate() {
        if dropped.contains(&index) {
            continue;
        }
        if let Some(frame) = decoder.push_datagram(datagram).expect("decode datagram") {
            return Some(frame);
        }
    }
    None
}

fn encoded_block_layout(encoded: &EncodedMediaFrame) -> Vec<EncodedMediaBlock> {
    assert_eq!(
        encoded.blocks.len(),
        usize::from(encoded.fragment_count),
        "media encoder should expose one FEC block per media fragment"
    );
    encoded.blocks.clone()
}

fn block_front_source_indices(
    blocks: &[EncodedMediaBlock],
    max_per_block: usize,
) -> BTreeSet<usize> {
    let mut selected = BTreeSet::new();
    for block in blocks {
        selected.extend(
            block
                .source_datagram_indices()
                .take((block.repair_symbols as usize).min(max_per_block)),
        );
    }
    selected
}

fn block_late_source_indices(
    blocks: &[EncodedMediaBlock],
    max_per_block: usize,
) -> BTreeSet<usize> {
    let mut selected = BTreeSet::new();
    for block in blocks {
        let count = (block.repair_symbols as usize).min(max_per_block);
        selected.extend(block.source_datagram_indices().rev().take(count));
    }
    selected
}

fn block_periodic_source_indices(
    blocks: &[EncodedMediaBlock],
    every: usize,
    phase: usize,
    max_per_block: usize,
) -> BTreeSet<usize> {
    assert!(every > 0);
    let mut selected = BTreeSet::new();
    for block in blocks {
        selected.extend(
            block
                .source_datagram_indices()
                .enumerate()
                .filter(|(index, _)| index % every == phase % every)
                .map(|(_, datagram_index)| datagram_index)
                .take((block.repair_symbols as usize).min(max_per_block)),
        );
    }
    selected
}

fn block_random_source_indices(blocks: &[EncodedMediaBlock], seed: u64) -> BTreeSet<usize> {
    let mut selected = BTreeSet::new();
    let mut state = seed;
    for block in blocks {
        let source_indices = block.source_datagram_indices().collect::<Vec<_>>();
        let target_count = (block.repair_symbols as usize).min(source_indices.len());
        let before_block = selected.len();
        while selected.len() - before_block < target_count {
            state = splitmix64(state);
            let source_index = (state as usize) % source_indices.len();
            selected.insert(source_indices[source_index]);
        }
    }
    selected
}

fn block_loss_is_within_repair_budget(
    blocks: &[EncodedMediaBlock],
    dropped: &BTreeSet<usize>,
) -> bool {
    blocks.iter().all(|block| {
        let lost_source_symbols = lost_source_symbols_for_block(block, dropped);
        lost_source_symbols <= block.repair_symbols as usize
    })
}

fn assert_loss_within_repair_budget(
    name: &str,
    blocks: &[EncodedMediaBlock],
    dropped: &BTreeSet<usize>,
) {
    for block in blocks {
        let lost_source_symbols = lost_source_symbols_for_block(block, dropped);
        assert!(
            lost_source_symbols <= block.repair_symbols as usize,
            "{name} lost {lost_source_symbols} source symbols in block {} but only has {} repair symbols; dropped={dropped:?}",
            block.block_id,
            block.repair_symbols
        );
    }
}

fn total_source_symbols(blocks: &[EncodedMediaBlock]) -> usize {
    blocks
        .iter()
        .map(|block| usize::from(block.source_symbols))
        .sum()
}

fn total_lost_source_symbols(blocks: &[EncodedMediaBlock], dropped: &BTreeSet<usize>) -> usize {
    blocks
        .iter()
        .map(|block| lost_source_symbols_for_block(block, dropped))
        .sum()
}

fn total_lost_repair_symbols(blocks: &[EncodedMediaBlock], dropped: &BTreeSet<usize>) -> usize {
    blocks
        .iter()
        .map(|block| {
            block
                .repair_datagram_indices()
                .filter(|datagram_index| dropped.contains(datagram_index))
                .count()
        })
        .sum()
}

fn max_lost_source_symbols_per_block(
    blocks: &[EncodedMediaBlock],
    dropped: &BTreeSet<usize>,
) -> usize {
    blocks
        .iter()
        .map(|block| lost_source_symbols_for_block(block, dropped))
        .max()
        .unwrap_or(0)
}

fn lost_source_symbols_for_block(block: &EncodedMediaBlock, dropped: &BTreeSet<usize>) -> usize {
    block
        .source_datagram_indices()
        .filter(|datagram_index| dropped.contains(datagram_index))
        .count()
}

fn first_block_over_budget_source_indices(blocks: &[EncodedMediaBlock]) -> BTreeSet<usize> {
    let block = blocks
        .iter()
        .find(|block| usize::from(block.source_symbols) > block.repair_symbols as usize)
        .expect("at least one block should have source datagrams beyond repair budget");
    block
        .source_datagram_indices()
        .take(block.repair_symbols as usize + 1)
        .collect()
}

async fn run_live_media_fec_udp_scenario(
    scenario: VideoScenario,
) -> (
    EncodedMediaFrame,
    DecodedMediaFrame,
    BTreeSet<usize>,
    LossyUdpProxyStats,
    MediaFrameMetadata,
) {
    let payload = video_payload(scenario.payload_len);
    let mut encoder = MediaFecEncoder::new(video_controller());
    let metadata = MediaFrameMetadata {
        duration_ms: 16,
        flags: scenario.flags,
        ..MediaFrameMetadata::new(42, encoder.allocate_sequence(), 1_000, MediaCodec::H264)
    };
    let encoded = encoder
        .encode_frame(MediaFrame {
            metadata,
            payload: &payload,
        })
        .expect("encode video access unit");
    let dropped = dropped_indices(scenario.pattern, encoded.datagrams.len());

    let receiver_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind receiver socket");
    let receiver_addr = receiver_socket
        .local_addr()
        .expect("receiver local address");
    let proxy_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind lossy proxy socket");
    let proxy_addr = proxy_socket.local_addr().expect("proxy local address");
    let sender_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .expect("bind sender socket");

    let proxy = tokio::spawn(forward_datagrams_with_loss(
        proxy_socket,
        receiver_addr,
        encoded.datagrams.len(),
        dropped.clone(),
    ));
    let receiver = tokio::spawn(receive_decoded_media_frame(receiver_socket));

    for datagram in &encoded.datagrams {
        sender_socket
            .send_to(datagram, proxy_addr)
            .await
            .expect("send datagram to lossy proxy");
    }

    let stats = timeout(Duration::from_secs(3), proxy)
        .await
        .unwrap_or_else(|_| panic!("{} lossy proxy timed out", scenario.name))
        .unwrap_or_else(|_| panic!("{} lossy proxy task panicked", scenario.name))
        .unwrap_or_else(|error| panic!("{} lossy proxy failed: {}", scenario.name, error));
    let decoded = timeout(Duration::from_secs(3), receiver)
        .await
        .unwrap_or_else(|_| panic!("{} media receiver timed out", scenario.name))
        .unwrap_or_else(|_| panic!("{} media receiver task panicked", scenario.name))
        .unwrap_or_else(|error| panic!("{} media receiver failed: {}", scenario.name, error));

    (encoded, decoded, dropped, stats, metadata)
}

async fn forward_datagrams_with_loss(
    socket: UdpSocket,
    target: SocketAddr,
    expected_datagrams: usize,
    dropped_indices: BTreeSet<usize>,
) -> std::io::Result<LossyUdpProxyStats> {
    let mut buf = vec![0_u8; 4096];
    let mut stats = LossyUdpProxyStats {
        received: 0,
        forwarded: 0,
        dropped: 0,
        delayed: 0,
    };

    for index in 0..expected_datagrams {
        let (len, _peer) = socket.recv_from(&mut buf).await?;
        stats.received += 1;
        if dropped_indices.contains(&index) {
            stats.dropped += 1;
            continue;
        }

        if index % 5 == 0 {
            stats.delayed += 1;
            sleep(Duration::from_millis(2)).await;
        }
        socket.send_to(&buf[..len], target).await?;
        stats.forwarded += 1;
    }

    Ok(stats)
}

async fn forward_datagrams_with_impairments(
    socket: UdpSocket,
    target: SocketAddr,
    impairments: Vec<DatagramImpairment>,
) -> std::io::Result<StreamUdpProxyStats> {
    let mut buf = vec![0_u8; 4096];
    let mut stats = StreamUdpProxyStats::default();
    let mut scheduled = Vec::new();

    for (ordinal, impairment) in impairments.into_iter().enumerate() {
        let (len, _peer) = socket.recv_from(&mut buf).await?;
        stats.received += 1;
        if impairment.drop {
            stats.dropped += 1;
            continue;
        }
        if impairment.delay_ms > 0 {
            stats.delayed += 1;
        }
        scheduled.push(ScheduledDatagram {
            ordinal,
            delay_ms: impairment.delay_ms,
            bytes: buf[..len].to_vec(),
        });
    }

    scheduled.sort_by_key(|datagram| (datagram.delay_ms, datagram.ordinal));
    let mut current_delay_ms = 0;
    let mut max_original_ordinal = 0;
    for datagram in scheduled {
        if datagram.delay_ms > current_delay_ms {
            sleep(Duration::from_millis(datagram.delay_ms - current_delay_ms)).await;
            current_delay_ms = datagram.delay_ms;
        }
        if datagram.ordinal < max_original_ordinal {
            stats.reordered += 1;
        } else {
            max_original_ordinal = datagram.ordinal;
        }
        socket.send_to(&datagram.bytes, target).await?;
        stats.forwarded += 1;
    }

    Ok(stats)
}

async fn receive_decoded_media_frame(socket: UdpSocket) -> Result<DecodedMediaFrame, String> {
    let mut buf = vec![0_u8; 4096];
    let mut decoder = MediaFecDecoder::new();

    loop {
        let len = socket
            .recv(&mut buf)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(frame) = decoder
            .push_datagram(&buf[..len])
            .map_err(|error| error.to_string())?
        {
            return Ok(frame);
        }
    }
}

async fn receive_decoded_media_frames(
    socket: UdpSocket,
    expected_frames: usize,
) -> Result<Vec<DecodedMediaFrame>, String> {
    let mut buf = vec![0_u8; 4096];
    let mut decoder = MediaFecDecoder::new();
    let mut frames = Vec::with_capacity(expected_frames);

    while frames.len() < expected_frames {
        let len = socket
            .recv(&mut buf)
            .await
            .map_err(|error| error.to_string())?;
        if let Some(frame) = decoder
            .push_datagram(&buf[..len])
            .map_err(|error| error.to_string())?
        {
            frames.push(frame);
        }
    }

    Ok(frames)
}

fn stream_reorder_delay_ms(ordinal: usize) -> u64 {
    match ordinal % 13 {
        0 => 9,
        4 => 7,
        8 => 4,
        _ if ordinal % 5 == 0 => 2,
        _ => 0,
    }
}

async fn run_pure_rist_live_udp_frame_recovery(
    payload: &[u8],
    dropped_indices: &BTreeSet<usize>,
    rtt_ms: u32,
    feedback_interval_ms: u32,
) -> Result<RistFrameRecovery, String> {
    let start = Instant::now();
    let ntp = ntp_from_unix_duration(Duration::from_secs(1));
    let intervals = RtcpIntervals {
        feedback: Duration::from_millis(u64::from(feedback_interval_ms)),
        report: Duration::from_secs(1),
        echo: Duration::from_secs(1),
    };
    let mut sender = SimpleSenderCore::new(0x1122_3344, 1024).with_rtcp_intervals(intervals);
    let mut receiver =
        SimpleReceiverCore::new(0x5566_7788, "raptor-fec-live-rist", NackMode::Range)
            .with_rtcp_intervals(intervals);
    let packets = payload
        .chunks(usize::from(DEFAULT_SYMBOL_SIZE))
        .map(|chunk| sender.send_payload(chunk, ntp, start))
        .collect::<Vec<_>>();
    let dropped_source_indices = dropped_indices
        .iter()
        .copied()
        .filter(|index| *index < packets.len())
        .collect::<BTreeSet<_>>();
    let expected_missing_sequences = dropped_source_indices
        .iter()
        .map(|index| packets[*index].sequence)
        .collect::<Vec<_>>();
    let sender_data_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let receiver_data_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let receiver_data_addr = receiver_data_socket
        .local_addr()
        .map_err(|error| error.to_string())?;
    let sender_feedback_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let sender_feedback_addr = sender_feedback_socket
        .local_addr()
        .map_err(|error| error.to_string())?;
    let receiver_feedback_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let mut received = BTreeMap::new();

    if dropped_source_indices.is_empty() {
        return Err("live pure-RIST test must drop at least one source packet".to_string());
    }

    for (index, packet) in packets.iter().enumerate() {
        if dropped_source_indices.contains(&index) {
            continue;
        }
        sender_data_socket
            .send_to(&packet.bytes, receiver_data_addr)
            .await
            .map_err(|error| error.to_string())?;
    }

    let mut buf = vec![0_u8; 4096];
    for _ in 0..packets.len().saturating_sub(dropped_source_indices.len()) {
        let (len, _peer) = timeout(
            Duration::from_secs(1),
            receiver_data_socket.recv_from(&mut buf),
        )
        .await
        .map_err(|_| "timed out receiving initial pure-RIST UDP packet".to_string())?
        .map_err(|error| error.to_string())?;
        let observed = receiver
            .accept_packet(&buf[..len])
            .map_err(|error| error.to_string())?;
        received.insert(observed.sequence, observed.payload);
    }

    let missing = receiver.missing_sequences();
    if missing != expected_missing_sequences {
        return Err(format!(
            "live pure-RIST missing mismatch: expected {:?}, observed {:?}",
            expected_missing_sequences, missing
        ));
    }
    if receiver.poll_rtcp(start, ntp).is_some() {
        return Err("first pure-RIST live feedback poll should only arm scheduler".to_string());
    }

    sleep(Duration::from_millis(u64::from(feedback_interval_ms))).await;
    let feedback = receiver
        .poll_rtcp(
            start + Duration::from_millis(u64::from(feedback_interval_ms)),
            ntp,
        )
        .ok_or_else(|| "pure-RIST live receiver did not emit NACK feedback".to_string())?;

    sleep(Duration::from_millis(u64::from(rtt_ms / 2))).await;
    receiver_feedback_socket
        .send_to(&feedback, sender_feedback_addr)
        .await
        .map_err(|error| error.to_string())?;

    let (feedback_len, _peer) = timeout(
        Duration::from_secs(1),
        sender_feedback_socket.recv_from(&mut buf),
    )
    .await
    .map_err(|_| "timed out receiving pure-RIST live feedback".to_string())?
    .map_err(|error| error.to_string())?;
    let retries = sender
        .handle_feedback_at(&buf[..feedback_len], ntp)
        .map_err(|error| error.to_string())?;
    if retries.len() != dropped_source_indices.len() {
        return Err(format!(
            "live pure-RIST retry count mismatch: expected {}, observed {}",
            dropped_source_indices.len(),
            retries.len()
        ));
    }

    sleep(Duration::from_millis(u64::from(rtt_ms - (rtt_ms / 2)))).await;
    for retry in &retries {
        sender_data_socket
            .send_to(&retry.bytes, receiver_data_addr)
            .await
            .map_err(|error| error.to_string())?;
    }
    for _ in 0..retries.len() {
        let (len, _peer) = timeout(
            Duration::from_secs(1),
            receiver_data_socket.recv_from(&mut buf),
        )
        .await
        .map_err(|_| "timed out receiving pure-RIST live retransmission".to_string())?
        .map_err(|error| error.to_string())?;
        let observed = receiver
            .accept_packet(&buf[..len])
            .map_err(|error| error.to_string())?;
        if !observed.recovered {
            return Err(format!(
                "live pure-RIST retransmission {} was not marked recovered",
                observed.sequence
            ));
        }
        received.insert(observed.sequence, observed.payload);
    }
    if !receiver.missing_sequences().is_empty() {
        return Err(format!(
            "live pure-RIST receiver still missing {:?}",
            receiver.missing_sequences()
        ));
    }

    let mut recovered_payload = Vec::with_capacity(payload.len());
    for packet in &packets {
        recovered_payload.extend_from_slice(
            received
                .get(&packet.sequence)
                .ok_or_else(|| format!("missing live pure-RIST packet {}", packet.sequence))?,
        );
    }

    Ok(RistFrameRecovery {
        dropped_packets: dropped_source_indices.len(),
        retransmitted_packets: retries.len(),
        retransmission_arrival_ms: feedback_interval_ms.saturating_add(rtt_ms),
        recovered_payload,
    })
}

async fn run_pure_rist_live_udp_stream_recovery(
    frames: &[StreamFrameScenario],
    rtt_ms: u32,
    feedback_interval_ms: u32,
) -> Result<RistStreamRecovery, String> {
    let start = Instant::now();
    let ntp = ntp_from_unix_duration(Duration::from_secs(1));
    let intervals = RtcpIntervals {
        feedback: Duration::from_millis(u64::from(feedback_interval_ms)),
        report: Duration::from_secs(1),
        echo: Duration::from_secs(1),
    };
    let mut sender = SimpleSenderCore::new(0x1122_3344, 4096).with_rtcp_intervals(intervals);
    let mut receiver =
        SimpleReceiverCore::new(0x5566_7788, "raptor-fec-live-rist-stream", NackMode::Range)
            .with_rtcp_intervals(intervals);
    let sender_data_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let receiver_data_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let receiver_data_addr = receiver_data_socket
        .local_addr()
        .map_err(|error| error.to_string())?;
    let sender_feedback_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let sender_feedback_addr = sender_feedback_socket
        .local_addr()
        .map_err(|error| error.to_string())?;
    let receiver_feedback_socket = UdpSocket::bind("127.0.0.1:0")
        .await
        .map_err(|error| error.to_string())?;
    let mut packets = Vec::new();
    let mut dropped_sequences = BTreeSet::new();
    let mut frame_sequences = BTreeMap::<usize, Vec<u32>>::new();
    let mut expected_payloads = BTreeMap::<usize, Vec<u8>>::new();
    let mut lost_frames = 0usize;
    let mut feedback_missed_frames = 0usize;

    for (frame_index, frame) in frames.iter().enumerate() {
        let payload = video_payload(frame.payload_len);
        let chunks = payload
            .chunks(usize::from(DEFAULT_SYMBOL_SIZE))
            .map(Vec::from)
            .collect::<Vec<_>>();
        let dropped = dropped_indices(frame.pattern, chunks.len());

        if !dropped.is_empty() {
            lost_frames += 1;
            if !feedback_only_arq_can_recover_before_deadline(
                dropped.len(),
                rtt_ms,
                feedback_interval_ms,
                LOW_LATENCY_PLAYOUT_MS,
            ) {
                feedback_missed_frames += 1;
            }
        }

        for (chunk_index, chunk) in chunks.iter().enumerate() {
            let packet = sender.send_payload(chunk, ntp, start);
            frame_sequences
                .entry(frame_index)
                .or_default()
                .push(packet.sequence);
            if dropped.contains(&chunk_index) {
                dropped_sequences.insert(packet.sequence);
            }
            packets.push(packet);
        }
        expected_payloads.insert(frame_index, payload);
    }

    if dropped_sequences.is_empty() {
        return Err("live pure-RIST sustained stream test must drop packets".to_string());
    }

    for packet in &packets {
        if dropped_sequences.contains(&packet.sequence) {
            continue;
        }
        sender_data_socket
            .send_to(&packet.bytes, receiver_data_addr)
            .await
            .map_err(|error| error.to_string())?;
    }

    let mut received = BTreeMap::new();
    let mut buf = vec![0_u8; 4096];
    let initial_packet_count = packets.len().saturating_sub(dropped_sequences.len());
    for _ in 0..initial_packet_count {
        let (len, _peer) = timeout(
            Duration::from_secs(2),
            receiver_data_socket.recv_from(&mut buf),
        )
        .await
        .map_err(|_| "timed out receiving initial pure-RIST stream packet".to_string())?
        .map_err(|error| error.to_string())?;
        let observed = receiver
            .accept_packet(&buf[..len])
            .map_err(|error| error.to_string())?;
        received.insert(observed.sequence, observed.payload);
    }

    let expected_missing = dropped_sequences.iter().copied().collect::<Vec<_>>();
    let missing = receiver.missing_sequences();
    if missing != expected_missing {
        return Err(format!(
            "live pure-RIST stream missing mismatch: expected {:?}, observed {:?}",
            expected_missing, missing
        ));
    }
    if receiver.poll_rtcp(start, ntp).is_some() {
        return Err("first pure-RIST stream feedback poll should only arm scheduler".to_string());
    }

    sleep(Duration::from_millis(u64::from(feedback_interval_ms))).await;
    let feedback = receiver
        .poll_rtcp(
            start + Duration::from_millis(u64::from(feedback_interval_ms)),
            ntp,
        )
        .ok_or_else(|| "pure-RIST stream receiver did not emit NACK feedback".to_string())?;

    sleep(Duration::from_millis(u64::from(rtt_ms / 2))).await;
    receiver_feedback_socket
        .send_to(&feedback, sender_feedback_addr)
        .await
        .map_err(|error| error.to_string())?;
    let (feedback_len, _peer) = timeout(
        Duration::from_secs(1),
        sender_feedback_socket.recv_from(&mut buf),
    )
    .await
    .map_err(|_| "timed out receiving pure-RIST stream feedback".to_string())?
    .map_err(|error| error.to_string())?;
    let retries = sender
        .handle_feedback_at(&buf[..feedback_len], ntp)
        .map_err(|error| error.to_string())?;
    if retries.len() != dropped_sequences.len() {
        return Err(format!(
            "live pure-RIST stream retry count mismatch: expected {}, observed {}",
            dropped_sequences.len(),
            retries.len()
        ));
    }

    sleep(Duration::from_millis(u64::from(rtt_ms - (rtt_ms / 2)))).await;
    for retry in &retries {
        sender_data_socket
            .send_to(&retry.bytes, receiver_data_addr)
            .await
            .map_err(|error| error.to_string())?;
    }
    for _ in 0..retries.len() {
        let (len, _peer) = timeout(
            Duration::from_secs(1),
            receiver_data_socket.recv_from(&mut buf),
        )
        .await
        .map_err(|_| "timed out receiving pure-RIST stream retransmission".to_string())?
        .map_err(|error| error.to_string())?;
        let observed = receiver
            .accept_packet(&buf[..len])
            .map_err(|error| error.to_string())?;
        if !observed.recovered {
            return Err(format!(
                "live pure-RIST stream retransmission {} was not marked recovered",
                observed.sequence
            ));
        }
        received.insert(observed.sequence, observed.payload);
    }
    if !receiver.missing_sequences().is_empty() {
        return Err(format!(
            "live pure-RIST stream receiver still missing {:?}",
            receiver.missing_sequences()
        ));
    }

    let mut recovered_frames = 0usize;
    for (frame_index, sequences) in frame_sequences {
        let mut recovered_payload = Vec::new();
        for sequence in sequences {
            recovered_payload.extend_from_slice(
                received
                    .get(&sequence)
                    .ok_or_else(|| format!("missing live pure-RIST stream packet {sequence}"))?,
            );
        }
        let expected = expected_payloads
            .get(&frame_index)
            .ok_or_else(|| format!("missing expected frame {frame_index}"))?;
        if &recovered_payload != expected {
            return Err(format!(
                "live pure-RIST stream frame {frame_index} payload mismatch"
            ));
        }
        recovered_frames += 1;
    }

    Ok(RistStreamRecovery {
        frame_count: frames.len(),
        recovered_frames,
        lost_frames,
        feedback_missed_frames,
        dropped_packets: dropped_sequences.len(),
        retransmitted_packets: retries.len(),
        retransmission_arrival_ms: feedback_interval_ms.saturating_add(rtt_ms),
    })
}

fn video_controller() -> AdaptiveFecController {
    let policy = AdaptiveFecPolicy {
        min_source_symbols: 4,
        max_source_symbols: 64,
        min_repair_symbols: 0,
        max_repair_symbols: 20,
        min_repair_ratio: 0.04,
        max_repair_ratio: 0.33,
        keyframe_repair_boost: 0.10,
        audio_repair_boost: 0.08,
        symbol_size: DEFAULT_SYMBOL_SIZE,
    };
    let mut controller = AdaptiveFecController::new(policy, CongestionConfig::default());
    controller.update_network_metrics(NetworkMetrics {
        loss_fraction: 0.08,
        jitter_ms: 25.0,
        queue_delay_ms: 20.0,
        rtt_ms: 70.0,
        available_bitrate_bps: Some(8_000_000),
    });
    controller
}

fn dropped_indices(pattern: LossPattern, datagram_count: usize) -> BTreeSet<usize> {
    match pattern {
        LossPattern::Burst { start, len } => (start..start.saturating_add(len))
            .filter(|index| *index < datagram_count)
            .collect(),
        LossPattern::Periodic { every, phase } => {
            assert!(every > 0);
            (0..datagram_count)
                .filter(|index| index % every == phase % every)
                .collect()
        }
        LossPattern::RandomExact { seed, count } => {
            let mut selected = BTreeSet::new();
            let mut state = seed;
            while selected.len() < count.min(datagram_count) {
                state = splitmix64(state);
                selected.insert((state as usize) % datagram_count);
            }
            selected
        }
    }
}

fn video_payload(len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            let mixed = (index as u32)
                .wrapping_mul(1_103_515_245)
                .wrapping_add(12_345);
            (mixed >> 16) as u8
        })
        .collect()
}

fn splitmix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut mixed = value;
    mixed = (mixed ^ (mixed >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    mixed = (mixed ^ (mixed >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    mixed ^ (mixed >> 31)
}

fn random_unit_interval(state: &mut u64) -> f32 {
    *state = splitmix64(*state);
    ((*state >> 11) as f64 / ((1_u64 << 53) as f64)) as f32
}

fn random_usize(state: &mut u64, max_exclusive: usize) -> usize {
    if max_exclusive <= 1 {
        return 0;
    }
    *state = splitmix64(*state);
    (*state as usize) % max_exclusive
}
