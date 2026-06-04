use raptorq_datagram_fec::{
    AdaptiveFecController, AdaptiveFecPolicy, CongestionConfig, EncodedMediaFrame, MediaCodec,
    MediaFecDecoder, MediaFecEncoder, MediaFrame, MediaFrameMetadata, NetworkMetrics,
    DEFAULT_SYMBOL_SIZE,
};
use std::collections::BTreeSet;

const AUDIO_TEST_RTT_MS: u32 = 70;
const AUDIO_PLAYOUT_MS: u32 = 20;

#[test]
fn opus_audio_frames_recover_single_packet_loss_before_feedback_can_help() {
    assert_audio_codec_recovers_single_packet_loss(MediaCodec::Opus);
}

#[test]
fn aac_audio_frames_get_audio_priority_and_recover_single_packet_loss() {
    assert_audio_codec_recovers_single_packet_loss(MediaCodec::Aac);
}

fn assert_audio_codec_recovers_single_packet_loss(codec: MediaCodec) {
    let mut encoder = MediaFecEncoder::new(audio_controller());
    let mut decoder = MediaFecDecoder::new();
    let payload_lengths = [72usize, 96, 140, 220];
    let mut recovered_frames = 0usize;
    let mut recovered_loss_frames = 0usize;
    let mut lost_frames = 0usize;
    let mut source_datagrams = 0usize;
    let mut wire_datagrams = 0usize;

    for sequence in 0..80u64 {
        let payload_len = payload_lengths[sequence as usize % payload_lengths.len()];
        let payload = audio_payload(sequence, payload_len);
        let mut metadata = MediaFrameMetadata::new(7, sequence, sequence * 20, codec);
        metadata.duration_ms = AUDIO_PLAYOUT_MS;

        let encoded = encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode audio frame");
        assert_single_packet_audio_layout(codec, &encoded);

        let drop_source = sequence % 5 == 0;
        let mut dropped = BTreeSet::new();
        if drop_source {
            lost_frames += 1;
            dropped.insert(encoded.blocks[0].source_datagram_indices().start);
            assert!(
                !feedback_only_arq_can_recover_before_deadline(
                    1,
                    AUDIO_TEST_RTT_MS,
                    0,
                    AUDIO_PLAYOUT_MS,
                ),
                "feedback repair cannot recover a lost audio packet inside a 20 ms playout budget over 70 ms RTT"
            );
        }

        source_datagrams += usize::from(encoded.blocks[0].source_symbols);
        wire_datagrams += encoded.datagrams.len();
        let decoded =
            decode_with_drops(&mut decoder, &encoded, &dropped).expect("audio frame recovered");

        assert_eq!(decoded.metadata.codec, codec);
        assert_eq!(decoded.metadata.sequence, sequence);
        assert_eq!(decoded.metadata.pts_ms, sequence * 20);
        assert_eq!(decoded.metadata.duration_ms, AUDIO_PLAYOUT_MS);
        assert_eq!(decoded.payload, payload);
        recovered_frames += 1;
        recovered_loss_frames += usize::from(drop_source);
    }

    assert_eq!(recovered_frames, 80);
    assert_eq!(recovered_loss_frames, lost_frames);
    assert!(
        lost_frames >= 12,
        "test must exercise repeated audio packet loss"
    );
    assert_eq!(
        source_datagrams, 80,
        "Opus/AAC packets in this matrix should each fit in one source symbol"
    );
    assert!(
        wire_datagrams <= source_datagrams * 2,
        "audio FEC should stay at one source plus one repair datagram per packet"
    );
}

fn assert_single_packet_audio_layout(codec: MediaCodec, encoded: &EncodedMediaFrame) {
    assert_eq!(encoded.fragment_count, 1);
    assert_eq!(encoded.blocks.len(), 1);
    let block = &encoded.blocks[0];
    assert_eq!(
        block.source_symbols, 1,
        "{codec:?} packet should fit in one source symbol"
    );
    assert_eq!(
        block.repair_symbols, 1,
        "{codec:?} should receive one forward repair symbol under the lossy audio profile"
    );
    assert_eq!(
        block.datagram_count,
        usize::from(block.source_symbols) + block.repair_symbols as usize
    );
}

fn decode_with_drops(
    decoder: &mut MediaFecDecoder,
    encoded: &EncodedMediaFrame,
    dropped: &BTreeSet<usize>,
) -> Option<raptorq_datagram_fec::DecodedMediaFrame> {
    let mut decoded = None;
    for (index, datagram) in encoded.datagrams.iter().enumerate() {
        if dropped.contains(&index) {
            continue;
        }
        if let Some(frame) = decoder.push_datagram(datagram).expect("decode datagram") {
            decoded = Some(frame);
        }
    }
    decoded
}

fn audio_controller() -> AdaptiveFecController {
    let policy = AdaptiveFecPolicy {
        min_source_symbols: 1,
        max_source_symbols: 8,
        min_repair_symbols: 0,
        max_repair_symbols: 2,
        delta_repair_floor_source_symbols: 8,
        delta_repair_floor_symbols: 1,
        min_repair_ratio: 0.04,
        max_repair_ratio: 0.5,
        keyframe_repair_boost: 0.10,
        audio_repair_boost: 0.08,
        symbol_size: DEFAULT_SYMBOL_SIZE,
    };
    let mut controller = AdaptiveFecController::new(policy, CongestionConfig::default());
    controller.update_network_metrics(NetworkMetrics {
        loss_fraction: 0.08,
        jitter_ms: 15.0,
        queue_delay_ms: 5.0,
        rtt_ms: AUDIO_TEST_RTT_MS as f32,
        available_bitrate_bps: Some(512_000),
    });
    controller
}

fn feedback_only_arq_can_recover_before_deadline(
    lost_packets: usize,
    rtt_ms: u32,
    feedback_interval_ms: u32,
    frame_deadline_ms: u32,
) -> bool {
    lost_packets == 0 || rtt_ms.saturating_add(feedback_interval_ms) <= frame_deadline_ms
}

fn audio_payload(sequence: u64, len: usize) -> Vec<u8> {
    (0..len)
        .map(|index| {
            let mixed = (index as u64)
                .wrapping_mul(0x9E37_79B1)
                .wrapping_add(sequence.rotate_left(17));
            (mixed >> 11) as u8
        })
        .collect()
}
