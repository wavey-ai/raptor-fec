use raptorq_datagram_fec::{
    AdaptiveFecController, DatagramFecHeader, MediaCodec, MediaFecDecoder, MediaFecEncoder,
    MediaFrame, MediaFrameMetadata, MediaPriority, NetworkMetrics, DEFAULT_SYMBOL_SIZE,
    ENCODING_PACKET_HEADER_LEN, HEADER_LEN, MEDIA_FRAME_HEADER_LEN,
};

const FRAMES_PER_SECOND: usize = 200;
const OPUS_96_KBPS_5_MS_BYTES: usize = 60;
const CHACHA_NONCE_AND_TAG_BYTES: usize = 28;
// FrameHeaderV2 base (8) + u64 id (8) + PTS (8) + packet CRC (4).
const REPRESENTATIVE_SOUNDKIT_HEADER_BYTES: usize = 28;
const REPRESENTATIVE_ENCRYPTED_AUDIO_PACKET_BYTES: usize =
    OPUS_96_KBPS_5_MS_BYTES + CHACHA_NONCE_AND_TAG_BYTES + REPRESENTATIVE_SOUNDKIT_HEADER_BYTES;

#[test]
fn clean_link_96_kbps_five_ms_audio_stays_within_wire_budget() {
    let mut encoder = MediaFecEncoder::default();
    let encoded = encode_audio(&mut encoder, REPRESENTATIVE_ENCRYPTED_AUDIO_PACKET_BYTES);
    let stats = encoded.stats();
    let protected_bytes = MEDIA_FRAME_HEADER_LEN + REPRESENTATIVE_ENCRYPTED_AUDIO_PACKET_BYTES;
    let expected_datagram_bytes = HEADER_LEN + ENCODING_PACKET_HEADER_LEN + protected_bytes;

    assert_eq!(encoded.priority, MediaPriority::Audio);
    assert_eq!(encoded.fragment_count, 1);
    assert_eq!(stats.source_datagrams, 1);
    assert_eq!(stats.repair_datagrams, 0);
    assert_eq!(stats.wire_datagrams, 1);
    assert_eq!(
        encoded.decision.config.symbol_size as usize,
        protected_bytes
    );
    assert_eq!(stats.wire_bytes, expected_datagram_bytes);
    assert!(
        stats.wire_bytes * FRAMES_PER_SECOND <= 48_000,
        "clean-link audio must stay at or below 384 kbps including media/FEC headers"
    );
    assert_eq!(
        stats.wire_datagrams * FRAMES_PER_SECOND,
        FRAMES_PER_SECOND,
        "one 5 ms source frame should produce one clean-link datagram"
    );
}

#[test]
fn observed_loss_adds_small_repair_and_recovers_the_audio_frame() {
    let mut controller = AdaptiveFecController::default();
    controller.update_network_metrics(NetworkMetrics {
        loss_fraction: 0.03,
        ..NetworkMetrics::default()
    });
    let mut encoder = MediaFecEncoder::new(controller);
    let encoded = encode_audio(&mut encoder, REPRESENTATIVE_ENCRYPTED_AUDIO_PACKET_BYTES);
    let stats = encoded.stats();

    assert_eq!(stats.source_datagrams, 1);
    assert_eq!(stats.repair_datagrams, 2);
    assert_eq!(stats.wire_datagrams, 3);
    assert!(
        stats.wire_bytes * FRAMES_PER_SECOND <= 144_000,
        "loss-triggered audio tail protection must stay at or below 1.152 Mbps including headers"
    );

    let source_index = encoded.blocks[0].source_datagram_indices().start;
    let mut decoder = MediaFecDecoder::new();
    let mut decoded = None;
    for (index, datagram) in encoded.datagrams.iter().enumerate() {
        if index != source_index {
            let recovered = decoder
                .push_datagram(datagram)
                .expect("decode repair datagram");
            if recovered.is_some() {
                decoded = recovered;
            }
        }
    }
    let decoded = decoded.expect("repair symbol should recover the audio frame");
    assert_eq!(
        decoded.payload.len(),
        REPRESENTATIVE_ENCRYPTED_AUDIO_PACKET_BYTES
    );
}

#[test]
fn default_video_still_uses_mtu_sized_symbols() {
    let mut encoder = MediaFecEncoder::default();
    let payload = vec![0x56; 4_000];
    let metadata = MediaFrameMetadata::new(9, encoder.allocate_sequence(), 1_000, MediaCodec::H264);
    let encoded = encoder
        .encode_frame(MediaFrame {
            metadata,
            payload: &payload,
        })
        .expect("encode video frame");

    assert_eq!(encoded.decision.config.symbol_size, DEFAULT_SYMBOL_SIZE);
    assert!(encoded.blocks[0].source_symbols > 1);
    for datagram in &encoded.datagrams {
        let header = DatagramFecHeader::decode(datagram).expect("decode FEC header");
        assert_eq!(header.symbol_size, DEFAULT_SYMBOL_SIZE);
        assert_eq!(
            datagram.len(),
            HEADER_LEN + ENCODING_PACKET_HEADER_LEN + usize::from(DEFAULT_SYMBOL_SIZE)
        );
    }
}

fn encode_audio(
    encoder: &mut MediaFecEncoder,
    payload_len: usize,
) -> raptorq_datagram_fec::EncodedMediaFrame {
    let payload = vec![0xA5; payload_len];
    let metadata = MediaFrameMetadata {
        duration_ms: 5,
        ..MediaFrameMetadata::new(7, encoder.allocate_sequence(), 1_000, MediaCodec::Opus)
    };
    encoder
        .encode_frame(MediaFrame {
            metadata,
            payload: &payload,
        })
        .expect("encode audio frame")
}
