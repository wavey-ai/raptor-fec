use raptorq::EncodingPacket;
use raptorq_datagram_fec::{
    AudioPayloadKind, AudioSampleFormat, DatagramFecHeader, DecodedMultichannelAudioShard,
    MultichannelAudioDatagram, MultichannelAudioDatagramRole, MultichannelAudioEpoch,
    MultichannelAudioFecConfig, MultichannelAudioFecDecoder, MultichannelAudioFecEncoder,
    MultichannelAudioGroup, MultichannelAudioRecovery, MultichannelAudioShardHeader,
};
use std::collections::{BTreeMap, BTreeSet};

const MAX_DATAGRAM_SIZE: usize = 1_200;
const OUTER_TRANSPORT_OVERHEAD: usize = 12;

#[test]
fn opus_epoch_preserves_caller_group_order_and_empty_erasure_marker() {
    let first_packet = vec![0x41; 75];
    let third_packet = vec![0x63; 103];
    let groups = [
        opus_group(42, 6, 2, &first_packet),
        opus_group(7, 0, 2, &[]),
        opus_group(19, 2, 2, &third_packet),
    ];
    let mut encoder = encoder(3);
    let encoded = encoder
        .encode_epoch(opus_epoch(91, 14_400, &groups))
        .expect("encode Opus epoch");

    assert_eq!(encoded.source_symbols, groups.len() as u16);
    assert_eq!(encoded.source_datagram_count(), groups.len());
    assert_eq!(encoded.repair_datagram_count(), 3);
    assert!(encoded.datagrams.iter().all(|datagram| {
        datagram.payload.len() + OUTER_TRANSPORT_OVERHEAD <= MAX_DATAGRAM_SIZE
    }));

    for (source_index, datagram) in encoded
        .datagrams
        .iter()
        .take(encoded.source_datagram_count())
        .enumerate()
    {
        assert_eq!(
            datagram.role,
            MultichannelAudioDatagramRole::Source {
                source_index: source_index as u16,
            }
        );
    }
    assert!(encoded
        .datagrams
        .iter()
        .skip(encoded.source_datagram_count())
        .all(|datagram| matches!(datagram.role, MultichannelAudioDatagramRole::Repair { .. })));

    let headers = encoded
        .datagrams
        .iter()
        .take(encoded.source_datagram_count())
        .map(source_shard_header)
        .collect::<Vec<_>>();
    assert_eq!(
        headers
            .iter()
            .map(|header| header.group_id)
            .collect::<Vec<_>>(),
        vec![42, 7, 19],
        "the generic transport must preserve the caller's canonical track order"
    );
    assert_eq!(
        headers
            .iter()
            .map(|header| header.group_index)
            .collect::<Vec<_>>(),
        vec![0, 1, 2]
    );
    assert!(headers.iter().all(|header| {
        header.epoch_id == 91
            && header.pts_samples == 14_400
            && header.payload_kind == AudioPayloadKind::Opus
            && header.sample_format == AudioSampleFormat::Unspecified
    }));

    let erasure = headers[1];
    assert_eq!(erasure.group_payload_len, 0);
    assert_eq!(erasure.fragment_count, 1);
    assert_eq!(erasure.fragment_index, 0);
    assert_eq!(erasure.payload_offset, 0);
    assert_eq!(erasure.payload_len, 0);
}

#[test]
fn same_block_repair_recovers_empty_opus_group_and_packet_fragment_exactly() {
    // 1,275 bytes is the maximum legal Opus packet size and forces this group
    // to span source symbols under the complete 1,200-byte transport budget.
    let large_packet = (0..1_275)
        .map(|index| (index as u8).wrapping_mul(29))
        .collect::<Vec<_>>();
    let final_packet = (0..211)
        .map(|index| (index as u8).wrapping_add(17))
        .collect::<Vec<_>>();
    let groups = [
        opus_group(501, 0, 2, &large_packet),
        opus_group(17, 2, 2, &[]),
        opus_group(99, 4, 2, &final_packet),
    ];
    let mut encoder = encoder(4);
    let encoded = encoder
        .encode_epoch(opus_epoch(92, 14_640, &groups))
        .expect("encode fragmented Opus epoch");

    assert!(encoded.source_symbols > groups.len() as u16);
    assert!(encoded.datagrams.iter().all(|datagram| {
        datagram.block_id == encoded.block_id
            && datagram.payload.len() + OUTER_TRANSPORT_OVERHEAD <= MAX_DATAGRAM_SIZE
    }));
    assert!(encoded.datagrams[..encoded.source_datagram_count()]
        .iter()
        .all(|datagram| matches!(datagram.role, MultichannelAudioDatagramRole::Source { .. })));

    let mut dropped_sources = BTreeSet::new();
    for datagram in encoded
        .datagrams
        .iter()
        .take(encoded.source_datagram_count())
    {
        let header = source_shard_header(datagram);
        if header.group_id == 17 || (header.group_id == 501 && header.fragment_index == 1) {
            dropped_sources.insert(header.source_index);
        }
    }
    assert_eq!(
        dropped_sources.len(),
        2,
        "drop the empty erasure group and one fragment from the large Opus packet"
    );

    let mut decoder = MultichannelAudioFecDecoder::new();
    let mut decoded = Vec::new();
    for datagram in &encoded.datagrams {
        if matches!(
            datagram.role,
            MultichannelAudioDatagramRole::Source { source_index }
                if dropped_sources.contains(&source_index)
        ) {
            continue;
        }
        decoded.extend(
            decoder
                .push_datagram(&datagram.payload)
                .expect("decode same-block Opus shard"),
        );
    }

    assert_eq!(decoded.len(), usize::from(encoded.source_symbols));
    assert!(decoded.iter().all(|shard| {
        shard.block_id == encoded.block_id
            && shard.header.epoch_id == 92
            && shard.header.pts_samples == 14_640
    }));

    let recovered_groups = reassemble_groups(decoded);
    assert_eq!(
        recovered_groups
            .iter()
            .map(|group| group.group_id)
            .collect::<Vec<_>>(),
        vec![501, 17, 99]
    );
    assert_eq!(recovered_groups[0].payload, large_packet);
    assert_eq!(recovered_groups[0].raptorq_fragments, 1);
    assert!(recovered_groups[1].payload.is_empty());
    assert_eq!(recovered_groups[1].raptorq_fragments, 1);
    assert_eq!(recovered_groups[2].payload, final_packet);
    assert_eq!(recovered_groups[2].raptorq_fragments, 0);
}

fn encoder(repair_symbols: u32) -> MultichannelAudioFecEncoder {
    MultichannelAudioFecEncoder::new(MultichannelAudioFecConfig {
        max_datagram_size: MAX_DATAGRAM_SIZE,
        transport_overhead: OUTER_TRANSPORT_OVERHEAD,
        repair_symbols,
        ..MultichannelAudioFecConfig::default()
    })
}

fn opus_epoch<'a>(
    epoch_id: u64,
    pts_samples: u64,
    groups: &'a [MultichannelAudioGroup<'a>],
) -> MultichannelAudioEpoch<'a> {
    MultichannelAudioEpoch {
        session_id: 0xDA_70,
        config_generation: 3,
        epoch_id,
        pts_samples,
        sample_rate: 48_000,
        frame_count: 240,
        groups,
    }
}

fn opus_group<'a>(
    group_id: u16,
    channel_start: u16,
    channel_count: u16,
    payload: &'a [u8],
) -> MultichannelAudioGroup<'a> {
    MultichannelAudioGroup {
        group_id,
        channel_start,
        channel_count,
        payload_kind: AudioPayloadKind::Opus,
        sample_format: AudioSampleFormat::Unspecified,
        flags: 0,
        payload,
    }
}

fn source_shard_header(datagram: &MultichannelAudioDatagram) -> MultichannelAudioShardHeader {
    assert!(matches!(
        datagram.role,
        MultichannelAudioDatagramRole::Source { .. }
    ));
    let fec_header = DatagramFecHeader::decode(&datagram.payload).expect("decode FEC header");
    let packet = EncodingPacket::deserialize(
        fec_header
            .payload(&datagram.payload)
            .expect("extract encoding packet"),
    );
    MultichannelAudioShardHeader::decode(packet.data()).expect("decode audio shard header")
}

#[derive(Debug)]
struct ReassembledGroup {
    group_id: u16,
    payload: Vec<u8>,
    raptorq_fragments: usize,
}

fn reassemble_groups(shards: Vec<DecodedMultichannelAudioShard>) -> Vec<ReassembledGroup> {
    let mut by_group = BTreeMap::<u16, Vec<DecodedMultichannelAudioShard>>::new();
    for shard in shards {
        by_group
            .entry(shard.header.group_index)
            .or_default()
            .push(shard);
    }

    by_group
        .into_values()
        .map(|mut fragments| {
            fragments.sort_by_key(|shard| shard.header.fragment_index);
            let first = &fragments[0];
            assert_eq!(fragments.len(), usize::from(first.header.fragment_count));
            let group_id = first.header.group_id;
            let expected_len = first.header.group_payload_len as usize;
            let raptorq_fragments = fragments
                .iter()
                .filter(|shard| shard.recovery == MultichannelAudioRecovery::RaptorQ)
                .count();
            let mut payload = Vec::with_capacity(expected_len);
            for fragment in fragments {
                assert_eq!(fragment.header.group_id, group_id);
                assert_eq!(fragment.header.payload_offset as usize, payload.len());
                payload.extend_from_slice(&fragment.payload);
            }
            assert_eq!(payload.len(), expected_len);
            ReassembledGroup {
                group_id,
                payload,
                raptorq_fragments,
            }
        })
        .collect()
}
