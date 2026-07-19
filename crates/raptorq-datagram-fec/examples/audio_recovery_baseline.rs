use raptorq_datagram_fec::{
    AudioPayloadKind, AudioSampleFormat, MultichannelAudioDatagramRole, MultichannelAudioEpoch,
    MultichannelAudioFecConfig, MultichannelAudioFecDecoder, MultichannelAudioFecEncoder,
    MultichannelAudioGroup, MultichannelAudioRecovery,
};
use serde::Serialize;
use std::collections::HashSet;
use std::env;
use std::time::Instant;

const SAMPLE_RATE: u32 = 48_000;
const FRAME_COUNT: u32 = 240;
const CHANNELS_PER_GROUP: u16 = 8;
const BASE_ONE_WAY_US: u64 = 2_000;
const JITTER_US: u64 = 500;
const PACING_US: u64 = 50;
const EPOCH_US: u64 = FRAME_COUNT as u64 * 1_000_000 / SAMPLE_RATE as u64;
const IPV6_UDP_HEADER_BYTES: u64 = 48;
const DECODER_IN_FLIGHT_LIMIT: usize = 128;

#[derive(Debug, Clone, Copy)]
enum LossProfile {
    Clean,
    Independent { parts_per_million: u32 },
    SourceBurst { length: u16, every_epochs: u64 },
}

impl LossProfile {
    fn name(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::Independent {
                parts_per_million: 1_000,
            } => "independent_0_1pct",
            Self::Independent {
                parts_per_million: 5_000,
            } => "independent_0_5pct",
            Self::Independent {
                parts_per_million: 10_000,
            } => "independent_1pct",
            Self::Independent {
                parts_per_million: 20_000,
            } => "independent_2pct",
            Self::Independent {
                parts_per_million: 50_000,
            } => "independent_5pct",
            Self::SourceBurst { length: 2, .. } => "source_burst_2",
            Self::SourceBurst { length: 4, .. } => "source_burst_4",
            Self::SourceBurst { length: 8, .. } => "source_burst_8",
            _ => "custom",
        }
    }

    fn seeds(self) -> u64 {
        if matches!(self, Self::Clean) {
            1
        } else {
            10
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct Candidate {
    name: &'static str,
    repair_percent: u32,
}

#[derive(Debug)]
struct OwnedGroup {
    group_id: u16,
    channel_start: u16,
    channel_count: u16,
    payload_kind: AudioPayloadKind,
    sample_format: AudioSampleFormat,
    payload: Vec<u8>,
    signal_energy: f64,
}

#[derive(Debug, Serialize)]
struct Percentiles {
    count: usize,
    p50: u64,
    p95: u64,
    p99: u64,
    max: u64,
}

#[derive(Debug, Serialize)]
struct PayloadGeometryRow {
    case: &'static str,
    payload_kind: &'static str,
    payload_semantics: &'static str,
    channels: u16,
    payload_bytes: usize,
    source_symbols: u16,
    repair_symbols: u32,
    source_datagrams: usize,
    repair_datagrams: usize,
    source_datagram_bytes: u64,
    repair_datagram_bytes: u64,
    maximum_datagram_bytes: usize,
    configured_mtu_bytes: usize,
    has_mtu_padded_datagram: bool,
    repair_datagram_count_ratio: f64,
    repair_to_source_datagram_byte_ratio: f64,
}

#[derive(Debug, Serialize)]
struct BaselineRow {
    candidate: &'static str,
    loss_profile: &'static str,
    seeds: u64,
    epochs: u64,
    source_symbols_per_epoch: u16,
    repair_symbols_per_epoch: u32,
    trace_exact_before_deadline: u64,
    trace_loss_recovered_by_raptorq_before_deadline: u64,
    trace_raptorq_recovered_fragments_before_deadline: u64,
    trace_lost_source_fragments_recovered_before_deadline: u64,
    trace_late_exact: u64,
    trace_unrecovered: u64,
    trace_deadline_misses: u64,
    observed_elapsed_exact_before_deadline: u64,
    observed_elapsed_late_exact: u64,
    observed_elapsed_deadline_misses: u64,
    trace_maximum_consecutive_missing_epochs: u64,
    trace_missing_audio_us: u64,
    trace_maximum_consecutive_missing_audio_us: u64,
    trace_non_exact_samples: u64,
    trace_whole_stream_silence_fallback_snr_db: Option<f64>,
    sent_datagrams: u64,
    source_datagrams: u64,
    repair_datagrams: u64,
    received_datagrams: u64,
    dropped_datagrams: u64,
    source_payload_bytes: u64,
    source_datagram_bytes: u64,
    repair_datagram_bytes: u64,
    application_datagram_bytes: u64,
    estimated_ipv6_udp_wire_bytes: u64,
    repair_datagram_count_ratio: f64,
    repair_to_source_datagram_byte_ratio: f64,
    application_datagram_overhead_ratio: f64,
    estimated_ipv6_udp_wire_overhead_ratio: f64,
    packet_arrival_ready_from_epoch_start_us: Percentiles,
    capture_to_render_ready_elapsed_us: Percentiles,
    encode_elapsed_ns: Percentiles,
    decode_pipeline_elapsed_ns: Percentiles,
    peak_decoder_in_flight_blocks: usize,
}

#[derive(Debug, Serialize)]
struct BaselineReport {
    schema: &'static str,
    build_profile: &'static str,
    payload_case: &'static str,
    source_corpus: &'static str,
    exactness_gate: &'static str,
    deadline_origin: &'static str,
    deadline_classification: &'static str,
    deadline_fallback: &'static str,
    latency_model: &'static str,
    timing_scope: &'static str,
    observed_elapsed_interpretation: &'static str,
    elapsed_timing_clock: &'static str,
    network_wire_model: &'static str,
    sample_rate: u32,
    sample_format: &'static str,
    channels: u16,
    frame_count: u32,
    epoch_ms: u32,
    epoch_us: u64,
    deadline_ms: u64,
    mtu_bytes: usize,
    base_one_way_us: u64,
    jitter_us: u64,
    pacing_us: u64,
    epochs_per_seed: u64,
    payload_geometry_cases: Vec<PayloadGeometryRow>,
    rows: Vec<BaselineRow>,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if cfg!(debug_assertions) {
        return Err("audio recovery timings require `cargo run --release`".into());
    }
    let channels = env_u16("NEEDLETAIL_AUDIO_BENCH_CHANNELS", 16)?;
    let epochs_per_seed = env_u64("NEEDLETAIL_AUDIO_BENCH_EPOCHS", 200)?;
    let deadline_ms = env_u64("NEEDLETAIL_AUDIO_BENCH_DEADLINE_MS", 20)?;
    if channels == 0 || channels > 128 {
        return Err("NEEDLETAIL_AUDIO_BENCH_CHANNELS must be between 1 and 128".into());
    }
    if epochs_per_seed == 0 || deadline_ms == 0 {
        return Err("benchmark epochs and deadline must be positive".into());
    }

    let candidates = [
        Candidate {
            name: "raptorq_systematic_no_repair",
            repair_percent: 0,
        },
        Candidate {
            name: "source_first_raptorq_20pct",
            repair_percent: 20,
        },
    ];
    let profiles = [
        LossProfile::Clean,
        LossProfile::Independent {
            parts_per_million: 1_000,
        },
        LossProfile::Independent {
            parts_per_million: 5_000,
        },
        LossProfile::Independent {
            parts_per_million: 10_000,
        },
        LossProfile::Independent {
            parts_per_million: 20_000,
        },
        LossProfile::Independent {
            parts_per_million: 50_000,
        },
        LossProfile::SourceBurst {
            length: 2,
            every_epochs: 10,
        },
        LossProfile::SourceBurst {
            length: 4,
            every_epochs: 10,
        },
        LossProfile::SourceBurst {
            length: 8,
            every_epochs: 10,
        },
    ];

    let mut rows = Vec::new();
    for candidate in candidates {
        for profile in profiles {
            rows.push(run_case(
                candidate,
                profile,
                channels,
                epochs_per_seed,
                deadline_ms,
            )?);
        }
    }

    let report = BaselineReport {
        schema: "needletail.audio-recovery-baseline.v2",
        build_profile: "release",
        payload_case: "pcm_s24le",
        source_corpus: "deterministic_decorrelated_full_scale_noise_v1",
        exactness_gate: "trace gate: all encoded PCM bytes equal by the deterministic packet-arrival deadline",
        deadline_origin: "first sample of each 5 ms capture epoch",
        deadline_classification: "deterministic capture-plus-arrival trace; measured encode/decode elapsed time is reported separately and never changes recovery outcomes",
        deadline_fallback: "trace-quality fallback: silence the whole multichannel epoch",
        latency_model: "capture interval + measured encode elapsed time + paced synthetic one-way path + measured single-thread decode pipeline elapsed time",
        timing_scope: "each epoch starts with an empty simulated execution queue; elapsed percentiles do not model sustained decoder backlog",
        observed_elapsed_interpretation: "measured diagnostic: use observed_elapsed_* and capture_to_render_ready_elapsed_us for host execution results; never present trace_* success as measured capture-to-render success",
        elapsed_timing_clock: "std::time::Instant monotonic wall elapsed; not CPU time",
        network_wire_model: "encoded application datagram bytes plus 40-byte IPv6 and 8-byte UDP headers; excludes link-layer overhead",
        sample_rate: SAMPLE_RATE,
        sample_format: "s24le",
        channels,
        frame_count: FRAME_COUNT,
        epoch_ms: FRAME_COUNT * 1_000 / SAMPLE_RATE,
        epoch_us: EPOCH_US,
        deadline_ms,
        mtu_bytes: MultichannelAudioFecConfig::default().max_datagram_size,
        base_one_way_us: BASE_ONE_WAY_US,
        jitter_us: JITTER_US,
        pacing_us: PACING_US,
        epochs_per_seed,
        payload_geometry_cases: payload_geometry_cases()?,
        rows,
    };
    println!("{}", serde_json::to_string_pretty(&report)?);
    Ok(())
}

fn payload_geometry_cases() -> Result<Vec<PayloadGeometryRow>, Box<dyn std::error::Error>> {
    let cases = [
        (
            "pcm_s24le_mono_5ms",
            "pcm",
            "one 5 ms S24LE mono epoch",
            AudioPayloadKind::Pcm,
            AudioSampleFormat::S24Le,
            720_usize,
        ),
        (
            "flac_s24le_mono_360_bytes",
            "flac",
            "opaque encoded-payload length fixture; no codec decode",
            AudioPayloadKind::Flac,
            AudioSampleFormat::S24Le,
            360,
        ),
        (
            "opus_mono_5_bytes",
            "opus",
            "opaque encoded-payload length fixture; no codec decode",
            AudioPayloadKind::Opus,
            AudioSampleFormat::Unspecified,
            5,
        ),
        (
            "opus_mono_20_bytes",
            "opus",
            "opaque encoded-payload length fixture; no codec decode",
            AudioPayloadKind::Opus,
            AudioSampleFormat::Unspecified,
            20,
        ),
        (
            "opus_mono_60_bytes",
            "opus",
            "opaque encoded-payload length fixture; no codec decode",
            AudioPayloadKind::Opus,
            AudioSampleFormat::Unspecified,
            60,
        ),
        (
            "opus_mono_160_bytes",
            "opus",
            "opaque encoded-payload length fixture; no codec decode",
            AudioPayloadKind::Opus,
            AudioSampleFormat::Unspecified,
            160,
        ),
        (
            "opus_mono_400_bytes",
            "opus",
            "opaque encoded-payload length fixture; no codec decode",
            AudioPayloadKind::Opus,
            AudioSampleFormat::Unspecified,
            400,
        ),
        (
            "opus_mono_maximum_1275_bytes",
            "opus",
            "opaque maximum Opus packet length fixture; no codec decode",
            AudioPayloadKind::Opus,
            AudioSampleFormat::Unspecified,
            1_275,
        ),
    ];

    cases
        .into_iter()
        .enumerate()
        .map(
            |(case_index, (case, kind, semantics, payload_kind, sample_format, payload_len))| {
                let payload = (0..payload_len)
                    .map(|index| mix64(case_index as u64 ^ index as u64) as u8)
                    .collect::<Vec<_>>();
                let groups = [MultichannelAudioGroup {
                    group_id: 0,
                    channel_start: 0,
                    channel_count: 1,
                    payload_kind,
                    sample_format,
                    flags: 0,
                    payload: &payload,
                }];
                let mut config = MultichannelAudioFecConfig::default();
                let source_symbols = config.geometry_for_groups(&groups)?.source_symbols;
                config.repair_symbols = u32::from(source_symbols)
                    .saturating_mul(20)
                    .div_ceil(100)
                    .max(1);
                let mut encoder = MultichannelAudioFecEncoder::new(config);
                let encoded = encoder.encode_epoch(MultichannelAudioEpoch {
                    session_id: 0x4745_4f4d_4554_5259,
                    config_generation: 1,
                    epoch_id: case_index as u64,
                    pts_samples: case_index as u64 * u64::from(FRAME_COUNT),
                    sample_rate: SAMPLE_RATE,
                    frame_count: FRAME_COUNT,
                    groups: &groups,
                })?;
                let source_datagrams = encoded.source_datagram_count();
                let repair_datagrams = encoded.repair_datagram_count();
                let source_datagram_bytes = encoded
                    .datagrams
                    .iter()
                    .filter(|datagram| {
                        matches!(datagram.role, MultichannelAudioDatagramRole::Source { .. })
                    })
                    .map(|datagram| datagram.payload.len() as u64)
                    .sum::<u64>();
                let repair_datagram_bytes = encoded
                    .datagrams
                    .iter()
                    .filter(|datagram| {
                        matches!(datagram.role, MultichannelAudioDatagramRole::Repair { .. })
                    })
                    .map(|datagram| datagram.payload.len() as u64)
                    .sum::<u64>();
                let maximum_datagram_bytes = encoded
                    .datagrams
                    .iter()
                    .map(|datagram| datagram.payload.len())
                    .max()
                    .unwrap_or(0);

                Ok(PayloadGeometryRow {
                    case,
                    payload_kind: kind,
                    payload_semantics: semantics,
                    channels: 1,
                    payload_bytes: payload_len,
                    source_symbols,
                    repair_symbols: config.repair_symbols,
                    source_datagrams,
                    repair_datagrams,
                    source_datagram_bytes,
                    repair_datagram_bytes,
                    maximum_datagram_bytes,
                    configured_mtu_bytes: config.max_datagram_size,
                    has_mtu_padded_datagram: maximum_datagram_bytes == config.max_datagram_size,
                    repair_datagram_count_ratio: ratio(
                        repair_datagrams as u64,
                        source_datagrams as u64,
                    ),
                    repair_to_source_datagram_byte_ratio: ratio(
                        repair_datagram_bytes,
                        source_datagram_bytes,
                    ),
                })
            },
        )
        .collect()
}

fn run_case(
    candidate: Candidate,
    profile: LossProfile,
    channels: u16,
    epochs_per_seed: u64,
    deadline_ms: u64,
) -> Result<BaselineRow, Box<dyn std::error::Error>> {
    let sizing_groups = make_groups(0, channels);
    let sizing_views = group_views(&sizing_groups);
    let mut config = MultichannelAudioFecConfig::default();
    let source_symbols = config.geometry_for_groups(&sizing_views)?.source_symbols;
    config.repair_symbols = if candidate.repair_percent == 0 {
        0
    } else {
        u32::from(source_symbols)
            .saturating_mul(candidate.repair_percent)
            .div_ceil(100)
            .max(1)
    };
    let repair_symbols = config.repair_symbols;
    let mut encoder = MultichannelAudioFecEncoder::new(config);
    let mut decoder =
        MultichannelAudioFecDecoder::new().with_in_flight_limit(DECODER_IN_FLIGHT_LIMIT);

    let mut exact_before_deadline = 0_u64;
    let mut loss_recovered_by_raptorq_before_deadline = 0_u64;
    let mut raptorq_recovered_fragments = 0_u64;
    let mut lost_source_fragments_recovered = 0_u64;
    let mut late_exact = 0_u64;
    let mut unrecovered = 0_u64;
    let mut observed_elapsed_exact_before_deadline = 0_u64;
    let mut observed_elapsed_late_exact = 0_u64;
    let mut sent_datagrams = 0_u64;
    let mut source_datagrams = 0_u64;
    let mut repair_datagrams = 0_u64;
    let mut dropped_datagrams = 0_u64;
    let mut source_payload_bytes = 0_u64;
    let mut source_datagram_bytes = 0_u64;
    let mut repair_datagram_bytes = 0_u64;
    let mut total_signal_energy = 0_f64;
    let mut error_energy = 0_f64;
    let mut non_exact_samples = 0_u64;
    let mut max_missing_run = 0_u64;
    let mut encode_elapsed = Vec::new();
    let mut decode_pipeline_elapsed = Vec::new();
    let mut packet_arrival_ready = Vec::new();
    let mut capture_to_render_ready = Vec::new();
    let mut peak_decoder_in_flight_blocks = 0_usize;

    for seed in 0..profile.seeds() {
        let mut missing_run = 0_u64;
        for epoch_id in 0..epochs_per_seed {
            let absolute_epoch = seed
                .saturating_mul(epochs_per_seed)
                .saturating_add(epoch_id);
            let groups = make_groups(absolute_epoch, channels);
            let views = group_views(&groups);
            let epoch_energy = groups.iter().map(|group| group.signal_energy).sum::<f64>();
            let epoch_payload_bytes = groups
                .iter()
                .map(|group| group.payload.len())
                .sum::<usize>();
            total_signal_energy += epoch_energy;
            source_payload_bytes = source_payload_bytes.saturating_add(epoch_payload_bytes as u64);

            let encode_started = Instant::now();
            let encoded = encoder.encode_epoch(MultichannelAudioEpoch {
                session_id: 0x4155_4449_4f42_454e,
                config_generation: 1,
                epoch_id: absolute_epoch,
                pts_samples: absolute_epoch.saturating_mul(u64::from(FRAME_COUNT)),
                sample_rate: SAMPLE_RATE,
                frame_count: FRAME_COUNT,
                groups: &views,
            })?;
            let epoch_encode_elapsed_ns = duration_ns(encode_started);
            encode_elapsed.push(epoch_encode_elapsed_ns);
            sent_datagrams = sent_datagrams.saturating_add(encoded.datagrams.len() as u64);
            for datagram in &encoded.datagrams {
                match datagram.role {
                    MultichannelAudioDatagramRole::Source { .. } => {
                        source_datagrams = source_datagrams.saturating_add(1);
                        source_datagram_bytes =
                            source_datagram_bytes.saturating_add(datagram.payload.len() as u64);
                    }
                    MultichannelAudioDatagramRole::Repair { .. } => {
                        repair_datagrams = repair_datagrams.saturating_add(1);
                        repair_datagram_bytes =
                            repair_datagram_bytes.saturating_add(datagram.payload.len() as u64);
                    }
                }
            }

            let mut arrivals = Vec::with_capacity(encoded.datagrams.len());
            let mut dropped_sources = HashSet::new();
            for (wire_index, datagram) in encoded.datagrams.iter().enumerate() {
                if should_drop(
                    profile,
                    seed,
                    epoch_id,
                    datagram.role,
                    encoded.source_symbols,
                ) {
                    dropped_datagrams = dropped_datagrams.saturating_add(1);
                    if let MultichannelAudioDatagramRole::Source { source_index } = datagram.role {
                        dropped_sources.insert(source_index);
                    }
                    continue;
                }
                let jitter = mix64(
                    seed ^ absolute_epoch.rotate_left(17) ^ (wire_index as u64).rotate_left(41),
                ) % (JITTER_US + 1);
                let arrival_us = EPOCH_US
                    .saturating_add(BASE_ONE_WAY_US)
                    .saturating_add((wire_index as u64).saturating_mul(PACING_US))
                    .saturating_add(jitter);
                arrivals.push((arrival_us, wire_index));
            }
            arrivals.sort_unstable();

            let mut delivered = HashSet::new();
            let mut packet_arrival_completion_us = None;
            let mut render_completion_us = None;
            let mut decoder_available_us = 0_u64;
            let mut epoch_decode_elapsed_ns = 0_u64;
            let mut epoch_recovered_fragments = 0_u64;
            let mut epoch_lost_source_fragments_recovered = 0_u64;
            for (arrival_us, wire_index) in arrivals {
                let observed_arrival_us =
                    arrival_us.saturating_add(ns_to_us_ceil(epoch_encode_elapsed_ns));
                let processing_started_us = observed_arrival_us.max(decoder_available_us);
                let decode_started = Instant::now();
                let shards = decoder.push_datagram(&encoded.datagrams[wire_index].payload)?;
                for shard in shards {
                    let group = &groups[usize::from(shard.header.group_index)];
                    let start = shard.header.payload_offset as usize;
                    let end = start.saturating_add(shard.header.payload_len as usize);
                    if group.payload.get(start..end) != Some(shard.payload.as_ref()) {
                        return Err(format!(
                            "candidate {} profile {} produced non-exact epoch {} shard {}",
                            candidate.name,
                            profile.name(),
                            absolute_epoch,
                            shard.header.source_index
                        )
                        .into());
                    }
                    if delivered.insert(shard.header.source_index)
                        && matches!(shard.recovery, MultichannelAudioRecovery::RaptorQ)
                    {
                        epoch_recovered_fragments = epoch_recovered_fragments.saturating_add(1);
                        if dropped_sources.contains(&shard.header.source_index) {
                            epoch_lost_source_fragments_recovered =
                                epoch_lost_source_fragments_recovered.saturating_add(1);
                        }
                    }
                }
                let datagram_decode_elapsed_ns = duration_ns(decode_started);
                epoch_decode_elapsed_ns =
                    epoch_decode_elapsed_ns.saturating_add(datagram_decode_elapsed_ns);
                decoder_available_us =
                    processing_started_us.saturating_add(ns_to_us_ceil(datagram_decode_elapsed_ns));
                peak_decoder_in_flight_blocks =
                    peak_decoder_in_flight_blocks.max(decoder.in_flight_block_count());
                if render_completion_us.is_none()
                    && delivered.len() == usize::from(encoded.source_symbols)
                {
                    packet_arrival_completion_us = Some(arrival_us);
                    render_completion_us = Some(decoder_available_us);
                }
            }
            decode_pipeline_elapsed.push(epoch_decode_elapsed_ns);
            if render_completion_us.is_none() {
                decoder.expire_block(encoded.block_id);
            }

            let deadline_us = deadline_ms.saturating_mul(1_000);
            match packet_arrival_completion_us {
                Some(arrival_completed_us) if arrival_completed_us <= deadline_us => {
                    exact_before_deadline = exact_before_deadline.saturating_add(1);
                    raptorq_recovered_fragments =
                        raptorq_recovered_fragments.saturating_add(epoch_recovered_fragments);
                    lost_source_fragments_recovered = lost_source_fragments_recovered
                        .saturating_add(epoch_lost_source_fragments_recovered);
                    if epoch_lost_source_fragments_recovered > 0 {
                        loss_recovered_by_raptorq_before_deadline =
                            loss_recovered_by_raptorq_before_deadline.saturating_add(1);
                    }
                    let render_completed_us = render_completion_us.unwrap_or(arrival_completed_us);
                    if render_completed_us <= deadline_us {
                        observed_elapsed_exact_before_deadline =
                            observed_elapsed_exact_before_deadline.saturating_add(1);
                    } else {
                        observed_elapsed_late_exact = observed_elapsed_late_exact.saturating_add(1);
                    }
                    packet_arrival_ready.push(arrival_completed_us);
                    capture_to_render_ready.push(render_completed_us);
                    missing_run = 0;
                }
                Some(_) => {
                    late_exact = late_exact.saturating_add(1);
                    observed_elapsed_late_exact = observed_elapsed_late_exact.saturating_add(1);
                    missing_run = missing_run.saturating_add(1);
                    max_missing_run = max_missing_run.max(missing_run);
                    error_energy += epoch_energy;
                    non_exact_samples = non_exact_samples
                        .saturating_add(u64::from(FRAME_COUNT) * u64::from(channels));
                }
                None => {
                    unrecovered = unrecovered.saturating_add(1);
                    missing_run = missing_run.saturating_add(1);
                    max_missing_run = max_missing_run.max(missing_run);
                    error_energy += epoch_energy;
                    non_exact_samples = non_exact_samples
                        .saturating_add(u64::from(FRAME_COUNT) * u64::from(channels));
                }
            }
        }
    }

    let epochs = epochs_per_seed.saturating_mul(profile.seeds());
    let deadline_misses = epochs.saturating_sub(exact_before_deadline);
    let observed_elapsed_deadline_misses =
        epochs.saturating_sub(observed_elapsed_exact_before_deadline);
    let silence_fallback_snr_db =
        (error_energy > 0.0).then(|| 10.0 * (total_signal_energy / error_energy).log10());
    let application_datagram_bytes = source_datagram_bytes.saturating_add(repair_datagram_bytes);
    let estimated_ipv6_udp_wire_bytes = application_datagram_bytes
        .saturating_add(sent_datagrams.saturating_mul(IPV6_UDP_HEADER_BYTES));
    Ok(BaselineRow {
        candidate: candidate.name,
        loss_profile: profile.name(),
        seeds: profile.seeds(),
        epochs,
        source_symbols_per_epoch: source_symbols,
        repair_symbols_per_epoch: repair_symbols,
        trace_exact_before_deadline: exact_before_deadline,
        trace_loss_recovered_by_raptorq_before_deadline: loss_recovered_by_raptorq_before_deadline,
        trace_raptorq_recovered_fragments_before_deadline: raptorq_recovered_fragments,
        trace_lost_source_fragments_recovered_before_deadline: lost_source_fragments_recovered,
        trace_late_exact: late_exact,
        trace_unrecovered: unrecovered,
        trace_deadline_misses: deadline_misses,
        observed_elapsed_exact_before_deadline,
        observed_elapsed_late_exact,
        observed_elapsed_deadline_misses,
        trace_maximum_consecutive_missing_epochs: max_missing_run,
        trace_missing_audio_us: deadline_misses.saturating_mul(EPOCH_US),
        trace_maximum_consecutive_missing_audio_us: max_missing_run.saturating_mul(EPOCH_US),
        trace_non_exact_samples: non_exact_samples,
        trace_whole_stream_silence_fallback_snr_db: silence_fallback_snr_db,
        sent_datagrams,
        source_datagrams,
        repair_datagrams,
        received_datagrams: sent_datagrams.saturating_sub(dropped_datagrams),
        dropped_datagrams,
        source_payload_bytes,
        source_datagram_bytes,
        repair_datagram_bytes,
        application_datagram_bytes,
        estimated_ipv6_udp_wire_bytes,
        repair_datagram_count_ratio: ratio(repair_datagrams, source_datagrams),
        repair_to_source_datagram_byte_ratio: ratio(repair_datagram_bytes, source_datagram_bytes),
        application_datagram_overhead_ratio: ratio(
            application_datagram_bytes,
            source_payload_bytes,
        ),
        estimated_ipv6_udp_wire_overhead_ratio: ratio(
            estimated_ipv6_udp_wire_bytes,
            source_payload_bytes,
        ),
        packet_arrival_ready_from_epoch_start_us: percentiles(packet_arrival_ready),
        capture_to_render_ready_elapsed_us: percentiles(capture_to_render_ready),
        encode_elapsed_ns: percentiles(encode_elapsed),
        decode_pipeline_elapsed_ns: percentiles(decode_pipeline_elapsed),
        peak_decoder_in_flight_blocks,
    })
}

fn make_groups(epoch_id: u64, channels: u16) -> Vec<OwnedGroup> {
    let mut groups = Vec::new();
    let mut channel_start = 0_u16;
    while channel_start < channels {
        let channel_count = CHANNELS_PER_GROUP.min(channels - channel_start);
        let mut payload = Vec::with_capacity(FRAME_COUNT as usize * usize::from(channel_count) * 3);
        let mut signal_energy = 0_f64;
        for frame in 0..FRAME_COUNT {
            for channel_offset in 0..channel_count {
                let channel = channel_start + channel_offset;
                let sample = sample_value(epoch_id, frame, channel);
                signal_energy += f64::from(sample) * f64::from(sample);
                let encoded = sample.to_le_bytes();
                payload.extend_from_slice(&encoded[..3]);
            }
        }
        groups.push(OwnedGroup {
            group_id: channel_start / CHANNELS_PER_GROUP,
            channel_start,
            channel_count,
            payload_kind: AudioPayloadKind::Pcm,
            sample_format: AudioSampleFormat::S24Le,
            payload,
            signal_energy,
        });
        channel_start += channel_count;
    }
    groups
}

fn group_views(groups: &[OwnedGroup]) -> Vec<MultichannelAudioGroup<'_>> {
    groups
        .iter()
        .map(|group| MultichannelAudioGroup {
            group_id: group.group_id,
            channel_start: group.channel_start,
            channel_count: group.channel_count,
            payload_kind: group.payload_kind,
            sample_format: group.sample_format,
            flags: 0,
            payload: &group.payload,
        })
        .collect()
}

fn sample_value(epoch_id: u64, frame: u32, channel: u16) -> i32 {
    let mixed =
        mix64(epoch_id ^ u64::from(frame).rotate_left(19) ^ u64::from(channel).rotate_left(43));
    ((mixed & 0x00ff_ffff) as i32) - 0x0080_0000
}

fn should_drop(
    profile: LossProfile,
    seed: u64,
    epoch_id: u64,
    role: MultichannelAudioDatagramRole,
    source_symbols: u16,
) -> bool {
    match profile {
        LossProfile::Clean => false,
        LossProfile::Independent { parts_per_million } => {
            let role_key = match role {
                MultichannelAudioDatagramRole::Source { source_index } => u64::from(source_index),
                MultichannelAudioDatagramRole::Repair { encoding_symbol_id } => {
                    0x8000_0000 | u64::from(encoding_symbol_id)
                }
            };
            mix64(seed ^ epoch_id.rotate_left(23) ^ role_key.rotate_left(47)) % 1_000_000
                < u64::from(parts_per_million)
        }
        LossProfile::SourceBurst {
            length,
            every_epochs,
        } => {
            if epoch_id % every_epochs != seed % every_epochs {
                return false;
            }
            let MultichannelAudioDatagramRole::Source { source_index } = role else {
                return false;
            };
            let available_starts = source_symbols.saturating_sub(length).saturating_add(1);
            let start = if available_starts == 0 {
                0
            } else {
                (mix64(seed ^ epoch_id.rotate_left(31)) % u64::from(available_starts)) as u16
            };
            source_index >= start && source_index < start.saturating_add(length)
        }
    }
}

fn mix64(mut value: u64) -> u64 {
    value = value.wrapping_add(0x9e37_79b9_7f4a_7c15);
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

fn duration_ns(started: Instant) -> u64 {
    started.elapsed().as_nanos().min(u128::from(u64::MAX)) as u64
}

fn ns_to_us_ceil(nanoseconds: u64) -> u64 {
    nanoseconds.saturating_add(999) / 1_000
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn percentiles(mut values: Vec<u64>) -> Percentiles {
    if values.is_empty() {
        return Percentiles {
            count: 0,
            p50: 0,
            p95: 0,
            p99: 0,
            max: 0,
        };
    }
    values.sort_unstable();
    Percentiles {
        count: values.len(),
        p50: percentile(&values, 50),
        p95: percentile(&values, 95),
        p99: percentile(&values, 99),
        max: *values.last().unwrap_or(&0),
    }
}

fn percentile(values: &[u64], percentile: usize) -> u64 {
    let rank = values.len().saturating_mul(percentile).saturating_add(99) / 100;
    values[rank.saturating_sub(1).min(values.len() - 1)]
}

fn env_u64(name: &str, default: u64) -> Result<u64, Box<dyn std::error::Error>> {
    match env::var(name) {
        Ok(value) => Ok(value.parse::<u64>()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

fn env_u16(name: &str, default: u16) -> Result<u16, Box<dyn std::error::Error>> {
    match env::var(name) {
        Ok(value) => Ok(value.parse::<u16>()?),
        Err(env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn payload_geometry_report_proves_tiny_opus_is_not_mtu_padded() {
        let rows = payload_geometry_cases().unwrap();
        assert!(rows.iter().all(|row| !row.has_mtu_padded_datagram));
        assert!(rows
            .iter()
            .all(|row| row.maximum_datagram_bytes < row.configured_mtu_bytes));

        let tiny_opus = rows
            .iter()
            .find(|row| row.case == "opus_mono_5_bytes")
            .unwrap();
        assert_eq!(tiny_opus.source_symbols, 1);
        assert_eq!(tiny_opus.repair_symbols, 1);
        assert_eq!(tiny_opus.repair_datagram_count_ratio, 1.0);

        let maximum_opus = rows
            .iter()
            .find(|row| row.case == "opus_mono_maximum_1275_bytes")
            .unwrap();
        assert_eq!(maximum_opus.payload_bytes, 1_275);
        assert!(maximum_opus.maximum_datagram_bytes < 1_200);
    }

    #[test]
    fn source_loss_is_candidate_independent() {
        let profile = LossProfile::Independent {
            parts_per_million: 50_000,
        };
        let groups = make_groups(0, 16);
        let views = group_views(&groups);
        let epoch = MultichannelAudioEpoch {
            session_id: 1,
            config_generation: 1,
            epoch_id: 0,
            pts_samples: 0,
            sample_rate: SAMPLE_RATE,
            frame_count: FRAME_COUNT,
            groups: &views,
        };
        let mut no_repair = MultichannelAudioFecEncoder::new(MultichannelAudioFecConfig {
            repair_symbols: 0,
            ..MultichannelAudioFecConfig::default()
        });
        let mut with_repair = MultichannelAudioFecEncoder::new(MultichannelAudioFecConfig {
            repair_symbols: 3,
            ..MultichannelAudioFecConfig::default()
        });
        let no_repair = no_repair.encode_epoch(epoch).unwrap();
        let with_repair = with_repair.encode_epoch(epoch).unwrap();

        let source_trace = |encoded: &raptorq_datagram_fec::EncodedMultichannelAudioEpoch| {
            encoded
                .datagrams
                .iter()
                .filter_map(|packet| match packet.role {
                    MultichannelAudioDatagramRole::Source { source_index } => Some((
                        source_index,
                        should_drop(profile, 7, 19, packet.role, encoded.source_symbols),
                    )),
                    MultichannelAudioDatagramRole::Repair { .. } => None,
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(source_trace(&no_repair), source_trace(&with_repair));
    }

    #[test]
    fn benchmark_reuses_decoder_but_expires_every_deadline_miss() {
        let row = run_case(
            Candidate {
                name: "no_repair_test",
                repair_percent: 0,
            },
            LossProfile::SourceBurst {
                length: 1,
                every_epochs: 1,
            },
            16,
            4,
            20,
        )
        .unwrap();

        assert_eq!(row.epochs, 40);
        assert_eq!(row.trace_exact_before_deadline, 0);
        assert_eq!(row.trace_unrecovered, 40);
        assert_eq!(row.peak_decoder_in_flight_blocks, 1);
    }

    #[test]
    fn burst_has_exact_requested_length() {
        let profile = LossProfile::SourceBurst {
            length: 4,
            every_epochs: 1,
        };
        let dropped = (0..16)
            .filter(|source_index| {
                should_drop(
                    profile,
                    3,
                    9,
                    MultichannelAudioDatagramRole::Source {
                        source_index: *source_index,
                    },
                    16,
                )
            })
            .count();
        assert_eq!(dropped, 4);
    }
}
