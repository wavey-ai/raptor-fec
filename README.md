# raptor-fec

Reusable RaptorQ forward-error-correction framing for low-latency datagram transports.

This repository contains two public crates:

- `raptorq-datagram-fec`: the wire protocol, RaptorQ block encoder/decoder, media access-unit framing, adaptive repair policy, congestion state, and optional Tokio UDP sender/receiver helpers.
- `raptorq-fec-transport`: transport-level wrappers for carrying the same FEC datagrams over WebTransport datagrams and WebRTC data channels without adding a per-datagram stream prefix by default.

For live lossless PCM, read
[`docs/pcm-low-latency-transport.md`](docs/pcm-low-latency-transport.md) before
selecting this crate's RaptorQ audio API. RaptorQ is not the default Wavey live
PCM FEC: paced UDP/RTP with same-epoch XOR or small Reed-Solomon is the primary
fixed-deadline design, while TCP/TLS is the sustained-throughput and reliable
recording baseline.

The current datagram wire format is a byte-oriented Wavey RaptorQ v2 packet.
It is intentionally not TS-, RIST-, or contributor-protocol-aware; ingress and
egress crates can wrap or unwrap those protocols, but mesh sync should move
these RaptorQ datagrams.

```text
0               4       6       8              12              16
+---------------+-------+-------+---------------+---------------+
| magic "RQD2"  |ver|len|kind|fl|   block_id    |transfer_length|
+---------------+-------+-------+---------------+---------------+
| packet_seq    |src_syms|sym_sz |  payload_len  | packet_crc32  |
+---------------+-------+-------+---------------+---------------+
|             RaptorQ EncodingPacket payload bytes ...           |
```

The fixed header is 32 bytes:

- `magic`: four bytes, currently `RQD2`.
- `version`: one byte, currently `2`.
- `header_len`: one byte, currently `32`.
- `kind`: one byte, currently `1` for a serialized RaptorQ `EncodingPacket`.
- `flags`: one byte, currently bit `0` means `packet_crc32` is present.
- `block_id`, `transfer_length`, and `packet_seq`: little-endian `u32`.
- `src_syms` and `sym_sz`: little-endian `u16`.
- `payload_len`: exact byte length of the serialized RaptorQ payload.
- `packet_crc32`: IEEE CRC32 over the encoded header prefix and payload. The stored CRC field itself is excluded from the checksum, matching the SoundKit v2 packet-header convention.

The CRC implementation uses `crc32fast` with an explicit initial state. This
changes no RQD2 bytes or incremental-update semantics: reference bitwise,
single-pass, split-update, empty-input, and known-vector tests must all agree.
In the matched Needletail private-GCP profile, this removed the prior bitwise
CRC hot spot while preserving exact delivery; the product-level result and its
remaining strict deadline failure are documented in
[`Needletail's tail-bundle report`](../needletail/docs/real-world-tests/2026-07-19-opus-h3-tail-bundle.md).

Media frames can still use the optional 44-byte protected fragment header above
the byte payload when a caller wants a `u64` stream id, access-unit sequence,
PTS/DTS delta, duration, initialization/keyframe/config flags, and fragment boundaries. The
RaptorQ datagram layer itself treats that as ordinary bytes.
`EncodedMediaFrame.blocks` exposes each protected media block's source-symbol
count, repair-symbol count, payload length, and source/repair datagram ranges so
callers can reason about recovery budget per fragment instead of inferring it
from private packet headers.
`EncodedMediaFrame` also exposes source-first send plans and per-frame FEC
stats so a transport can prioritize block-fill datagrams, defer lower-priority
repair, and report overhead without parsing private RaptorQ payloads.
For multi-frame queues, `plan_media_datagrams_with_deadlines` accepts absolute
microsecond expiry and emits a bounded source-first plan. Source and repair
symbols are ordered by initialization, codec configuration, keyframe, audio,
delta, then data importance. Every source symbol carries primary-path intent;
every repair symbol carries independent secondary-path intent. Queue delay and
pacing are included when expired work is removed.
`MediaRecoveryPolicy` deterministically prefers RaptorQ repair that can arrive
inside the remaining deadline, selects reliable object fetch when that estimate
fits instead, and expires obsolete work. Extra repair is hard bounded.
`MediaDeadlineOutcome` exposes elapsed time, hit/miss, headroom, and lateness for
deadline-hit and p50/p95/p99 histograms; `MediaFecRepairCounters` separates
RaptorQ-repair, reliable-fetch, recovery-expiry, and send-expiry outcomes.
`NetworkMetricsObservation` adapts real sequence loss, RTT, jitter, queue delay,
and bitrate inputs into `AdaptiveFecController`. `MediaFecRepairCounters`
collects repair-effectiveness counters, and `MediaBackfillStore` keeps recent
encoded datagrams available for reliable-path backfill when loss exceeds parity.

Music-production audio can use `MusicAudioMicroBlockEncoder` and
`MusicAudioMicroBlockDecoder` for exact micro-block repair of caller-supplied
audio chunks. These are correctness prototypes, not production PCM presets. The
2.5 ms preset groups four chunks and the 5 ms preset groups two, so both wait for
10 ms of audio before emitting a block. The 5 ms preset also creates a 2,084-byte
RQD2 datagram before outer transport overhead, which is not Internet-MTU safe.
The documented `4 source + 1 repair` result depends on a 960-byte test fixture;
the config does not define PCM sample representation or bit depth. See the PCM
transport note above for the complete limitations and replacement direction.

This API is not PLC or codec concealment: when enough source or repair datagrams
arrive, the decoder returns the exact original chunks; when the repair budget is
exceeded, the caller decides whether to drop, silence, or halt. The caller owns
the actual monotonic playout deadline; `playout_delay_samples` does not currently
schedule or enforce one.

## Multichannel Audio Recovery Baseline

The release-only baseline replays deterministic loss and arrival traces through
the real source-first multichannel encoder/decoder. Its default corpus is 48 kHz
S24LE PCM, 16 channels, and 5 ms epochs:

```sh
cargo run --release -p raptorq-datagram-fec \
  --example audio_recovery_baseline
```

`NEEDLETAIL_AUDIO_BENCH_CHANNELS`, `NEEDLETAIL_AUDIO_BENCH_EPOCHS`, and
`NEEDLETAIL_AUDIO_BENCH_DEADLINE_MS` select the channel count, epochs per seed,
and trace deadline. The JSON separates source and repair datagrams and bytes,
application framing, and an IPv6/UDP wire estimate. It also exposes repair-ratio
rounding: a 20% policy produces three repairs for twelve sources (25%), while a
one-source packet requires one whole repair (100% packet overhead).

Fields prefixed with `trace_` describe deterministic packet-arrival outcomes.
They do not include measured host execution time. The `observed_elapsed_*`,
`capture_to_render_ready_elapsed_us`, `encode_elapsed_ns`, and
`decode_pipeline_elapsed_ns` fields are the measured release-build diagnostics.
Those per-epoch timings start with an empty simulated execution queue and do not
model sustained decoder backlog.

`MultichannelAudioFecDecoder::expire_block` releases an incomplete epoch when
its playout deadline passes. A configurable in-flight limit also expires the
oldest incomplete epoch, so permanent loss cannot grow decoder state without
bound. CRC-valid but invalid audio shards are rejected before they can consume
that capacity or evict live state.

The core tests cover exact opaque FLAC and Opus payload-byte recovery and 5,
20, 60, 160, 400, and 1,275-byte one-channel mono payload geometry without MTU
padding.
This crate preserves each payload's format identity; it does not decode or
transcode codecs. Opus remains Opus, FLAC remains FLAC, and PCM remains PCM for
downstream fMP4 LL-HLS packaging. Decoded-PCM exactness for FLAC and decoded
quality for Opus require the separate codec-integration benchmark; encoded Opus
must never be decoded merely to re-encode it as FLAC.

## Useful Deltas From QUIC

RaptorQ-FEC and QUIC solve different parts of the media transport problem.
This crate should stay focused on bounded, feedback-free repair for datagram
media paths; QUIC remains the better substrate for sessions, fanout, caching,
encryption, congestion control, and eventual retransmission.

This functional comparison is not a continuous-PCM throughput recommendation.
Wavey uses measured TCP/TLS as the bulk PCM baseline and paced UDP/RTP as the
native fixed-deadline lane. QUIC/WebTransport remains a browser/datagram
comparison lane.

Useful differences in favor of this crate:

- Recovery does not wait for ACK/NACK feedback, RTT, or QUIC PTO. If loss stays
  within the repair budget, decode completes as soon as enough source or repair
  symbols arrive.
- Datagram loss is repaired at the media block boundary instead of stalling a
  QUIC stream behind retransmission. That is the main niche for sub-RTT playout
  budgets such as 33 ms video over 70 ms RTT.
- The wire format is transport-independent. The same FEC datagrams can ride
  over UDP, WebTransport datagrams, WebRTC data channels, or a mesh-specific
  socket path.
- Repair cost is explicit and media-aware. Audio, keyframes, delta frames, and
  generic data can use different repair ratios and floors.
- Failure is bounded and visible: when loss exceeds parity, the frame fails
  closed instead of pretending to provide eventual reliability.

Useful differences in favor of QUIC/MoQ:

- QUIC has mature congestion control, pacing, TLS, connection migration,
  stream multiplexing, and retransmission. This crate deliberately does not.
- QUIC/MoQ model freshness with stream priorities, group ordering, cache
  windows, and late-join/backfill semantics. RaptorQ repairs the hot path but
  does not provide a distribution plane.
- QUIC can eventually recover loss beyond a fixed FEC budget if the application
  can tolerate the latency. RaptorQ cannot manufacture symbols beyond the
  repair sent for that block.
- QUIC implementations have strong socket-level pacing and flow-control
  behavior. FEC callers still need to avoid dumping large keyframes into the
  socket without backpressure.

## Interop Testing

The `raptorq-datagram-fec` crate has ignored integration tests that verify raw
RaptorQ packet compatibility against the independent C implementation
[`nanorq`](https://github.com/sleepybishop/nanorq). The tests compile a small
C helper at runtime, then verify both directions:

- Rust `raptorq` symbols decode successfully with `nanorq`.
- `nanorq` symbols decode successfully with Rust `raptorq`.

```sh
git clone --recurse-submodules https://github.com/sleepybishop/nanorq /tmp/nanorq
NANORQ_DIR=/tmp/nanorq cargo test -p raptorq-datagram-fec --test nanorq_interop -- --ignored
```

## Video Loss Verification

The `video_loss_matrix` integration test exercises H.264-style access units
through the real media FEC encoder/decoder. It verifies:

- keyframe recovery under burst and periodic datagram loss
- delta-frame recovery under bounded random loss
- fail-closed behavior when loss exceeds the configured repair budget
- a deterministic payload-size sweep over tiny/small/large keyframes and delta
  frames, with front-burst, late-burst, periodic, and random source loss kept
  within each FEC block's repair budget, plus a fail-closed check one source
  datagram past the per-block repair budget
- explicit source-symbol accounting per FEC block, so recoverable cases do not
  get credit for merely dropping repair packets or staying under a frame-level
  aggregate repair count
- a 90-frame stream where RaptorQ FEC repairs keyframe and delta losses inside a
  33 ms playout budget while a RIST/SRT-style feedback retransmission model
  misses those frames at 70 ms RTT plus RIST's default 20 ms feedback interval
- the same feedback model catches up when the playout buffer is raised above the
  feedback turn plus RTT, making the latency tradeoff explicit.
- a pure-RIST-core comparison that uses `SimpleSenderCore` and
  `SimpleReceiverCore` to detect dropped RTP packets, emit scheduled NACK
  feedback, retransmit from sender history, and prove the retransmitted frame
  reconstructs only after the feedback turn plus RTT.
- a live pure-RIST loopback UDP comparison that sends RTP packets over a data
  socket, drops the same burst, sends RTCP feedback over a feedback socket, and
  only reconstructs the access unit after feedback scheduling plus the simulated
  RTT.
- a sustained 30-frame live pure-RIST UDP stream over separate data and
  feedback sockets showing repeated keyframe and delta-frame losses recover
  after NACK retransmission, but every lost frame misses the 33 ms playout
  deadline at 70 ms RTT.
- a best-case SRT-style ARQ comparison using 1316-byte payload chunks where
  retransmission is allowed after a single RTT with no extra feedback scheduling
  delay; this still misses the 33 ms low-latency deadline on a 70 ms RTT path.
- a companion live libsrt matrix in `av-rs/srt` that sends 18 KB, 40 KB, and
  64 KB video-like payloads, plus a sustained 30-frame video-like stream,
  through a UDP proxy with SRT data burst loss and 35 ms one-way delay,
  verifies exact recovery, and proves the recovery path relies on
  retransmission rather than forward repair.
- a low-latency stream scorecard over 20 ms, 35 ms, and 70 ms RTT paths showing
  RaptorQ is never worse than pure-RIST feedback or the best-case SRT ARQ lower
  bound, and is strictly better once RTT exceeds the 33 ms playout budget.
- a live loopback UDP matrix where encoded media datagrams pass through a lossy
  proxy that drops keyframe and delta-frame datagrams and adds jitter before the
  media decoder reconstructs each exact H.264-style access unit.
- a sustained 30-frame live UDP stream where mixed keyframe and delta-frame
  datagrams are dropped, delayed, and deterministically reordered before the
  decoder reconstructs every frame, while the same losses miss a 33 ms
  feedback-only deadline on a 70 ms RTT path.
- an `av-mesh` media-FEC ingest regression that sends a multi-frame H.264-style
  stream, including a 96 KB multi-block keyframe, through the actual mesh UDP
  ingest path with bounded per-block datagram loss and deterministic
  reordering, then verifies each access unit is cached intact.

```sh
cargo test -p raptorq-datagram-fec --test video_loss_matrix
```

The in-crate SRT comparison is deliberately a favorable lower-bound model, not
a full libsrt benchmark. The companion libsrt socket tests cover the same
burst-loss and sustained-stream shapes over a delayed UDP proxy and confirm SRT
recovers by retransmission. Together with the socket-level RaptorQ media-FEC and
pure-RIST feedback checks, this proves the core algorithmic advantage we need
for low-latency video: if the playout budget is below the
feedback/retransmission turn and packet loss stays within the repair budget,
forward RaptorQ repair can recover frames that feedback-only retransmission
cannot deliver in time.

Do not read that as "RaptorQ is always more reliable than RIST." RaptorQ-FEC is
better for bounded-loss, low-latency media because recovery latency is fixed by
block fill time. RIST/SRT are better for eventual delivery once loss exceeds the
FEC repair budget, because ARQ can retransmit from sender history if the
application has enough latency buffer. Production mesh reliability should pair
this FEC hot path with explicit missing-block repair/backfill instead of
expecting forward repair alone to cover every WAN loss profile.

```sh
(cd ../rist-rs && cargo test -p rist-core feedback)
(cd ../av-rs && cargo test -p srt --lib)
(cd ../av-mesh && cargo test fec)
```

## Useful QUIC/MoQ Deltas Closed

These are the transport-discipline gaps closed in this crate while preserving
RaptorQ's role as the feedback-free repair hot path:

- [x] Source-first media frame planning:
      `EncodedMediaFrame::datagram_send_plan(SourceFirst)` and
      `scheduled_datagram_send_plan` let callers send source symbols before
      lower-priority repair.
- [x] Per-frame pacing and admission caps:
      `MediaSendPolicy` bounds each pass and in-flight work while adding
      per-datagram pacing offsets.
- [x] Absolute deadline-aware send order:
      `plan_media_datagrams_with_deadlines` applies strict source-first ordering,
      explicit initialization/config/keyframe/audio/delta importance, and
      primary-source/secondary-repair path intent.
- [x] Deadline-based recovery choice:
      `MediaRecoveryPolicy` chooses bounded extra RaptorQ, reliable fetch, or
      expiry from the remaining deadline and path RTT/fetch estimates.
- [x] Stale delta-repair dropping:
      the scheduler drops delta repair under configured queue pressure or missed
      deadline while keeping newer audio/keyframe work ahead.
- [x] Real metric ingestion:
      `NetworkMetricsObservation` feeds sequence loss, RTT, jitter, queue delay,
      and available bitrate into `AdaptiveFecController`.
- [x] Repair-effectiveness counters:
      `MediaFecRepairCounters` records source symbols, repair symbols, repaired
      source loss, unused repair, FEC overhead, failed blocks, send-plan drops,
      recovery choices, backfill hit/miss counts, and delivery deadline outcomes.
- [x] Backfill beside FEC:
      `MediaBackfillStore` keeps recent encoded datagrams as `Bytes` so a
      reliable path can request full frames or specific missing datagrams after
      parity is exhausted.
- [x] Reusable datagram buffers:
      `DatagramBufferPool`, `encode_*_reusing`, and
      `MediaFecEncoder::encode_frame_reusing` let callers recycle datagram
      storage between frames.
- [x] Carrier-neutral integration:
      scheduler, telemetry, recovery, and backfill hooks map onto UDP, QUIC
      Datagram, or another paced carrier while the FEC wire format stays stable.

## Publishing

After GitHub authentication is available:

```sh
gh repo create wavey-ai/raptor-fec --public --source . --remote origin --push
cargo publish -p raptorq-datagram-fec
cargo publish -p raptorq-fec-transport
```
