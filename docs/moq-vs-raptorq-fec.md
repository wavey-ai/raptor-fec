# MoQ QUIC vs RaptorQ-FEC for av-contrib

Date: 2026-06-04

## Summary

MoQ and `raptorq-datagram-fec` solve different layers of the live media problem.
MoQ is a QUIC/WebTransport pub-sub transport with relay fanout, stream
prioritization, caching, TLS, congestion control, and retransmission. Our
RaptorQ-FEC layer is a bounded-loss repair codec for datagram transports, used
by `av-contrib` to forward contributor bytes and media access units into
`av-mesh`.

For `av-contrib`, RaptorQ-FEC remains the better hot-path recovery mechanism
when the playout budget is below the RTT or QUIC PTO. It can reconstruct a
frame from repair symbols without waiting for feedback. MoQ is stronger as a
browser/CDN-facing live transport and fanout protocol, but its loss recovery is
QUIC ARQ or group skip/reset, not forward repair.

The main improvement opportunity is to borrow MoQ's transport discipline:
priority, bounded in-flight groups, deadline-aware dropping, cache/backfill, and
zero-copy frame buffers. Our FEC works, but `av-contrib` currently sends encoded
datagrams in a simple loop and the default adaptive policy gives low-loss H.264
delta frames zero repair.

## Code Context

RaptorQ-FEC:

- `MediaFecEncoder::encode_frame` fragments access units into protected blocks,
  adds the 44-byte media fragment header, chooses repair per media priority, and
  emits datagrams per block:
  `crates/raptorq-datagram-fec/src/media.rs:357`.
- The adaptive policy chooses source/repair symbols from payload size, media
  priority, loss, jitter, and queue pressure:
  `crates/raptorq-datagram-fec/src/adaptive.rs:211`.
- The datagram encoder serializes RaptorQ packets with a 32-byte Wavey header,
  and the decoder completes a block as soon as the RaptorQ decoder has enough
  symbols:
  `crates/raptorq-datagram-fec/src/lib.rs:555` and
  `crates/raptorq-datagram-fec/src/lib.rs:613`.

av-contrib:

- Byte slots use a per-stream `FecDatagramEncoder` with an 8-byte stream prefix,
  then send every FEC datagram to the mesh UDP target:
  `/Users/jamie/wavey.ai/av-contrib/src/bin/av-contrib.rs:262`.
- Media access units use one shared `MediaFecEncoder`, then send every encoded
  media datagram to the mesh media-FEC target:
  `/Users/jamie/wavey.ai/av-contrib/src/bin/av-contrib.rs:318`.

MoQ:

- MoQ tracks are semi-reliable, semi-ordered groups. The source comments state
  that consumers may not receive all streams in order or at all:
  `/private/tmp/moq-dev-moq/rs/moq-net/src/model/track.rs:1`.
- In `moq-lite`, each group is served on a unidirectional QUIC stream, with a
  priority handle for track and group ordering:
  `/private/tmp/moq-dev-moq/rs/moq-net/src/lite/publisher.rs:430`.
- The publisher writes each frame size followed by frame bytes, and stream
  priority can be updated while writing:
  `/private/tmp/moq-dev-moq/rs/moq-net/src/lite/publisher.rs:512`.

## Benchmark Method

Temporary harness: `/private/tmp/av-moq-raptor-bench`

Command:

```sh
cargo run --release > /private/tmp/av-moq-raptor-bench/results.md
```

Environment:

- Machine: Apple M1, 8 logical CPUs
- OS: Darwin 25.5.0 arm64
- Rust: `rustc 1.89.0-nightly (8405332bd 2025-05-12)`
- Cargo: `cargo 1.89.0-nightly (056f5f4f3 2025-05-09)`
- `raptor-fec`: `0ab44ef6c37c2ec141b71a689ab861b0805f29c0`
- `av-contrib`: `52fa3154d4ba3eb84bd1e8bce778c27d07893ebb`
- `moq-dev/moq`: `0ebcabf16fa4bc9589f7fa8630bbb3af6cdeda01`

Scenarios:

- Audio: 1000 frames of 960-byte Opus-like payloads.
- Video delta: 500 frames of 18 KB H.264-like payloads.
- Video key: 160 frames of 64 KB H.264 keyframe-like payloads.
- MoQ loss: 60 18 KB frames through a UDP proxy with 2 ms one-way delay and
  every 100th server-to-client packet dropped after subscribe.

Caveats:

- RaptorQ results are local media-FEC encode/decode CPU plus FEC wire expansion.
  They do not include UDP socket cost.
- MoQ QUIC results include local raw QUIC loopback with `moq-lite-03`, but no
  application FEC. Direct MoQ wire bytes were not exposed by the API; only the
  proxy run reports packet bytes.
- In the raw benchmark table, RaptorQ `p50/p95/max` are delivered datagram counts
  until decode, not milliseconds. MoQ `p50/p95/max` are frame latency in ms.

## Results

| system | scenario | mode | frames rx/sent | app Mbps | wire Mbps | packets | dropped | elapsed ms | p50 | p95 | max | notes |
|---|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---|
| raptorq_media_fec | audio_960B | no_loss | 1000/1000 | 27.0 | 76.2 | 2000 | 0 | 284.1 | 1.000 | 1.000 | 1.000 | all source and repair datagrams delivered |
| raptorq_media_fec | audio_960B | source_loss_within_repair_budget | 1000/1000 | 12.6 | 35.6 | 2000 | 1000 | 607.8 | 1.000 | 1.000 | 1.000 | dropped source datagrams up to each encoded block's repair budget |
| raptorq_media_fec | audio_960B | source_loss_beyond_repair_budget | 0/1000 | 25.0 | 70.3 | 2000 | 2000 | 307.6 | n/a | n/a | n/a | dropped one more source datagram than each encoded block's repair budget |
| moq_net_model | audio_960B | in_process_group_per_frame | 1000/1000 | 5424.4 | n/a | n/a | n/a | 1.4 | 0.001 | 0.002 | 0.144 | moq-net producer/consumer model only, no QUIC socket |
| moq_native_quic | audio_960B | raw_quic_loopback | 1000/1000 | 141.4 | n/a | n/a | n/a | 54.3 | 13.068 | 16.087 | 20.273 | raw QUIC loopback, no injected loss |
| raptorq_media_fec | video_delta_18KB | no_loss | 500/500 | 193.9 | 203.9 | 7000 | 0 | 371.4 | 14.000 | 14.000 | 14.000 | all source and repair datagrams delivered |
| raptorq_media_fec | video_delta_18KB | source_loss_within_repair_budget | 500/500 | 191.7 | 201.6 | 7000 | 0 | 375.5 | 14.000 | 14.000 | 14.000 | dropped source datagrams up to each encoded block's repair budget |
| raptorq_media_fec | video_delta_18KB | source_loss_beyond_repair_budget | 0/500 | 104.0 | 109.3 | 7000 | 500 | 692.5 | n/a | n/a | n/a | dropped one more source datagram than each encoded block's repair budget |
| moq_net_model | video_delta_18KB | in_process_group_per_frame | 500/500 | 31169.4 | n/a | n/a | n/a | 2.3 | 0.004 | 0.007 | 0.063 | moq-net producer/consumer model only, no QUIC socket |
| moq_native_quic | video_delta_18KB | raw_quic_loopback | 500/500 | 593.8 | n/a | n/a | n/a | 121.2 | 14.727 | 23.912 | 24.085 | raw QUIC loopback, no injected loss |
| raptorq_media_fec | video_key_64KB | no_loss | 160/160 | 73.8 | 90.5 | 9280 | 0 | 1109.4 | 57.000 | 57.000 | 57.000 | all source and repair datagrams delivered |
| raptorq_media_fec | video_key_64KB | source_loss_within_repair_budget | 160/160 | 78.6 | 96.2 | 9280 | 1440 | 1042.9 | 49.000 | 49.000 | 49.000 | dropped source datagrams up to each encoded block's repair budget |
| raptorq_media_fec | video_key_64KB | source_loss_beyond_repair_budget | 0/160 | 107.9 | 132.2 | 9280 | 1760 | 759.4 | n/a | n/a | n/a | dropped one more source datagram than each encoded block's repair budget |
| moq_net_model | video_key_64KB | in_process_group_per_frame | 160/160 | 63813.0 | n/a | n/a | n/a | 1.3 | 0.008 | 0.010 | 0.063 | moq-net producer/consumer model only, no QUIC socket |
| moq_native_quic | video_key_64KB | raw_quic_loopback | 160/160 | 636.7 | n/a | n/a | n/a | 128.7 | 6.071 | 10.491 | 12.114 | raw QUIC loopback, no injected loss |
| moq_native_quic | video_delta_18KB_lossy_proxy | raw_quic_proxy_loss | 60/60 | 5.3 | 6.0 | 1501 | 10 | 1634.3 | 796.392 | 1634.051 | 1634.066 | raw QUIC via UDP proxy, 2 ms one-way delay, every 100th server-to-client packet dropped after subscribe |

## Interpretation

Throughput:

- MoQ raw QUIC loopback moved application bytes faster than the RaptorQ media
  FEC encode/decode loop in these local runs: 141 Mbps vs 27 Mbps for small
  audio, 594 Mbps vs 194 Mbps for 18 KB deltas, and 637 Mbps vs 74 Mbps for
  64 KB keyframes.
- That does not mean MoQ is a faster replacement for FEC. The MoQ run did no
  RaptorQ encoding, no FEC header generation, and no forward repair. It mostly
  measured QUIC stream movement on loopback.
- RaptorQ's wire overhead is payload-dependent. Audio was expensive at about
  2.8x wire/app bytes because each 960-byte payload became one source symbol
  plus one repair symbol. Delta video at default low-loss settings had about
  1.05x overhead but no repair. The 64 KB keyframe case had about 1.23x overhead
  and recovered from nine source datagram losses per frame.

Latency:

- RaptorQ recovery latency is bounded by block fill time and repair datagram
  arrival, not by RTT. In the keyframe loss case, frames reconstructed after
  49 delivered datagrams even with 9 dropped datagrams per frame.
- MoQ no-loss local latency was low on loopback, but the lossy proxy run shows
  the core tradeoff: QUIC recovered all 60 frames eventually, but 10 dropped
  server-to-client packets produced a 796 ms p50 and 1.63 s p95/tail on this
  harness. That is fine for eventual delivery, poor for a 33 ms playout budget.
- MoQ's own model is designed to skip or starve stale groups under congestion.
  That protects freshness but is not equivalent to reconstructing a damaged
  access unit.

Loss recovery:

- RaptorQ succeeded exactly when loss stayed within repair budget and failed
  closed beyond it.
- The default policy gave 18 KB H.264 delta frames zero repair at zero reported
  loss, so the within-repair test had no source drop to apply. One dropped source
  datagram then failed. This is the biggest current mismatch with the stated
  low-latency mesh goal.
- MoQ recovered the lossy proxy run through QUIC retransmission, but recovery
  latency followed the retransmission path rather than forward repair.

## Strengths and Weaknesses

### RaptorQ-FEC

Strengths:

- Recovers bounded source datagram loss without feedback, which is the right
  property for low-latency `av-contrib` to `av-mesh` ingest.
- Works over UDP, WebTransport datagrams, or WebRTC data channels because the
  FEC layer is transport-independent.
- Media-aware framing already distinguishes audio, video keyframes, and deltas.
- Fail-closed behavior is clear once loss exceeds the configured budget.

Weaknesses:

- CPU cost and allocation cost are visible. The local FEC encode/decode path is
  much slower than MoQ's in-memory model and slower than no-FEC QUIC loopback.
- Small audio payloads pay high overhead when protected one-for-one.
- The default adaptive state has no real network metrics in `av-contrib`, so
  low-loss delta video can get zero repair.
- No transport scheduler: `av-contrib` encodes a frame or slot and immediately
  sends every datagram in order.
- No built-in fanout/cache/backfill plane. Forward repair alone cannot cover
  sustained loss beyond repair budget.

### MoQ

Strengths:

- Strong transport substrate: QUIC congestion control, encryption, stream
  multiplexing, independent stream loss domains, and browser WebTransport.
- Good relay model for fanout, caching, subscription, late join, and priority.
- Group-per-stream model maps well to GoP/keyframe boundaries and lets old
  groups be skipped without corrupting decoder state.
- In-process model is very efficient due to preallocated frame buffers and cheap
  producer/consumer clones.

Weaknesses:

- Loss recovery is retransmission or skipping, not forward repair. If playout is
  below RTT/PTO, a lost packet can miss deadline even though QUIC eventually
  repairs it.
- Relay-level partial reliability drops groups, which preserves freshness but
  sacrifices the affected media rather than reconstructing it.
- Requires QUIC/TLS/WebTransport machinery, which is heavier than UDP datagrams
  for an internal mesh hot path.
- Publisher code needs careful in-flight control for large groups. In the first
  harness attempt, finishing a track while large group streams were still active
  truncated streams with `wrong frame size`. A bounded in-flight window fixed
  the benchmark.

## Recommendations for RaptorQ-FEC

1. Add a priority/deadline send scheduler to `av-contrib`.
   Send audio, codec config, and keyframe source symbols before delta repair.
   Drop stale delta repair before it competes with newer keyframes.

2. Bound in-flight FEC datagrams per stream.
   Borrow MoQ's group concurrency idea. Large keyframes should not dump all
   source and repair datagrams into the socket without pacing or backpressure.

3. Make the media policy deadline-aware.
   Use target playout latency, observed RTT, jitter, and frame type to decide
   whether to spend bytes on FEC, drop deltas, or request/backfill keyframes.

4. Change the default delta-frame floor.
   In `av-contrib`, absent real metrics, give video deltas at least one repair
   symbol above a size threshold, or initialize the controller from conservative
   mesh loss assumptions. The benchmark showed 18 KB deltas had no repair at
   default zero-loss metrics.

5. Reduce audio overhead.
   Options: smaller symbols for audio, grouping several audio frames into one
   protected block when latency allows, or using a lower audio repair floor when
   Opus in-band FEC or concealment is already available.

6. Add a backfill plane beside FEC.
   Keep recent encoded blocks or source payloads and support explicit missing
   block repair over QUIC/WebTransport/HTTP. FEC handles the 33 ms hot path;
   backfill handles loss beyond budget.

7. Reuse buffers and move toward `Bytes`/scatter-gather output.
   MoQ's frame path avoids extra intermediate allocations. RaptorQ currently
   serializes each encoding packet into a new `Vec` and then wraps it with a new
   datagram buffer. A reusable datagram buffer pool should improve throughput.

8. Expose repair effectiveness metrics.
   Add counters for source symbols, repair symbols, source drops repaired,
   repair symbols unused, decode deadline misses, and per-priority FEC overhead.
   Feed those back into `AdaptiveFecController`.

9. Consider MoQ as an optional contributor or distribution protocol, not a mesh
   FEC replacement.
   For browsers and CDN fanout, MoQ is a good fit. For `av-contrib` to mesh
   ingest under bounded WAN loss and tight playout, keep RaptorQ-FEC and improve
   its scheduler/adaptation.

