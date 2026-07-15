# PCM transport and RaptorQ suitability

Date: 2026-07-14

## Decision

RaptorQ is not the default FEC for Wavey's live PCM hot path.

Use the following performance-first hierarchy:

1. TCP/TLS is the sustained-throughput baseline for clean-path bulk PCM,
   recording, and reliable backfill.
2. Paced UDP, or RTP/SRTP over UDP, is the native fixed-deadline PCM media
   plane on lossy paths.
3. Immediate duplicate UDP/RTP over two genuinely disjoint paths is the
   lowest-recovery-latency mode when 100% bandwidth overhead is acceptable.
4. On one path, use same-epoch XOR for one-erasure repair or small systematic
   Reed-Solomon for multiple-erasure repair.
5. WebSocket/HTTP streaming is the browser throughput baseline. WebTransport
   is a browser low-latency datagram experiment, not the assumed bulk PCM
   winner.
6. Retain RaptorQ for video, large recording/backfill objects, and one measured
   high-`k` PCM experiment where 64/128 channels create a large block inside a
   single already-captured epoch.

This is a choice of application framing and repair policy over existing Layer 4
transports. It is not a proposal to create a new IP transport protocol.

## Fastest fixed-deadline PCM path

For native endpoints with admitted or reserved bandwidth, the minimum-latency
media path is:

```text
audio callback -> preallocated handoff -> MTU-sized source shards -> paced UDP
                                                     |-> duplicate path A
                                                     `-> duplicate path B
receiver -> authenticate -> first-arrival merge/FEC -> strict playout deadline
```

Requirements:

- Packetize an integer number of source samples. Do not rebuffer merely to make
  a round millisecond duration.
- Give every systematic source shard enough sequence, source-sample PTS,
  channel-group, shard-index, and exact-length metadata to be independently
  placed at the receiver.
- Transmit source shards immediately. FEC must not delay the lossless path while
  waiting to fill a temporal block.
- Derive shard size from the path/transport maximum. Avoid IP fragmentation.
- Pace at the admitted PCM plus repair rate. An unpaced sender can create the
  queue and loss that violate its own deadline.
- Keep allocation, locks, socket I/O, and FEC matrix work off the real-time audio
  callback.
- Use deadline-aware retransmission only when the estimated resend arrival plus
  a safety margin is earlier than playout. It is supplementary, not the
  sub-RTT recovery primitive.

RTP/UDP has essentially the same hot-path behavior as a compact custom UDP
header and already defines sequence and sample-clock timestamp semantics. A raw
Wavey header should replace RTP only if measured overhead or required channel
semantics justify it.

Path-diverse duplication has no feedback or repair-computation delay: the
receiver uses the first authenticated copy. On a single path, proactive XOR or
Reed-Solomon makes repair available without waiting an RTT. Duplication and FEC
consume capacity; if the path cannot carry PCM plus repair with headroom, no
protocol preserves both the source bitrate and fixed deadline.

## Throughput and packet-rate constraint

At 48 kHz/24-bit, before packet and repair overhead:

| Channels | PCM bitrate | Bytes per 2.5 ms epoch | Approx. 1,150-byte shards |
|---:|---:|---:|---:|
| 2 | 2.304 Mbit/s | 720 | 1 |
| 16 | 18.432 Mbit/s | 5,760 | 5 |
| 64 | 73.728 Mbit/s | 23,040 | 21 |
| 128 | 147.456 Mbit/s | 46,080 | 41 |

At about 1,150 bytes of PCM per shard, 128-channel 48 kHz/24-bit audio is roughly
16,000 source datagrams per second before FEC. A 25% repair/packet allowance
raises that toward 20,000 datagrams per second and about 184 Mbit/s. At 96 kHz,
both bitrate and packet rate double.

The transport cannot remove this physical cost. Full many-channel browser PCM
must be treated as an explicit benchmark, not an assumed product capability.
Prefer a priority monitor bus plus subscribed channel groups/stems, and preserve
full-channel delivery for native clients and paths that pass admission.

## Why RaptorQ is usually a poor fit for live PCM

RaptorQ operates on arbitrary bytes; PCM entropy is not the issue. Its mismatch
is the block geometry and deadline of live audio.

### 1. It is a block/object code

RFC 6330 specifies RaptorQ for object delivery. Repair symbols are generated
from a completed source block. Enlarging a stereo PCM block by collecting future
audio epochs creates block-fill latency. For lowest latency, source shards must
leave immediately and repair must cover data already captured at the same
deadline.

### 2. Tiny blocks repeat non-trivial setup

For a source block of `k` symbols, RaptorQ internally extends it to the next
supported `K'`; the first value in RFC 6330's systematic-index table is
`K'=10`. A `k=1..8` audio block therefore repeats precode/matrix work sized for
the extended block. The padding is internal rather than additional source
datagrams, but the computation remains.

PCM creates hundreds or thousands of blocks per second. Per-block setup,
allocation, whole-block decode, and tail latency matter more than throughput on
one large object.

### 3. It is not MDS

RaptorQ usually reconstructs from `k` received encoding symbols, but RFC 6330
explicitly allows rare cases that need more. A strict audio deadline must send
additional safety repair or accept a decode tail.

Reed-Solomon is Maximum Distance Separable: any `k` received symbols from a
configured codeword recover the `k` sources. XOR deterministically repairs one
erasure. That bounded behavior is preferable for modest same-deadline blocks.

### 4. The fountain property is normally unused

RaptorQ can generate an arbitrary number of new repair symbols. That is valuable
when a sender can continue until a receiver has enough. A live PCM sender usually
chooses a fixed `r` before loss is known. On a path whose RTT exceeds the audio
deadline, repair requested after loss detection is late.

With fixed `k+r` transmission, RaptorQ behaves as a more complicated fixed-rate
block code without using its main advantage.

### 5. It couples recovery to a whole object

The current `DatagramFecDecoder` returns data only after the RaptorQ decoder
completes the object. Intact systematic shards cannot advance playout through
that API. One loss beyond the repair budget can therefore withhold the block even
though most source shards arrived.

A production PCM packetizer should place intact systematic shards immediately
and invoke FEC only to reconstruct missing positions.

### 6. It does not make burst loss free

RaptorQ cannot recover more lost shards than the useful repair symbols delivered
before deadline. A larger/interleaved temporal block can cover a longer burst,
but it does so by increasing block-fill or recovery latency. Path diversity or a
bounded same-epoch code is preferable when latency is the primary objective.

## Where RaptorQ may still fit PCM

A 64/128-channel epoch can produce tens of MTU-sized source shards without
waiting for a future epoch. That removes the temporal block-fill objection and
creates the only credible live-PCM RaptorQ case.

Benchmark RaptorQ against systematic Reed-Solomon at those exact shapes. It
enters a live profile only if all of the following are better or acceptably
equal:

- encode and decode CPU P50/P95/P99;
- deadline-miss tail under bounded random and burst loss;
- total wire bytes including safety repair;
- allocation count and peak working memory;
- intact systematic-shard delivery latency;
- performance at 64/128 channels and 48/96 kHz.

RaptorQ remains naturally suited to this repository's video access units and to
recording/backfill objects, where blocks are larger and recovery can continue
beyond a live audio deadline.

## Current audio preset limitations

The `MusicAudioMicroBlockConfig` presets are correctness prototypes, not a
production PCM wire profile:

- `pcm48_stereo_2_5ms` groups four 120-sample chunks and emits only after 10 ms.
- `pcm48_stereo_5ms` groups two 240-sample chunks and also emits after 10 ms.
- The 5 ms preset's 2,048-byte symbol becomes a 2,084-byte RQD2 datagram before
  UDP/QUIC/WebTransport overhead. It exceeds a typical 1,500-byte path without
  fragmentation and cannot fit one QUIC DATAGRAM on that path.
- The 2.5 ms preset's `4 source + 1 repair` result is fixture-dependent. The
  preset does not define sample representation or bit depth; S24 and F32 payloads
  produce different source-symbol counts.
- `MusicAudioSender::push_chunk` returns no source datagrams until the configured
  temporal block is full.
- `DatagramFecDecoder` exposes only completed objects.
- `MusicAudioSessionConfig::playout_delay_samples` is stored but does not drive
  an absolute network/playout deadline.
- The audio block metadata lacks sample representation, valid bits, byte order,
  channel-group mapping, interleaving, and clock identity.
- The encoder copies each chunk and allocates block/datagram vectors.
- The WebTransport crate is a wrapper over a caller-supplied `DatagramSender`;
  it is not an integrated PCM WebTransport implementation and does not enforce a
  live maximum datagram size.

Do not integrate these presets unchanged into a release PCM path. Preserve them
as exact-reconstruction tests while a source-first, MTU-aware PCM packetizer and
XOR/Reed-Solomon comparison are built.

## Required benchmark

Compare:

- TCP/TLS sustained PCM and recording throughput;
- paced UDP/RTP without FEC;
- disjoint-path duplication;
- same-epoch XOR;
- same-epoch small Reed-Solomon;
- current and source-first RaptorQ;
- deadline-aware retransmission;
- SRT and RIST;
- WebSocket/HTTP and WebTransport browser lanes.

Run 2/16/64/128 channels, S24/F32, 48/96 kHz, multiple integer epoch sizes, MTU
reduction, random loss, correlated bursts, reorder, bandwidth steps,
bufferbloat, route change, and outage. Measure capture-to-render latency,
unrecovered channel-group epochs, wire overhead, packet rate, CPU tails,
allocations, and exact recording completion after backfill.

## References

- [RFC 3550: RTP: A Transport Protocol for Real-Time Applications](https://www.rfc-editor.org/rfc/rfc3550.html)
- [RFC 4585: Extended RTP Profile for RTCP-Based Feedback](https://www.rfc-editor.org/rfc/rfc4585.html)
- [RFC 5510: Reed-Solomon Forward Error Correction Schemes](https://www.rfc-editor.org/rfc/rfc5510.html)
- [RFC 6330: RaptorQ Forward Error Correction Scheme for Object Delivery](https://www.rfc-editor.org/rfc/rfc6330.html)
- [RFC 7198: Duplicating RTP Streams](https://www.rfc-editor.org/rfc/rfc7198.html)
- [RFC 8085: UDP Usage Guidelines](https://www.rfc-editor.org/rfc/rfc8085.html)
- [RFC 9221: An Unreliable Datagram Extension to QUIC](https://www.rfc-editor.org/rfc/rfc9221.html)
