# raptor-fec

Reusable RaptorQ forward-error-correction framing for low-latency datagram transports.

This repository contains two public crates:

- `raptorq-datagram-fec`: the wire protocol, RaptorQ block encoder/decoder, media access-unit framing, adaptive repair policy, congestion state, and optional Tokio UDP sender/receiver helpers.
- `raptorq-fec-transport`: transport-level wrappers for carrying the same FEC datagrams over WebTransport datagrams and WebRTC data channels without adding a per-datagram stream prefix by default.

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

Media frames can still use the optional 44-byte protected fragment header above
the byte payload when a caller wants a `u64` stream id, access-unit sequence,
PTS/DTS delta, duration, keyframe/config flags, and fragment boundaries. The
RaptorQ datagram layer itself treats that as ordinary bytes.
`EncodedMediaFrame.blocks` exposes each protected media block's source-symbol
count, repair-symbol count, payload length, and source/repair datagram ranges so
callers can reason about recovery budget per fragment instead of inferring it
from private packet headers.

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

- keyframe recovery under burst and periodic datagram loss;
- delta-frame recovery under bounded random loss;
- fail-closed behavior when loss exceeds the configured repair budget;
- a deterministic payload-size sweep over tiny/small/large keyframes and delta
  frames, with front-burst, late-burst, periodic, and random source loss kept
  within each FEC block's repair budget, plus a fail-closed check one source
  datagram past the per-block repair budget;
- explicit source-symbol accounting per FEC block, so recoverable cases do not
  get credit for merely dropping repair packets or staying under a frame-level
  aggregate repair count;
- a 90-frame stream where RaptorQ FEC repairs keyframe and delta losses inside a
  33 ms playout budget while a RIST/SRT-style feedback retransmission model
  misses those frames at 70 ms RTT plus RIST's default 20 ms feedback interval;
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

## Publishing

After GitHub authentication is available:

```sh
gh repo create wavey-ai/raptor-fec --public --source . --remote origin --push
cargo publish -p raptorq-datagram-fec
cargo publish -p raptorq-fec-transport
```
