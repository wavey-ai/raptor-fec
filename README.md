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

## Publishing

After GitHub authentication is available:

```sh
gh repo create wavey-ai/raptor-fec --public --source . --remote origin --push
cargo publish -p raptorq-datagram-fec
cargo publish -p raptorq-fec-transport
```
