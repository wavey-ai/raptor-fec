# raptor-fec

Reusable RaptorQ forward-error-correction framing for low-latency datagram transports.

This repository contains two public crates:

- `raptorq-datagram-fec`: the wire protocol, RaptorQ block encoder/decoder, and optional Tokio UDP sender/receiver helpers.
- `raptorq-fec-transport`: transport-level wrappers for carrying the same FEC datagrams over WebTransport datagrams and WebRTC data channels.

The datagram wire format matches the UDP implementation that previously lived in `web-services/upload-response`:

```text
0               4               8              12
+---------------+---------------+---------------+
|   block_id    |transfer_length|src_syms|sym_sz |
+---------------+---------------+---------------+
|          RaptorQ EncodingPacket bytes ...      |
```

All integer fields in the 12-byte header are little-endian.

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
