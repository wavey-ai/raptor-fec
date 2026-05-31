# raptor-fec

Reusable RaptorQ forward-error-correction framing for low-latency datagram transports.

This repository contains two public crates:

- `raptor-udp-fec`: the wire protocol, RaptorQ block encoder/decoder, and optional Tokio UDP sender/receiver helpers.
- `raptor-fec-transport`: transport-level wrappers for carrying the same FEC datagrams over WebTransport datagrams and WebRTC data channels.

The UDP wire format matches the implementation that previously lived in `web-services/upload-response`:

```text
0               4               8              12
+---------------+---------------+---------------+
|   block_id    |transfer_length|src_syms|sym_sz |
+---------------+---------------+---------------+
|          RaptorQ EncodingPacket bytes ...      |
```

All integer fields in the 12-byte header are little-endian.

## Publishing

After GitHub authentication is available:

```sh
gh repo create wavey-ai/raptor-fec --public --source . --remote origin --push
cargo publish -p raptor-udp-fec
cargo publish -p raptor-fec-transport
```
