# music-audio-session

Exact-or-missing music audio session primitives over RaptorQ micro-block FEC.

This crate is the caller layer above `raptorq-datagram-fec`'s
`MusicAudioMicroBlockEncoder` and `MusicAudioMicroBlockDecoder`. It owns the
sample-clock sequence, groups captured chunks into FEC micro-blocks, decodes
incoming datagrams, and exposes a playout buffer that returns either exact audio
bytes or an explicit missing-frame result.

It deliberately does not open sockets, bind WebRTC, talk to audio devices, or
perform PLC. Transport adapters and audio callbacks should sit above this crate.

## Production status

This crate proves exact reconstruction and explicit missing-frame behavior. It
is not the selected production transport for lowest-latency PCM.

- Both presets accumulate 10 ms before emitting a RaptorQ block.
- The 5 ms preset uses 2,048-byte symbols and produces 2,084-byte RQD2
  datagrams before outer transport overhead.
- The preset configs do not define PCM sample representation or bit depth.
- `playout_delay_samples` is not currently an absolute deadline scheduler.
- The decoder releases completed RaptorQ objects rather than independently
  placing intact systematic PCM shards.

Wavey's performance-first design uses TCP/TLS as the sustained-throughput and
recording baseline, and paced UDP/RTP with immediate source shards plus
same-epoch XOR or small Reed-Solomon for native fixed-deadline PCM. RaptorQ is
retained for large-object/backfill work and a measured high-channel,
same-epoch experiment.

See
[`../../docs/pcm-low-latency-transport.md`](../../docs/pcm-low-latency-transport.md)
for the decision, packet-rate calculations, current API audit, and required
benchmark.
