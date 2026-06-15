# music-audio-session

Exact-deadline music audio session primitives over RaptorQ micro-block FEC.

This crate is the caller layer above `raptorq-datagram-fec`'s
`MusicAudioMicroBlockEncoder` and `MusicAudioMicroBlockDecoder`. It owns the
sample-clock sequence, groups captured chunks into FEC micro-blocks, decodes
incoming datagrams, and exposes a playout buffer that returns either exact audio
bytes or an explicit missing-frame result.

It deliberately does not open sockets, bind WebRTC, talk to audio devices, or
perform PLC. Transport adapters and audio callbacks should sit above this crate.
