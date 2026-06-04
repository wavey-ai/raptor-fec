use crate::{EncodedMediaFrame, MediaFecFrameStats, MediaFrameMetadata};
use bytes::Bytes;
use std::collections::VecDeque;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MediaBackfillKey {
    pub stream_id: u64,
    pub sequence: u64,
}

impl From<MediaFrameMetadata> for MediaBackfillKey {
    fn from(metadata: MediaFrameMetadata) -> Self {
        Self {
            stream_id: metadata.stream_id,
            sequence: metadata.sequence,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaBackfillDatagram {
    pub datagram_index: usize,
    pub bytes: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MediaBackfillRequest {
    Frame {
        key: MediaBackfillKey,
    },
    Datagrams {
        key: MediaBackfillKey,
        datagram_indices: Vec<usize>,
    },
}

impl MediaBackfillRequest {
    pub fn key(&self) -> MediaBackfillKey {
        match self {
            Self::Frame { key } | Self::Datagrams { key, .. } => *key,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaBackfillResponse {
    pub key: MediaBackfillKey,
    pub datagrams: Vec<MediaBackfillDatagram>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaBackfillFrame {
    pub key: MediaBackfillKey,
    pub metadata: MediaFrameMetadata,
    pub stats: MediaFecFrameStats,
    datagrams: Vec<Bytes>,
}

impl MediaBackfillFrame {
    pub fn datagram_count(&self) -> usize {
        self.datagrams.len()
    }

    pub fn datagram(&self, index: usize) -> Option<Bytes> {
        self.datagrams.get(index).cloned()
    }

    pub fn datagrams(&self) -> Vec<MediaBackfillDatagram> {
        self.datagrams
            .iter()
            .cloned()
            .enumerate()
            .map(|(datagram_index, bytes)| MediaBackfillDatagram {
                datagram_index,
                bytes,
            })
            .collect()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MediaBackfillStore {
    capacity_frames: usize,
    frames: VecDeque<MediaBackfillFrame>,
}

impl Default for MediaBackfillStore {
    fn default() -> Self {
        Self::new(128)
    }
}

impl MediaBackfillStore {
    pub fn new(capacity_frames: usize) -> Self {
        Self {
            capacity_frames: capacity_frames.max(1),
            frames: VecDeque::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.frames.len()
    }

    pub fn is_empty(&self) -> bool {
        self.frames.is_empty()
    }

    pub fn capacity_frames(&self) -> usize {
        self.capacity_frames
    }

    pub fn insert_encoded_frame(&mut self, frame: &EncodedMediaFrame) {
        let key = MediaBackfillKey::from(frame.metadata);
        if let Some(index) = self
            .frames
            .iter()
            .position(|candidate| candidate.key == key)
        {
            self.frames.remove(index);
        }

        self.frames.push_back(MediaBackfillFrame {
            key,
            metadata: frame.metadata,
            stats: frame.stats(),
            datagrams: frame
                .datagrams
                .iter()
                .map(|datagram| Bytes::copy_from_slice(datagram))
                .collect(),
        });

        while self.frames.len() > self.capacity_frames {
            self.frames.pop_front();
        }
    }

    pub fn get(&self, key: MediaBackfillKey) -> Option<&MediaBackfillFrame> {
        self.frames.iter().find(|frame| frame.key == key)
    }

    pub fn fulfill(&self, request: &MediaBackfillRequest) -> Option<MediaBackfillResponse> {
        let frame = self.get(request.key())?;
        let datagrams = match request {
            MediaBackfillRequest::Frame { .. } => frame.datagrams(),
            MediaBackfillRequest::Datagrams {
                datagram_indices, ..
            } => datagram_indices
                .iter()
                .filter_map(|datagram_index| {
                    frame
                        .datagram(*datagram_index)
                        .map(|bytes| MediaBackfillDatagram {
                            datagram_index: *datagram_index,
                            bytes,
                        })
                })
                .collect(),
        };

        if datagrams.is_empty() {
            None
        } else {
            Some(MediaBackfillResponse {
                key: frame.key,
                datagrams,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MediaCodec, MediaFecEncoder, MediaFrame, MediaFrameMetadata};

    #[test]
    fn store_fulfills_frame_and_specific_datagram_backfill_requests() {
        let mut encoder = MediaFecEncoder::default();
        let encoded = encode_frame(&mut encoder, 1);
        let key = MediaBackfillKey::from(encoded.metadata);
        let mut store = MediaBackfillStore::new(2);
        store.insert_encoded_frame(&encoded);

        let response = store
            .fulfill(&MediaBackfillRequest::Datagrams {
                key,
                datagram_indices: vec![0, encoded.datagrams.len() - 1, 99_999],
            })
            .expect("backfill hit");
        assert_eq!(response.key, key);
        assert_eq!(response.datagrams.len(), 2);
        assert_eq!(
            response.datagrams[0].bytes,
            Bytes::copy_from_slice(&encoded.datagrams[0])
        );

        let full = store
            .fulfill(&MediaBackfillRequest::Frame { key })
            .expect("full frame");
        assert_eq!(full.datagrams.len(), encoded.datagrams.len());
    }

    #[test]
    fn store_evicts_old_frames_by_capacity() {
        let mut encoder = MediaFecEncoder::default();
        let first = encode_frame(&mut encoder, 1);
        let second = encode_frame(&mut encoder, 2);
        let mut store = MediaBackfillStore::new(1);
        store.insert_encoded_frame(&first);
        store.insert_encoded_frame(&second);

        assert!(store.get(MediaBackfillKey::from(first.metadata)).is_none());
        assert!(store.get(MediaBackfillKey::from(second.metadata)).is_some());
    }

    fn encode_frame(encoder: &mut MediaFecEncoder, sequence: u64) -> EncodedMediaFrame {
        let payload = vec![sequence as u8; 2_400];
        let metadata = MediaFrameMetadata::new(9, sequence, sequence * 33, MediaCodec::H264);
        encoder
            .encode_frame(MediaFrame {
                metadata,
                payload: &payload,
            })
            .expect("encode")
    }
}
