use js_sys::{Array, Object, Reflect, Uint8Array};
use music_audio_session::{
    DecodedMultichannelAudioGroup, MultichannelAudioReceiver, MultichannelAudioSessionConfig,
};
use raptorq_fec_transport::strip_audio_epoch_prefix;
use wasm_bindgen::prelude::*;

#[wasm_bindgen]
pub struct WasmMultichannelAudioReceiver {
    receiver: MultichannelAudioReceiver,
}

#[wasm_bindgen]
impl WasmMultichannelAudioReceiver {
    #[wasm_bindgen(constructor)]
    pub fn new() -> Self {
        Self {
            receiver: MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default()),
        }
    }

    /// Returns each channel/stem group that became exact while processing the
    /// datagram. Systematic groups are returned without waiting for parity.
    #[wasm_bindgen(js_name = pushDatagram)]
    pub fn push_datagram(&mut self, datagram: &[u8]) -> Result<Array, JsValue> {
        let datagram = strip_audio_epoch_prefix(datagram)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let outcome = self
            .receiver
            .push_datagram(datagram)
            .map_err(|error| JsValue::from_str(&error.to_string()))?;
        let groups = Array::new();
        for group in outcome.completed_groups {
            groups.push(&group_to_js(group)?.into());
        }
        Ok(groups)
    }

    #[wasm_bindgen(js_name = expireBefore)]
    pub fn expire_before(&mut self, pts_samples: u64) -> usize {
        self.receiver.expire_before(pts_samples)
    }

    pub fn reset(&mut self) {
        self.receiver = MultichannelAudioReceiver::new(MultichannelAudioSessionConfig::default());
    }

    #[wasm_bindgen(js_name = sequenceStats)]
    pub fn sequence_stats(&self) -> Result<Object, JsValue> {
        let stats = self.receiver.sequence_stats();
        let value = Object::new();
        set(&value, "received", stats.received.into())?;
        set(&value, "missing", stats.missing.into())?;
        set(
            &value,
            "duplicateOrReordered",
            stats.duplicate_or_reordered.into(),
        )?;
        if let Some(highest_seen) = stats.highest_seen {
            set(&value, "highestSeen", highest_seen.into())?;
        }
        Ok(value)
    }
}

impl Default for WasmMultichannelAudioReceiver {
    fn default() -> Self {
        Self::new()
    }
}

fn group_to_js(group: DecodedMultichannelAudioGroup) -> Result<Object, JsValue> {
    let value = Object::new();
    set(&value, "sessionId", group.session_id.to_string().into())?;
    set(&value, "configGeneration", group.config_generation.into())?;
    set(&value, "epochId", group.epoch_id.to_string().into())?;
    set(&value, "ptsSamples", group.pts_samples.to_string().into())?;
    set(&value, "sampleRate", group.sample_rate.into())?;
    set(&value, "frameCount", group.frame_count.into())?;
    set(&value, "groupCount", group.group_count.into())?;
    set(&value, "groupId", group.group_id.into())?;
    set(&value, "groupIndex", group.group_index.into())?;
    set(&value, "channelStart", group.channel_start.into())?;
    set(&value, "channelCount", group.channel_count.into())?;
    set(&value, "payloadKind", (group.payload_kind as u8).into())?;
    set(&value, "sampleFormat", (group.sample_format as u8).into())?;
    set(&value, "flags", group.flags.into())?;
    set(
        &value,
        "raptorqRecoveredFragments",
        group.raptorq_recovered_fragments.into(),
    )?;
    set(
        &value,
        "payload",
        Uint8Array::from(group.payload.as_ref()).into(),
    )?;
    Ok(value)
}

fn set(object: &Object, key: &str, value: JsValue) -> Result<(), JsValue> {
    Reflect::set(object, &JsValue::from_str(key), &value).map(|_| ())
}

#[cfg(all(test, target_arch = "wasm32"))]
mod tests {
    use super::*;
    use music_audio_session::{MultichannelAudioSender, MultichannelAudioSessionConfig};
    use raptorq_datagram_fec::{
        AudioPayloadKind, AudioSampleFormat, MultichannelAudioDatagramRole, MultichannelAudioEpoch,
        MultichannelAudioFecConfig, MultichannelAudioGroup,
    };
    use raptorq_fec_transport::MultichannelAudioTransportAdapter;
    use wasm_bindgen_test::wasm_bindgen_test;

    #[wasm_bindgen_test]
    fn browser_binding_recovers_a_missing_s24_source_shard_exactly() {
        let payload = (0..(240 * 2 * 3))
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>();
        let groups = [MultichannelAudioGroup {
            group_id: 7,
            channel_start: 112,
            channel_count: 2,
            payload_kind: AudioPayloadKind::Pcm,
            sample_format: AudioSampleFormat::S24Le,
            flags: 0,
            payload: &payload,
        }];
        let transport = MultichannelAudioTransportAdapter::webtransport(1200);
        let fec = transport.prepare_fec_config(MultichannelAudioFecConfig {
            repair_symbols: 2,
            ..MultichannelAudioFecConfig::default()
        });
        let mut sender = MultichannelAudioSender::new(MultichannelAudioSessionConfig {
            fec,
            ..MultichannelAudioSessionConfig::default()
        });
        let encoded = sender
            .encode_epoch(MultichannelAudioEpoch {
                session_id: 19,
                config_generation: 3,
                epoch_id: 41,
                pts_samples: 48_000,
                sample_rate: 48_000,
                frame_count: 240,
                groups: &groups,
            })
            .unwrap();
        let encoded = transport.wrap_epoch(encoded).unwrap();

        let mut receiver = WasmMultichannelAudioReceiver::new();
        let mut skipped_source = false;
        let mut completed = None;
        for datagram in encoded.datagrams {
            if !skipped_source
                && matches!(datagram.role, MultichannelAudioDatagramRole::Source { .. })
            {
                skipped_source = true;
                continue;
            }
            let groups = receiver.push_datagram(&datagram.payload).unwrap();
            if groups.length() > 0 {
                completed = Some(groups.get(0));
            }
        }

        let completed = completed.expect("repair packets should complete the group");
        assert_eq!(
            Reflect::get(&completed, &JsValue::from_str("groupId"))
                .unwrap()
                .as_f64(),
            Some(7.0)
        );
        assert!(
            Reflect::get(&completed, &JsValue::from_str("raptorqRecoveredFragments"))
                .unwrap()
                .as_f64()
                .unwrap()
                > 0.0
        );
        let recovered =
            Uint8Array::new(&Reflect::get(&completed, &JsValue::from_str("payload")).unwrap())
                .to_vec();
        assert_eq!(recovered, payload);
    }
}
