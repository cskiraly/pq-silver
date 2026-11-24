//! What the node sends, as gossipsub frames in the outgoing tcache: a chunk as
//! a message on the chunk topic, the strings as the ids of one IHAVE, IWANT
//! and IDONTWANT frame each per drain of a peer's queue.

use buffa::{
    encoding::{Tag, WireType, encode_varint, varint_len},
    types::{bytes_encoded_len, encode_bytes, encode_string, string_encoded_len},
};
use seacast_codec::ChunkBytes;
use seacast_core::ChunkMsg;
use seacast_gossipsub::RpcOut;
use silver_common::{
    Error, GossipMsgOut, GossipTraffic, TCacheProducer, TCacheRead, TCacheReader, TProducer,
};

use super::store::{Buf, Store};
use crate::{GossipHandlerEvent, field_budget::MAX_RPC_FIELDS, message};

/// `ControlMessage` fields.
const IHAVE: u32 = 1;
const IWANT: u32 = 2;
const IDONTWANT: u32 = 5;
/// A frame's fields besides its ids: control, the entry, the topic.
const MAX_IDS: usize = MAX_RPC_FIELDS as usize - 3;

/// The strings of one kind waiting for their frame.
#[derive(Default)]
pub(super) struct Strings {
    bytes: Vec<u8>,
    ends: Vec<u32>,
}

impl Strings {
    fn push(&mut self, entry: &[u8]) {
        self.bytes.extend_from_slice(entry);
        self.ends.push(self.bytes.len() as u32);
    }

    fn clear(&mut self) {
        self.bytes.clear();
        self.ends.clear();
    }

    fn iter(&self) -> impl Iterator<Item = &[u8]> + Clone {
        let starts = std::iter::once(0).chain(self.ends.iter().copied());
        starts.zip(&self.ends).map(|(start, &end)| &self.bytes[start as usize..end as usize])
    }
}

pub(super) struct Scratch {
    ihave: Strings,
    iwant: Strings,
    idontwant: Strings,
    /// A chunk's frame gathered, then its snappy.
    frame: Vec<u8>,
    snappy: Vec<u8>,
    encoder: snap::raw::Encoder,
}

impl Default for Scratch {
    fn default() -> Self {
        Self {
            ihave: Strings::default(),
            iwant: Strings::default(),
            idontwant: Strings::default(),
            frame: Vec::new(),
            snappy: Vec::new(),
            encoder: snap::raw::Encoder::new(),
        }
    }
}

/// The frames to one peer.
pub(super) struct Out<'a, F> {
    pub(super) peer: usize,
    pub(super) traffic: GossipTraffic,
    pub(super) topic: &'a str,
    pub(super) producer: &'a mut TProducer,
    pub(super) reader: &'a mut TCacheReader,
    pub(super) store: &'a mut Store,
    pub(super) scratch: &'a mut Scratch,
    pub(super) emit: &'a mut F,
}

impl<F: FnMut(GossipHandlerEvent)> Out<'_, F> {
    fn send(&mut self, tcache: TCacheRead, traffic: GossipTraffic) {
        (self.emit)(GossipHandlerEvent::SendGossip(GossipMsgOut {
            peer_id: self.peer,
            tcache,
            traffic,
        }));
    }

    /// The chunk's message: written once, pinned, and sent again from there.
    fn chunk_frame(&mut self, frame: &Buf) -> Option<TCacheRead> {
        if let Some(pinned) = self.store.wire(frame) {
            return Some(pinned.to_read());
        }
        let s = &mut *self.scratch;
        s.frame.clear();
        s.frame.extend_from_slice(frame.as_bytes());
        s.frame.extend_from_slice(frame.tail());
        s.snappy.resize(snap::raw::max_compress_len(s.frame.len()), 0);
        let len = s.encoder.compress(&s.frame, &mut s.snappy).ok()?;
        let read = message::copy_compressed_to_protobuf_output(
            self.producer,
            &s.snappy[..len],
            self.topic,
        )
        .ok()?;
        *self.store.wire(frame) = Some(self.reader.acquire(read));
        Some(read)
    }

    /// Writes the strings gathered since the last call.
    pub(super) fn flush(&mut self) {
        let s = &mut *self.scratch;
        let kinds = [
            (IHAVE, Some(self.topic), &s.ihave, GossipTraffic::IHAVE),
            (IWANT, None, &s.iwant, GossipTraffic::IWANT),
            (IDONTWANT, None, &s.idontwant, GossipTraffic::IDONTWANT),
        ];
        let mut frames = [(None, GossipTraffic::CONTROL); 3];
        for ((field, topic, strings, traffic), frame) in kinds.into_iter().zip(&mut frames) {
            if !strings.ends.is_empty() {
                *frame = (control_frame(self.producer, field, topic, strings.iter()).ok(), traffic);
            }
        }
        s.ihave.clear();
        s.iwant.clear();
        s.idontwant.clear();
        for (read, traffic) in frames {
            if let Some(read) = read {
                self.send(read, traffic);
            }
        }
    }
}

impl<F: FnMut(GossipHandlerEvent)> RpcOut<Buf> for Out<'_, F> {
    fn message(&mut self, chunk: &ChunkMsg<Buf>) {
        // A chunk of an assembly freed since it was queued is not sent.
        if !self.store.live(&chunk.frame) {
            return;
        }
        if let Some(read) = self.chunk_frame(&chunk.frame) {
            self.send(read, self.traffic);
        }
    }

    fn ihave(&mut self, entry: &[u8]) {
        self.scratch.ihave.push(entry);
    }

    fn iwant(&mut self, entry: &[u8]) {
        self.scratch.iwant.push(entry);
    }

    fn idontwant(&mut self, entry: &[u8]) {
        self.scratch.idontwant.push(entry);
    }
}

/// `RPC { control { <field>: [ { topic_id?, message_ids } ] } }` with ids of
/// any length. In an IHAVE the ids are field 2, after the topic; in an IWANT
/// and an IDONTWANT they are field 1. Ids past the frame's field budget are
/// left out.
fn control_frame<'a>(
    producer: &mut TProducer,
    field: u32,
    topic: Option<&str>,
    ids: impl Iterator<Item = &'a [u8]> + Clone,
) -> Result<TCacheRead, Error> {
    let ids = ids.take(MAX_IDS);
    let id_field = if topic.is_some() { 2 } else { 1 };
    let entry_len = topic.map_or(0, |topic| 1 + string_encoded_len(topic)) +
        ids.clone().map(|id| 1 + bytes_encoded_len(id)).sum::<usize>();
    let control_len = 1 + varint_len(entry_len as u64) + entry_len;
    let rpc_len = 1 + varint_len(control_len as u64) + control_len;
    let total = varint_len(rpc_len as u64) + rpc_len;

    let mut reservation = producer.reserve(total, true).ok_or(Error::BufferTooSmall)?;
    let out = producer.reservation_buffer(&mut reservation)?;
    let mut cursor: &mut [u8] = &mut out[..total];
    encode_varint(rpc_len as u64, &mut cursor);
    Tag::new(3, WireType::LengthDelimited).encode(&mut cursor);
    encode_varint(control_len as u64, &mut cursor);
    Tag::new(field, WireType::LengthDelimited).encode(&mut cursor);
    encode_varint(entry_len as u64, &mut cursor);
    if let Some(topic) = topic {
        Tag::new(1, WireType::LengthDelimited).encode(&mut cursor);
        encode_string(topic, &mut cursor);
    }
    for id in ids {
        Tag::new(id_field, WireType::LengthDelimited).encode(&mut cursor);
        encode_bytes(id, &mut cursor);
    }
    reservation.increment_offset(total);
    Ok(reservation.read())
}

#[cfg(test)]
mod tests {
    use buffa::{MessageView, encoding::decode_varint};
    use silver_common::{TCache, TCacheId};

    use super::*;
    use crate::generated::RPCView;

    #[test]
    fn control_frames_parse_back() {
        let mut producer = TCache::producer(TCacheId::ControlGossip, 1 << 16);
        let topic = "/eth2/00000000/execution_payload_segment/ssz_snappy";
        let ids: [&[u8]; 3] = [&[1; 43], &[2; 20], &[3; 200]];
        for (field, with_topic) in [(IHAVE, Some(topic)), (IWANT, None), (IDONTWANT, None)] {
            let read = control_frame(&mut producer, field, with_topic, ids.into_iter()).unwrap();
            let mut bytes = producer.read_buffer(read).unwrap();
            let len = decode_varint(&mut bytes).unwrap() as usize;
            assert_eq!(bytes.len(), len);
            let rpc = RPCView::decode_view(bytes).unwrap();
            let control = rpc.control.as_option().unwrap();
            let got: Vec<&[u8]> = match field {
                IHAVE => {
                    let ihave = (&control.ihave).into_iter().next().unwrap();
                    assert_eq!(ihave.topic_id, Some(topic));
                    (&ihave.message_ids).into_iter().copied().collect()
                }
                IWANT => {
                    let iwant = (&control.iwant).into_iter().next().unwrap();
                    (&iwant.message_ids).into_iter().copied().collect()
                }
                _ => {
                    let idontwant = (&control.idontwant).into_iter().next().unwrap();
                    (&idontwant.message_ids).into_iter().copied().collect()
                }
            };
            assert_eq!(got, ids);
        }
    }
}
