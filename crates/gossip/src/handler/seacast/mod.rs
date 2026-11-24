//! SEACAST on gossipsub (`seacast-bindings.md` §4) for the execution payload
//! envelope: the envelope is cut into Reed-Solomon coded chunks, each a
//! message on the chunk topic, and offers, holdings and asks travel as strings
//! among the ids of IHAVE, IDONTWANT and IWANT. The topic, the chunk's frame
//! and the group's key are Prysm's (`seacast-devnet`).
//!
//! The node is `seacast_core::SeacastNode`, run here on the control tile's
//! thread with level 1 and level 2 inline. The mesh is the peer manager's: a
//! peer is pushed to while it is in the mesh of the chunk topic and
//! subscribed to it. A decoded envelope is handed on as a message received on
//! `execution_payload`.

mod host;
mod store;
mod wire;

use std::time::{Duration, Instant};

use fxhash::FxHashMap;
use seacast_codec::{ChunkBytes, SnappyBlockCompressor, VecBufferPool, build_group, leaf_hash};
use seacast_core::{
    BoundedDeferredStore, ChunkId, ChunkMsg, DispatchOutcome, NodeConfig, NodeId, PeerId,
    PenaltyReason, PreallocVec, Rpc, RuntimeEnv, SeacastNode, Shape, SourceOffers, SplitMix64,
    TimerEvent, TypeCodec, ValidationResult,
};
use seacast_gossipsub::{
    Inbound, MAX_CHUNK_BYTES, PAYLOAD, PayloadCatalog, PayloadCodec, build_chunk, chunk_id_of,
    is_seacast, payload_group, payload_join, read_holding, read_offer, read_want, split_chunk,
    write_entry,
};
use silver_common::{
    BroadcastValidation, ForkName, GossipDomain, GossipTopic, LOCAL_GOSSIP_STREAM_ID, MessageId,
    Nanos, NewGossipMsg, PeerEvent, SszCache, TCacheProducer, msg_id_valid_snappy,
    ssz_view::SignedExecutionPayloadEnvelopePqView,
};

use self::{
    host::{Env, Host},
    store::{Buf, NoPool, Store, StoreCodec},
    wire::{Out, Scratch},
};
use super::GossipHandler;
use crate::{GossipHandlerEvent, generated::RPCView, message::copy_compressed_to_protobuf_output};

const CHUNK_TOPIC: GossipTopic = GossipTopic::ExecutionPayloadSegment;
const _: () = assert!(
    silver_common::PAYLOAD_SEGMENT_MIN == seacast_gossipsub::CHUNK_FIXED &&
        silver_common::PAYLOAD_SEGMENT_MAX == MAX_CHUNK_BYTES
);
/// Connection indices the node has a slot for.
const MAX_PEERS: usize = 1280;
const STORE_BYTES: usize = 128 << 20;
/// Queue entries written in one pass, over all peers.
const WRITE_BUDGET: usize = 4096;
const MAX_N: usize = seacast_codec::MAX_PLAIN_CHUNKS;

type Catalog = PayloadCatalog<Host, StoreCodec>;

/// The handler's SEACAST state. The node is made when the fork digest is
/// known, since the chunk topic and the chunk ids bind it.
pub(super) struct Seacast {
    /// The connections, for the node made after them.
    peers: FxHashMap<usize, NodeId>,
    core: Option<Box<Core>>,
    /// An envelope sent as chunks is not sent whole, and one decoded from
    /// chunks is not forwarded whole.
    payload_only: bool,
}

impl Default for Seacast {
    fn default() -> Self {
        Self { peers: FxHashMap::default(), core: None, payload_only: true }
    }
}

struct Core {
    node: SeacastNode<Buf>,
    catalog: Catalog,
    env: Env,
    rng: SplitMix64,
    deferred: BoundedDeferredStore,
    domain: GossipDomain,
    /// The chunk topic's wire name.
    chunk_topic: String,
    start: Instant,
    heartbeat: f64,
    /// By connection: in the mesh of the chunk topic, subscribed to it.
    meshed: Vec<bool>,
    joined: Vec<bool>,
    /// A received chunk, decompressed.
    frame: Vec<u8>,
    decoder: snap::raw::Decoder,
    out: Scratch,
    rebuilt: PreallocVec<ChunkMsg<Buf>>,
    unwanted: Vec<ChunkId>,
}

fn wire_topic(domain: GossipDomain, topic: GossipTopic) -> String {
    topic.to_wire(&hex::encode(domain.digest()))
}

fn node_id(peer: &silver_common::PeerId) -> NodeId {
    seacast_codec::sha256(peer.as_bytes())
}

impl Core {
    fn new(domain: GossipDomain, now: Instant) -> Self {
        let mut config = NodeConfig::default();
        config.capacities.max_peers = MAX_PEERS;
        // No write is acknowledged to this tile.
        config.source_offers = SourceOffers::Publish;
        let heartbeat = config.heartbeat;
        let chunk_topic = wire_topic(domain, CHUNK_TOPIC);
        let codec = StoreCodec {
            inner: PayloadCodec::new(NoPool, chunk_topic.as_bytes()),
            store: Store::new(STORE_BYTES),
        };
        let mut env = Env::new();
        env.schedule_after(heartbeat, TimerEvent::Heartbeat);
        Self {
            node: SeacastNode::new(config),
            catalog: PayloadCatalog::new(Host::new(MAX_PEERS), codec, domain.digest()),
            env,
            rng: SplitMix64::new(Nanos::now().0),
            deferred: BoundedDeferredStore::with_capacity(MAX_PEERS, 256, 4096),
            domain,
            chunk_topic,
            start: now,
            heartbeat,
            meshed: vec![false; MAX_PEERS],
            joined: vec![false; MAX_PEERS],
            frame: vec![0; MAX_CHUNK_BYTES],
            decoder: snap::raw::Decoder::new(),
            out: Scratch::default(),
            rebuilt: PreallocVec::with_capacity(MAX_N),
            unwanted: Vec::with_capacity(64),
        }
    }

    fn connect(&mut self, peer: usize, id: NodeId) {
        if peer < MAX_PEERS {
            self.catalog.host.set_node_id(peer as PeerId, id);
            self.node.connect_peer(peer as PeerId, true, &mut self.env, &self.catalog);
        }
    }

    fn disconnect(&mut self, peer: usize) {
        if peer < MAX_PEERS {
            self.node.disconnect_peer(peer as PeerId, &mut self.env, &mut self.deferred);
            (self.meshed[peer], self.joined[peer]) = (false, false);
        }
    }

    /// A part of a received frame, to the node.
    fn part(&mut self, peer: PeerId) -> impl FnMut(Rpc<Buf>, bool) -> bool + '_ {
        let Self { node, env, catalog, rng, deferred, .. } = self;
        move |rpc, offered| node.on_frame_part(peer, rpc, offered, env, catalog, rng, deferred)
    }

    /// The relation with `peer` follows the mesh, for a peer that is linked.
    fn relate(&mut self, peer: usize) {
        if self.meshed[peer] && self.joined[peer] {
            self.node.relation_added(peer as PeerId, PAYLOAD, 0, 0, &mut self.env, &self.catalog);
        } else {
            self.node.relation_removed(peer as PeerId, PAYLOAD, 0, 0);
        }
    }

    fn mesh(&mut self, peer: usize, meshed: bool) {
        if peer < MAX_PEERS && self.meshed[peer] != meshed {
            self.meshed[peer] = meshed;
            self.relate(peer);
        }
    }

    /// A message on the chunk topic: level 1 in full, then the chunk in its
    /// place in its group's assembly. `None`: not to be given to the node.
    fn read_chunk(&mut self, peer: usize, snappy: &[u8]) -> Option<ChunkMsg<Buf>> {
        let bad = |env: &mut Env| {
            env.penalties.push((peer as PeerId, PenaltyReason::InvalidChunkLevel1));
            None
        };
        let len = snap::raw::decompress_len(snappy).ok().filter(|&len| len <= MAX_CHUNK_BYTES);
        let Some(len) = len.filter(|_| self.decoder.decompress(snappy, &mut self.frame).is_ok())
        else {
            return bad(&mut self.env);
        };
        let frame = &self.frame[..len];
        let codec = &mut self.catalog.codec;
        let read = codec.inner.read_message(snappy, |_, _| Some(Buf::scratch(frame)));
        let Some(chunk) = read.filter(|chunk| {
            codec.inner.check(chunk) && codec.inner.check_proof(chunk) == ValidationResult::Accept
        }) else {
            return bad(&mut self.env);
        };
        let f = split_chunk(frame)?;
        // A group's slot is the wall slot at its first chunk.
        let (key, slot) = (f.descriptor.root, self.catalog.host.wall_slot);
        let store = &mut codec.store;
        let at = store.find(&key).or_else(|| store.open(&key, slot, &f.descriptor))?;
        // The key does not bind the descriptor: the first one taken stands.
        if store.descriptor(at) != Some(&f.descriptor) {
            return None;
        }
        let (index, head) = (f.index as usize, &frame[..len - f.data.len()]);
        match store.head(at, index) {
            // Another copy of the chunk held.
            Some(held) if held == head => {}
            Some(_) => return None,
            None => {
                store.put(at, index, head, f.data, &chunk.memo, &chunk.cached_id)?;
                store.note_sender(at, peer);
            }
        }
        Some(ChunkMsg { frame: store.chunk(at, index)?, ..chunk })
    }

    /// The parts of a received `RPC` that are SEACAST's, in the order the node
    /// takes a frame: the JOIN, the chunks, the offers, the asks, the
    /// holdings.
    fn on_rpc(&mut self, peer: usize, rpc: &RPCView<'_>) {
        if peer >= MAX_PEERS {
            return;
        }
        let id = peer as PeerId;
        let mut frame = Inbound::new();
        for subscription in &rpc.subscriptions {
            if subscription.topic_id != Some(self.chunk_topic.as_str()) {
                continue;
            }
            match subscription.subscribe {
                Some(true) => {
                    self.joined[peer] = true;
                    let join = payload_join(self.domain.digest());
                    frame.join(join, &mut self.part(id));
                }
                Some(false) => {
                    self.joined[peer] = false;
                    self.node.on_leave(id, PAYLOAD);
                }
                None => continue,
            }
            self.relate(peer);
        }
        let control = rpc.control.as_option();
        if let Some(control) = control {
            for graft in &control.graft {
                if graft.topic_id == Some(self.chunk_topic.as_str()) {
                    self.mesh(peer, true);
                }
            }
            for prune in &control.prune {
                if prune.topic_id == Some(self.chunk_topic.as_str()) {
                    self.mesh(peer, false);
                }
            }
        }
        for message in &rpc.publish {
            if message.topic == self.chunk_topic &&
                let Some(snappy) = message.data &&
                let Some(chunk) = self.read_chunk(peer, snappy)
            {
                frame.chunk(chunk, &mut self.part(id));
            }
        }
        if let Some(control) = control {
            let route_hops = self.catalog.params.route_hops;
            for ihave in &control.ihave {
                if ihave.topic_id != Some(self.chunk_topic.as_str()) {
                    continue;
                }
                for entry in &ihave.message_ids {
                    if let Some(offer) = read_offer(entry) {
                        frame.offer(offer, &mut self.part(id));
                    }
                }
            }
            for iwant in &control.iwant {
                for entry in &iwant.message_ids {
                    if let Some(want) = read_want(entry, route_hops) {
                        frame.want(want, &mut self.part(id));
                    }
                }
            }
            for idontwant in &control.idontwant {
                for entry in &idontwant.message_ids {
                    if !is_seacast(entry) {
                        continue;
                    }
                    if let Some(holding) = read_holding(entry) {
                        frame.holding(holding, &mut self.part(id));
                    } else if let Some(chunk) = chunk_id_of(entry) {
                        self.unwanted.push(chunk);
                    }
                }
            }
        }
        frame.finish(&mut self.part(id));
        for chunk in self.unwanted.drain(..) {
            self.node.note_unwanted(id, chunk);
        }
        self.decode();
    }

    /// Level 2 of the groups the node asked for, in place.
    fn decode(&mut self) {
        while let Some((group, shape)) = self.env.to_decode.pop() {
            let codec = &self.catalog.codec;
            // An assembly freed since: the group ends at its deadline.
            let Some(first) = codec
                .store
                .find(&group.key)
                .and_then(|at| codec.chunk_msg(at, codec.store.first(at)?, [0; 32]))
            else {
                continue;
            };
            self.rebuilt.clear();
            let object = self.catalog.decode(&group, &[first], &shape, &mut self.rebuilt);
            self.node.on_decoded(
                group,
                object,
                self.rebuilt.as_slice(),
                &mut self.env,
                &mut self.catalog,
                &mut self.rng,
            );
        }
    }

    fn timers(&mut self, now: Instant) {
        self.env.now = now.duration_since(self.start).as_secs_f64();
        self.catalog.codec.store.loop_start();
        while let Some(event) = self.env.pop_due() {
            if event == TimerEvent::Heartbeat {
                self.env.schedule_after(self.heartbeat, TimerEvent::Heartbeat);
            }
            self.node.on_timer(
                event,
                &mut self.env,
                &mut self.catalog,
                &mut self.rng,
                &mut self.deferred,
            );
        }
        self.decode();
    }

    fn clock(&mut self, wall_slot: u64, slot_start: Instant, slot: Duration) {
        let host = &mut self.catalog.host;
        if host.wall_slot != wall_slot {
            self.catalog.codec.store.expire(wall_slot);
        }
        host.wall_slot = wall_slot;
        host.slot_secs = slot.as_secs_f64();
        host.slot_start = match slot_start.checked_duration_since(self.start) {
            Some(since) => since.as_secs_f64(),
            None => -self.start.duration_since(slot_start).as_secs_f64(),
        };
    }

    /// Publishes a local envelope as its source. `false`: its chunks do not
    /// go out.
    fn publish(&mut self, ssz: &[u8]) -> bool {
        if self.domain.format() != ForkName::Pq ||
            !SignedExecutionPayloadEnvelopePqView::check_size(ssz) ||
            ssz.len() > seacast_codec::MAX_PLAIN_BODY
        {
            return false;
        }
        let slot = self.catalog.host.wall_slot;
        let (d, shards) =
            build_group::<Vec<u8>, _, _>(ssz, &mut VecBufferPool, &SnappyBlockCompressor);
        let key = d.root;
        let group = payload_group(key);
        let codec = &mut self.catalog.codec;
        if codec.store.find(&key).is_some() {
            return true;
        }
        let Some(at) = codec.store.open(&key, slot, &d) else {
            silver_log::warn!(slot, "seacast: no room to publish the envelope");
            return false;
        };
        self.rebuilt.clear();
        for shard in &shards {
            let head = build_chunk(&d, shard.index, &shard.proof, &[]);
            let id = codec.inner.chunk_id(&group, shard.index, &head, &shard.data);
            let leaf = leaf_hash(&shard.data);
            let index = shard.index as usize;
            if codec.store.put(at, index, &head, &shard.data, &leaf, &id).is_none() {
                return false;
            }
            self.rebuilt.push(codec.chunk_msg(at, index, leaf).expect("stored"));
        }
        // Our own group: authentic without the domain's word.
        self.catalog.host.admit(group);
        self.node.publish(
            group,
            Shape { n: d.n(), k: d.k() },
            self.rebuilt.as_slice(),
            &mut self.env,
            &self.catalog,
            &mut self.rng,
        );
        true
    }
}

impl GossipHandler {
    pub fn set_seacast_payload_only(&mut self, payload_only: bool) {
        self.seacast.payload_only = payload_only;
    }

    /// The node exists once the current domain is known. A later change of
    /// digest is not followed: the node keeps the topic it was made with.
    fn seacast_core(&mut self, now: Instant) -> Option<&mut Core> {
        if self.seacast.core.is_none() {
            let mut core = Box::new(Core::new(self.domains.current_domain()?, now));
            for (&peer, &id) in &self.seacast.peers {
                core.connect(peer, id);
            }
            self.seacast.core = Some(core);
        }
        self.seacast.core.as_deref_mut()
    }

    /// Our GRAFT or PRUNE of `peer` on `topic`.
    pub(super) fn seacast_mesh(
        &mut self,
        peer: usize,
        topic: GossipTopic,
        digest: [u8; 4],
        meshed: bool,
    ) {
        if topic == CHUNK_TOPIC &&
            let Some(core) = &mut self.seacast.core &&
            core.domain.digest() == digest
        {
            core.mesh(peer, meshed);
        }
    }

    pub(super) fn seacast_rpc(
        &mut self,
        peer: usize,
        rpc: &RPCView<'_>,
        now: Instant,
        emit: &mut impl FnMut(GossipHandlerEvent),
    ) {
        if let Some(core) = self.seacast_core(now) {
            core.on_rpc(peer, rpc);
        }
        self.seacast_flush(emit);
    }

    pub(super) fn seacast_tick(&mut self, now: Instant, emit: &mut impl FnMut(GossipHandlerEvent)) {
        if let Some(core) = self.seacast_core(now) {
            core.timers(now);
        }
        self.seacast_flush(emit);
    }

    /// Publishes a local envelope as chunks. `true`: they go out and the
    /// envelope is not to be sent whole.
    pub(super) fn seacast_publish(&mut self, topic: GossipTopic, ssz: &[u8]) -> bool {
        let payload_only = self.seacast.payload_only;
        topic == GossipTopic::ExecutionPayload &&
            self.seacast_core(Instant::now()).is_some_and(|core| core.publish(ssz)) &&
            payload_only
    }

    /// Connections open and close.
    pub fn seacast_peer_event(&mut self, event: &PeerEvent) {
        match *event {
            PeerEvent::P2pNewConnection { p2p_peer_id, ref peer_id_full, .. } => {
                let id = node_id(peer_id_full);
                self.seacast.peers.insert(p2p_peer_id, id);
                if let Some(core) = &mut self.seacast.core {
                    core.connect(p2p_peer_id, id);
                }
            }
            PeerEvent::P2pDisconnect { p2p_peer, .. } => {
                self.seacast.peers.remove(&p2p_peer);
                if let Some(core) = &mut self.seacast.core {
                    core.disconnect(p2p_peer);
                }
            }
            _ => {}
        }
    }

    /// The slot clock: the wall slot, its start and a slot's length.
    pub fn seacast_clock(&mut self, wall_slot: u64, slot_start: Instant, slot: Duration) {
        if let Some(core) = &mut self.seacast.core {
            core.clock(wall_slot, slot_start, slot);
        }
    }

    /// Writes what the node has queued, reports its penalties and hands on
    /// the envelopes it decoded.
    fn seacast_flush(&mut self, emit: &mut impl FnMut(GossipHandlerEvent)) {
        let Some(core) = self.seacast.core.as_deref_mut() else {
            return;
        };
        let mut budget = WRITE_BUDGET;
        let mut peers = std::mem::take(&mut core.env.to_write);
        peers.retain(|&peer| {
            macro_rules! out {
                () => {
                    Out {
                        peer: peer as usize,
                        topic: &core.chunk_topic,
                        producer: &mut self.mcache_publish,
                        reader: &mut self.reader,
                        store: &mut core.catalog.codec.store,
                        scratch: &mut core.out,
                        emit: &mut *emit,
                    }
                };
            }
            let mut done = false;
            while budget > 0 && !done {
                match core.node.next_to_send(peer, &mut core.env, &core.catalog) {
                    DispatchOutcome::Send(entry) => {
                        budget -= 1;
                        write_entry(&entry, &mut out!());
                    }
                    DispatchOutcome::Wait | DispatchOutcome::Empty => done = true,
                }
            }
            out!().flush();
            !done
        });
        // Peers woken by the writes above come after those left over.
        peers.append(&mut core.env.to_write);
        core.env.to_write = peers;

        for (peer, reason) in core.env.penalties.drain(..) {
            silver_log::debug!(peer, ?reason, "seacast penalty");
            emit(GossipHandlerEvent::PeerEvent(PeerEvent::P2pGossipInvalidControl {
                p2p_peer: peer as usize,
            }));
        }
        let domain = core.domain;
        let mut delivered = std::mem::take(&mut core.env.delivered);
        delivered.retain(|body| core.catalog.codec.store.live(body));
        for body in delivered.drain(..) {
            match self.seacast_deliver(domain, &body, emit) {
                Ok(new) => {
                    let store = self.seacast.core.as_ref().map(|core| &core.catalog.codec.store);
                    if let Some((slot, root, d, received, peers)) =
                        store.and_then(|store| store.origin(&body))
                    {
                        silver_log::info!(
                            slot,
                            root = %hex::encode(root),
                            k = d.k(),
                            n = d.n(),
                            received,
                            peers = ?peers,
                            new,
                            "seacast: envelope from segments"
                        );
                    }
                }
                Err(e) => silver_log::warn!(?e, "seacast: decoded envelope not delivered"),
            }
        }
        if let Some(core) = &mut self.seacast.core {
            core.env.delivered = delivered;
        }
    }

    /// A decoded envelope, as a message received on `execution_payload`: the
    /// SSZ to the incoming tcache, the message for forwarding to the outgoing
    /// one. `body` is the segmented body; where it was compressed before it
    /// was segmented it is the message's data as it stands. `false`: a copy
    /// that came whole was handed on before.
    fn seacast_deliver(
        &mut self,
        domain: GossipDomain,
        body: &Buf,
        emit: &mut impl FnMut(GossipHandlerEvent),
    ) -> Result<bool, silver_common::Error> {
        use silver_common::Error;
        let topic = GossipTopic::ExecutionPayload;
        let wire = wire_topic(domain, topic);
        let segmented = body.as_bytes();
        let coded = body.index() == 1;
        let len = match coded {
            true => snap::raw::decompress_len(segmented).map_err(|_| Error::InvalidSnappy)?,
            false => segmented.len(),
        };
        if len > topic.max_uncompressed_size(domain.format()) {
            return Err(Error::GossipPayloadTooLarge);
        }
        let mut reservation =
            self.incoming_gossip_publish.reserve(len, false).ok_or(Error::BufferTooSmall)?;
        let ssz = &mut self.incoming_gossip_publish.reservation_buffer(&mut reservation)?[..len];
        let compressed = match coded {
            true => {
                snap::raw::Decoder::new().decompress(segmented, ssz)?;
                segmented
            }
            false => {
                ssz.copy_from_slice(segmented);
                let n = self.compress(segmented)?;
                &self.snap_scratch[..n]
            }
        };
        let msg_hash: MessageId = msg_id_valid_snappy(&wire, ssz);
        reservation.increment_offset(len);
        // A copy that came as a full message has been handed on already.
        let Ok(fast) = self.dedup_cache.contains_fast(&wire, compressed) else {
            return Ok(false);
        };
        if !self.dedup_cache.insert(fast, msg_hash) {
            return Ok(false);
        }
        let protobuf = match self.seacast.payload_only {
            true => None,
            false => {
                copy_compressed_to_protobuf_output(&mut self.mcache_publish, compressed, &wire).ok()
            }
        };
        let ssz = reservation.read();
        std::io::Write::flush(&mut reservation)?;
        emit(GossipHandlerEvent::NewGossip(NewGossipMsg {
            stream_id: LOCAL_GOSSIP_STREAM_ID,
            topic,
            domain,
            ssz_cache: SszCache::Gossip,
            msg_hash,
            recv_ts: Nanos::now(),
            ssz,
            protobuf,
            validation: BroadcastValidation::Gossip,
        }));
        Ok(true)
    }
}

#[cfg(test)]
mod tests;
