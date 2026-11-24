//! The leanVM tile: proves and verifies in this process, as the leanVM
//! sidecar did in its own, behind the same spine messages and bodies.
//!
//! [`LeanVmTile`] takes `LeanVmReq` bodies off the spine and answers each
//! with a `LeanVmResp`, its body the sidecar's response (status byte
//! included), or `None` for no answer. `LeanVmHealthEvent` says the prover is
//! ready.
//!
//! The tile's thread proves. leanVM's pool runs the thread that calls
//! `aggregate` as its worker 0, so the tile's core is a prover core, and the
//! pool's other workers run on the rest of the prover cores. A proof blocks
//! the tile for its duration. Verifies run beside it on their own threads,
//! as on the sidecar's connection threads, one request each at a time.

#[cfg(test)]
mod tests;
mod wire;

use std::{
    collections::VecDeque,
    sync::{
        Arc, Mutex,
        mpsc::{self, Receiver, Sender},
    },
    thread,
    time::Instant,
};

use flux::{spine::SpineAdapter, tile::Tile, utils::try_thread_boot};
use lean_vm::{AggregateProof, OutputClaims};
use silver_common::{
    LeanVmHealthEvent, LeanVmReq, LeanVmResp, SilverSpine, TCacheError, TCacheId, TCacheProducer,
    TCacheReader, TCacheTable, TProducer, TReadMode,
};
use wire::{Call, Rejection, Request};

/// Aggregates taken off the spine and waiting for the prover. `beacon_state`
/// keeps one in flight, so a full queue means it lost track; past it each
/// request is answered `None`.
const MAX_QUEUED_AGGREGATES: usize = 16;
/// Verifies in flight, as the sidecar's 16 connections allowed. Past it each
/// request is answered `None`.
const MAX_VERIFIES: usize = 16;

pub struct LeanVmTile {
    /// The tile's core first; the prover runs on all of them. Empty: no
    /// prover, and every request is answered `None`.
    cores: Vec<usize>,
    reader: TCacheReader,
    producer: TProducer,
    /// From `try_init` on, with a prover.
    verifier: Option<Verifier>,
    aggregates: VecDeque<(u64, Vec<u8>)>,
    answers: Vec<LeanVmResp>,
    ready_published: bool,
}

impl Tile<SilverSpine> for LeanVmTile {
    /// Before any tile runs, so no request sent while the prover warms up
    /// goes unseen.
    fn on_attach(&mut self, adapter: &mut SpineAdapter<SilverSpine>) {
        adapter.subscribe_broadcast::<LeanVmReq>();
    }

    fn try_init(&mut self, _: &mut SpineAdapter<SilverSpine>) -> bool {
        self.open_tcaches().expect("tcache wiring");
        if !self.cores.is_empty() {
            self.verifier = Some(start_prover(&self.cores));
        }
        true
    }

    fn loop_body(&mut self, adapter: &mut SpineAdapter<SilverSpine>) {
        self.producer.loop_start();
        self.collect_verdicts();
        self.intake(adapter);
        if let Some((request_id, body)) = self.aggregates.pop_front() {
            let response = respond(&body);
            self.answer(request_id, Some(&response));
            adapter.mark_work();
        }
        self.publish(adapter);
    }
}

impl LeanVmTile {
    /// `cores`: the tile's own core first, which the caller pins the tile
    /// to, then the prover's other cores. Empty for no prover.
    pub fn new(cores: Vec<usize>, tcaches: TCacheTable, producer: TProducer) -> Self {
        Self {
            cores,
            reader: TCacheReader::new(tcaches),
            producer,
            verifier: None,
            aggregates: VecDeque::new(),
            answers: Vec::new(),
            ready_published: false,
        }
    }

    pub fn open_tcaches(&mut self) -> Result<(), TCacheError> {
        self.reader.open(
            TCacheId::BeaconStateHandoff,
            "leanvm_beacon_state_handoff",
            TReadMode::Sliding,
        )
    }

    /// Copies each request out of the tcache: a verify goes to the verify
    /// threads, an aggregate to the queue.
    fn intake(&mut self, adapter: &mut SpineAdapter<SilverSpine>) {
        // The previous pass drained the queue.
        self.reader.free();
        let Self { reader, verifier, aggregates, answers, .. } = self;
        adapter.consume(|req: LeanVmReq, _| {
            let request_id = req.request_id;
            let taken = verifier.as_mut().and_then(|verifier| {
                let body = copy_body(reader, req)?;
                if wire::is_verify(&body) {
                    verifier.send(request_id, body)
                } else if aggregates.len() < MAX_QUEUED_AGGREGATES {
                    aggregates.push_back((request_id, body));
                    Some(())
                } else {
                    silver_log::warn!(request_id, "leanvm aggregate queue full");
                    None
                }
            });
            if taken.is_none() {
                answers.push(LeanVmResp { request_id, body: None });
            }
        });
    }

    fn collect_verdicts(&mut self) {
        while let Some((request_id, response)) = self.verifier.as_mut().and_then(Verifier::try_recv)
        {
            self.answer(request_id, Some(&response));
        }
    }

    fn answer(&mut self, request_id: u64, response: Option<&[u8]>) {
        let body = response.and_then(|response| {
            let body =
                self.producer.write_with(response.len(), |out| out.copy_from_slice(response));
            if body.is_none() {
                silver_log::error!(request_id, "leanvm tcache full; response dropped");
            }
            body
        });
        self.answers.push(LeanVmResp { request_id, body });
    }

    /// Health before answers, so a reader sees the prover ready before its
    /// first answer.
    fn publish(&mut self, adapter: &mut SpineAdapter<SilverSpine>) {
        if self.verifier.is_some() && !self.ready_published {
            silver_log::info!("leanvm prover ready");
            self.ready_published = true;
            adapter.produce(LeanVmHealthEvent { ready: true });
        }
        for resp in self.answers.drain(..) {
            adapter.produce(resp);
        }
    }
}

/// The default prover cores: one hardware thread per physical core this
/// process may run on, from CPU `first` on. A core whose sibling thread lies
/// below `first` is left out: a tile or the logger already runs there. The
/// prover gains nothing from a second thread on a core.
pub fn default_cores(first: usize) -> Vec<usize> {
    let allowed = flux::core_affinity::get_core_ids().unwrap_or_default();
    let mut cores: Vec<usize> = allowed
        .into_iter()
        .map(|core| core.id)
        .filter(|&cpu| cpu >= first && first_sibling(cpu) == cpu)
        .collect();
    cores.sort_unstable();
    cores
}

/// The lowest CPU sharing `cpu`'s physical core, `cpu` itself when the
/// topology is unreadable.
fn first_sibling(cpu: usize) -> usize {
    let path = format!("/sys/devices/system/cpu/cpu{cpu}/topology/thread_siblings_list");
    let Ok(list) = std::fs::read_to_string(path) else { return cpu };
    // A list such as `1,17` or `0-1`: its first number is its lowest.
    let first = list.trim().split([',', '-']).next().and_then(|n| n.parse().ok());
    first.unwrap_or(cpu)
}

/// The request's body, if it is in an opened tcache and was not lapped
/// while copied.
fn copy_body(reader: &mut TCacheReader, req: LeanVmReq) -> Option<Vec<u8>> {
    let request_id = req.request_id;
    if !reader.is_open(req.body.id()) {
        silver_log::error!(request_id, "leanvm request outside the opened tcaches");
        return None;
    }
    let acquired = reader.acquire(req.body);
    let Ok((body, _)) = acquired.buffer() else {
        silver_log::warn!(request_id, "leanvm request lapped in the tcache");
        return None;
    };
    let body = body.to_vec();
    if acquired.revalidate().is_err() {
        silver_log::warn!(request_id, "leanvm request lapped in the tcache");
        return None;
    }
    Some(body)
}

/// Starts leanVM's pool on `cores` and the verify threads beside it, then
/// pins the calling thread back to `cores[0]` and warms the prover up.
///
/// The pool's workers and the verify threads inherit the calling thread's
/// CPU set, so it spans every prover core while they start.
fn start_prover(cores: &[usize]) -> Verifier {
    let started = Instant::now();
    try_thread_boot(cores, None).expect("pin to the leanvm cores");
    lean_vm::setup(cores.len());
    let verifier = Verifier::spawn(cores.len());
    try_thread_boot(&cores[..1], None).expect("pin to the leanvm tile core");
    warm_up();
    silver_log::info!(?cores, elapsed = ?started.elapsed(), "leanvm prover started");
    verifier
}

/// A fresh process's first proofs are slower, touching memory the allocator
/// then keeps. As the sidecar did, this pays for it before the first
/// request: a discarded leaf over one blob, the one input that needs no key,
/// and a full recursion over it, at the rate used in practice.
fn warm_up() {
    let blobs = lean_vm::lean_da::BlobMatrix::build(&[[0; lean_vm::lean_da::BLOB_SYMBOLS]])
        .expect("one blob");
    let rate = 2;
    let leaf = lean_vm::aggregate(&[], vec![], vec![], Some(&blobs), OutputClaims::All, rate)
        .expect("the warm-up's leaf proves");
    let children = vec![leaf; lean_vm::MAX_RECURSIONS];
    lean_vm::aggregate(&children, vec![], vec![], None, OutputClaims::All, rate)
        .expect("the warm-up's recursion proves");
}

/// The sidecar's response to one request body.
fn respond(body: &[u8]) -> Vec<u8> {
    match wire::decode_request(body) {
        Ok(Call::Verify(claims, proof)) => {
            let valid = AggregateProof::from_bytes(&proof).is_some_and(|p| p.verify(&claims));
            wire::encode_verdict(valid).to_vec()
        }
        Ok(Call::Aggregate(request)) => wire::encode_response(&aggregate(request)),
        Err(rejection) => {
            silver_log::warn!(rejection = ?rejection.0, message = rejection.1, "leanvm request refused");
            wire::encode_response(&Err(rejection))
        }
    }
}

fn aggregate(request: Request) -> Result<(lean_vm::AggregateClaims, Vec<u8>), (Rejection, String)> {
    let started = Instant::now();
    let shape = (request.children.len(), request.raw_lean_xmss.len(), request.log_inv_rate);
    let children = request
        .children
        .into_iter()
        .enumerate()
        .map(|(i, (claims, proof))| match AggregateProof::from_bytes(&proof) {
            Some(proof) => Ok((claims, proof)),
            None => Err((Rejection::InvalidChild, format!("child {i}'s proof does not decode"))),
        })
        .collect::<Result<Vec<_>, _>>()?;
    let blobs = match request.blobs.len() {
        0 => None,
        _ => Some(
            lean_vm::lean_da::BlobMatrix::build(&request.blobs)
                .map_err(|e| (Rejection::InvalidInput, e.to_string()))?,
        ),
    };
    let output = request.output.as_ref().map_or(OutputClaims::All, OutputClaims::Only);
    let outcome = lean_vm::aggregate(
        &children,
        request.raw_lean_xmss,
        request.raw_lean_sphincs,
        blobs.as_ref(),
        output,
        request.log_inv_rate,
    );
    let (children, signatures, rate) = shape;
    let elapsed = started.elapsed();
    match outcome {
        Ok((claims, proof)) => {
            silver_log::info!(children, signatures, rate, ?elapsed, "leanvm proven");
            Ok((claims, proof.to_bytes()))
        }
        Err(error) => {
            silver_log::warn!(children, signatures, rate, ?elapsed, %error, "leanvm refused");
            Err((error.into(), error.to_string()))
        }
    }
}

/// The verify threads: each takes one request body at a time from a shared
/// queue and sends back its response.
struct Verifier {
    jobs: Sender<(u64, Vec<u8>)>,
    done: Receiver<(u64, Vec<u8>)>,
    in_flight: usize,
}

impl Verifier {
    fn spawn(threads: usize) -> Self {
        let (jobs, queue) = mpsc::channel::<(u64, Vec<u8>)>();
        let (results, done) = mpsc::channel();
        let queue = Arc::new(Mutex::new(queue));
        for index in 0..threads {
            let (queue, results) = (queue.clone(), results.clone());
            thread::Builder::new()
                .name(format!("leanvm-verify-{index}"))
                .spawn(move || {
                    loop {
                        // The guard drops at the statement's end, so the
                        // others take jobs while this one verifies.
                        let job = queue.lock().unwrap().recv();
                        // Ends with the tile, which drops the sender.
                        let Ok((request_id, body)) = job else { return };
                        if results.send((request_id, respond(&body))).is_err() {
                            return;
                        }
                    }
                })
                .expect("spawn a leanvm verify thread");
        }
        Self { jobs, done, in_flight: 0 }
    }

    fn send(&mut self, request_id: u64, body: Vec<u8>) -> Option<()> {
        if self.in_flight == MAX_VERIFIES {
            silver_log::warn!(request_id, "leanvm verifies all busy");
            return None;
        }
        self.jobs.send((request_id, body)).ok()?;
        self.in_flight += 1;
        Some(())
    }

    fn try_recv(&mut self) -> Option<(u64, Vec<u8>)> {
        let result = self.done.try_recv().ok()?;
        self.in_flight -= 1;
        Some(result)
    }
}
