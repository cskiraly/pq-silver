//! The tile on the spine. The real-prover tests are ignored: they start
//! leanVM's pool on the cores in `LEANVM_TEST_CORES` (e.g. `0,1,2,3`), pin
//! the test thread to the first, and prove for a while.

use std::time::{Duration, Instant};

use flux::{spine::SpineAdapter, tile::Tile};
use lean_vm::{AggregateClaims, lean_xmss};
use silver_common::{
    LeanVmHealthEvent, LeanVmReq, LeanVmResp, SilverSpine, TCache, TCacheId, TCacheProducer,
    TCacheReader, TCacheTable, TProducer, TReadMode, test_util::ShmemDir,
};

use super::LeanVmTile;
use crate::wire::{Request, tests::encode_request};

struct Injector;
impl Tile<SilverSpine> for Injector {
    fn loop_body(&mut self, _: &mut SpineAdapter<SilverSpine>) {}
}

struct Rig {
    _base: ShmemDir,
    _spine: Box<SilverSpine>,
    tile: LeanVmTile,
    adapter: SpineAdapter<SilverSpine>,
    injector: SpineAdapter<SilverSpine>,
    handoff: TProducer,
    responses: TCacheReader,
    answers: Vec<(u64, Option<Vec<u8>>)>,
    health: Vec<bool>,
}

impl Rig {
    fn new(cores: Vec<usize>) -> Self {
        let base = ShmemDir::new().unwrap();
        let mut spine = Box::new(SilverSpine::new_with_base_dir(base.path(), None));
        let handoff = TCache::producer(TCacheId::BeaconStateHandoff, 1 << 24);
        let out = TCache::producer(TCacheId::LeanVmProcessing, 1 << 24);
        let out_ref = out.cache_ref();
        let tcaches = TCacheTable::from_iter([handoff.cache_ref(), out_ref]);
        let mut tile = LeanVmTile::new(cores, tcaches, out);
        let mut adapter = SpineAdapter::connect_tile(&tile, &mut *spine);
        tile.on_attach(&mut adapter);
        let mut injector = SpineAdapter::connect_tile(&Injector, &mut *spine);
        injector.consume(|_: LeanVmResp, _| {});
        injector.consume(|_: LeanVmHealthEvent, _| {});
        assert!(tile.try_init(&mut adapter));
        let responses = TCacheReader::single(out_ref, "test", TReadMode::Sliding).unwrap();
        Self {
            _base: base,
            _spine: spine,
            tile,
            adapter,
            injector,
            handoff,
            responses,
            answers: Vec::new(),
            health: Vec::new(),
        }
    }

    fn crank(&mut self) {
        self.tile.loop_body(&mut self.adapter);
        let Self { injector, responses, answers, health, .. } = self;
        injector.consume(|resp: LeanVmResp, _| {
            let body = resp.body.map(|read| {
                let acquired = responses.acquire(read);
                acquired.buffer().unwrap().0.to_vec()
            });
            answers.push((resp.request_id, body));
        });
        injector.consume(|event: LeanVmHealthEvent, _| health.push(event.ready));
    }

    fn request(&mut self, request_id: u64, body: &[u8]) {
        let body = self.handoff.write_with(body.len(), |out| out.copy_from_slice(body)).unwrap();
        self.handoff.loop_start();
        self.injector.produce(LeanVmReq { request_id, body });
    }

    fn answer(&mut self, request_id: u64) -> Option<Vec<u8>> {
        let deadline = Instant::now() + Duration::from_secs(300);
        loop {
            if let Some((_, body)) = self.answers.iter().find(|(id, _)| *id == request_id) {
                return body.clone();
            }
            assert!(Instant::now() < deadline, "no answer to {request_id}");
            self.crank();
        }
    }
}

#[test]
fn without_a_prover_every_request_is_answered_none() {
    let mut rig = Rig::new(Vec::new());
    rig.request(1, &[2]);
    rig.request(2, &[1]);
    assert_eq!(rig.answer(1), None);
    assert_eq!(rig.answer(2), None);
    assert!(rig.health.is_empty());
}

/// Whatever the machine, one CPU per physical core, none below `first`.
#[test]
fn default_cores_take_one_thread_per_core_from_first_on() {
    let all = super::default_cores(0);
    assert!(!all.is_empty());
    assert!(all.iter().all(|&cpu| super::first_sibling(cpu) == cpu));
    let from_two = super::default_cores(2);
    assert!(from_two.iter().all(|&cpu| cpu >= 2 && all.contains(&cpu)));
}

fn test_cores() -> Vec<usize> {
    let cores = std::env::var("LEANVM_TEST_CORES").expect("LEANVM_TEST_CORES, e.g. 0,1,2,3");
    cores.split(',').map(|core| core.trim().parse().unwrap()).collect()
}

/// An `aggregate` response's claims and proof.
fn proven(response: &[u8]) -> (AggregateClaims, Vec<u8>) {
    assert_eq!(response[0], 0, "refused: {response:?}");
    let field = |at: usize| {
        let len = u64::from_le_bytes(response[at..at + 8].try_into().unwrap()) as usize;
        (&response[at + 8..at + 8 + len], at + 8 + len)
    };
    let (claims, next) = field(1);
    let (proof, end) = field(next);
    assert_eq!(end, response.len());
    (AggregateClaims::from_bytes(claims).unwrap(), proof.to_vec())
}

fn verify_body(claims: &AggregateClaims, proof: &[u8]) -> Vec<u8> {
    let mut body = vec![2];
    for field in [&claims.to_bytes()[..], proof] {
        body.extend((field.len() as u64).to_le_bytes());
        body.extend(field);
    }
    body
}

/// Four keys sign one message; the tile proves, then judges the proof
/// against its claims and against one claim short, with verifies beside a
/// second proof.
#[test]
#[ignore = "proves with the real prover: set LEANVM_TEST_CORES"]
fn the_tile_proves_and_verifies_as_the_sidecar_did() {
    let message = [7; lean_xmss::MESSAGE_LEN];
    let raw_lean_xmss: Vec<_> = (0..4u8)
        .map(|k| {
            let (secret_key, public_key) = lean_xmss::key_gen(&[k; lean_xmss::SECRET_KEY_LEN]);
            secret_key.prepare(7);
            (public_key, message, lean_xmss::sign(&secret_key, &message, 7))
        })
        .collect();
    let request = |raw_lean_xmss: Vec<_>, children| Request {
        children,
        raw_lean_xmss,
        raw_lean_sphincs: vec![],
        blobs: vec![],
        output: None,
        log_inv_rate: 2,
    };

    let mut rig = Rig::new(test_cores());
    rig.crank();
    assert_eq!(rig.health, [true]);

    rig.request(1, &encode_request(&request(raw_lean_xmss[..2].to_vec(), vec![])));
    let (claims, proof) = proven(&rig.answer(1).expect("a response"));
    assert_eq!(claims.lean_xmss.len(), 2);

    // One crank takes all three: the verifies run while the tile proves 2.
    rig.request(
        2,
        &encode_request(&request(raw_lean_xmss[2..].to_vec(), vec![(
            claims.clone(),
            proof.clone(),
        )])),
    );
    let mut fewer = claims.clone();
    fewer.lean_xmss.pop();
    rig.request(3, &verify_body(&claims, &proof));
    rig.request(4, &verify_body(&fewer, &proof));
    assert_eq!(rig.answer(3), Some(vec![0, 1]));
    assert_eq!(rig.answer(4), Some(vec![0, 0]));
    let (union, _) = proven(&rig.answer(2).expect("a response"));
    assert_eq!(union.lean_xmss.len(), 4);

    rig.request(5, &[9]);
    assert_eq!(rig.answer(5).expect("a response")[0], 5, "malformed");
}
