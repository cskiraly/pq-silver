//! The leanVM sidecar's request and response bodies, kept byte for byte so
//! `beacon_state` speaks to this tile as it spoke to the sidecar (leanVM
//! `bins/leanvm-sidecar/README.md` at `bindings-v0.16.0`). Frames and the
//! hello are gone: each body travels whole through a tcache. Every integer is
//! little-endian; every variable-length field is a `u64` length, then its
//! bytes.

use lean_vm::{
    AggregateClaims, AggregateError, MAX_RECURSIONS, lean_da,
    lean_sphincs::{self, LeanSphincsPublicKey, LeanSphincsSignature},
    lean_xmss::{self, LeanXmssPublicKey, LeanXmssSignature},
};

const OP_AGGREGATE: u8 = 1;
const OP_VERIFY: u8 = 2;
const OK: u8 = 0;

const XMSS_RECORD: usize = lean_xmss::PUB_KEY_SIZE + lean_xmss::MESSAGE_LEN + lean_xmss::SIG_SIZE;
const SPHINCS_RECORD: usize =
    lean_sphincs::PUB_KEY_SIZE + lean_sphincs::MESSAGE_LEN + lean_sphincs::SIG_SIZE;
const BLOB_BYTES: usize = size_of::<lean_da::Blob>();

/// Why no proof was made: the response's status byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum Rejection {
    InvalidInput = 1,
    InvalidSignature = 2,
    InvalidChild = 3,
    ProvingFailed = 4,
    /// The body is not a request.
    Malformed = 5,
}

impl From<AggregateError> for Rejection {
    fn from(error: AggregateError) -> Self {
        match error {
            AggregateError::InvalidInput(_) => Self::InvalidInput,
            AggregateError::InvalidSignature => Self::InvalidSignature,
            AggregateError::InvalidChild => Self::InvalidChild,
            AggregateError::ProvingFailed => Self::ProvingFailed,
        }
    }
}

/// A request, decoded.
pub enum Call {
    Aggregate(Request),
    /// Claims, and their proof's bytes.
    Verify(AggregateClaims, Vec<u8>),
}

/// An `aggregate` call, its children's proofs still bytes.
#[derive(Debug, PartialEq)]
pub struct Request {
    pub children: Vec<(AggregateClaims, Vec<u8>)>,
    pub raw_lean_xmss: Vec<(LeanXmssPublicKey, lean_xmss::Message, LeanXmssSignature)>,
    pub raw_lean_sphincs: Vec<(LeanSphincsPublicKey, lean_sphincs::Message, LeanSphincsSignature)>,
    pub blobs: Vec<lean_da::Blob>,
    /// `None` for every covered claim.
    pub output: Option<AggregateClaims>,
    pub log_inv_rate: usize,
}

/// The operation a body asks for, read without decoding the rest: a verify
/// goes to the verify threads, an aggregate waits for the prover.
pub fn is_verify(body: &[u8]) -> bool {
    body.first() == Some(&OP_VERIFY)
}

pub fn decode_request(body: &[u8]) -> Result<Call, (Rejection, String)> {
    let malformed = |what: &str| (Rejection::Malformed, format!("malformed request: {what}"));
    let mut c = Cursor(body);
    let call = match c.u8() {
        Some(OP_AGGREGATE) => Call::Aggregate(decode_aggregate(&mut c, malformed)?),
        Some(OP_VERIFY) => {
            let claims = c.claims().ok_or_else(|| malformed("claims"))?;
            let proof = c.bytes().ok_or_else(|| malformed("proof"))?;
            Call::Verify(claims, proof.to_vec())
        }
        Some(op) => return Err((Rejection::Malformed, format!("unknown operation {op}"))),
        None => return Err(malformed("empty")),
    };
    if !c.is_empty() {
        return Err(malformed("trailing bytes"));
    }
    Ok(call)
}

fn decode_aggregate(
    c: &mut Cursor,
    malformed: impl Fn(&str) -> (Rejection, String),
) -> Result<Request, (Rejection, String)> {
    let log_inv_rate = c.u8().ok_or_else(|| malformed("no rate"))?.into();

    // Each child is at least its two lengths.
    let n = c.count(16).ok_or_else(|| malformed("children"))?;
    // Checked before decoding: a decoded child takes more memory than its bytes.
    if n > MAX_RECURSIONS {
        return Err((Rejection::InvalidInput, "more than MAX_RECURSIONS children".into()));
    }
    let mut children = Vec::with_capacity(n);
    for i in 0..n {
        let claims = c.claims().ok_or_else(|| malformed(&format!("child {i}'s claims")))?;
        let proof = c.bytes().ok_or_else(|| malformed(&format!("child {i}'s proof")))?;
        children.push((claims, proof.to_vec()));
    }

    // `count` checked that every record follows, so the reads below succeed.
    let n = c.count(XMSS_RECORD).ok_or_else(|| malformed("leanXMSS signatures"))?;
    let raw_lean_xmss = (0..n)
        .map(|_| {
            let public_key = LeanXmssPublicKey::from_bytes(c.array().unwrap());
            let message = *c.array().unwrap();
            (public_key, message, LeanXmssSignature::from_bytes(c.array().unwrap()))
        })
        .collect();
    let n = c.count(SPHINCS_RECORD).ok_or_else(|| malformed("leanSPHINCS signatures"))?;
    let raw_lean_sphincs = (0..n)
        .map(|_| {
            let public_key = LeanSphincsPublicKey::from_bytes(c.array().unwrap());
            let message = *c.array().unwrap();
            (public_key, message, LeanSphincsSignature::from_bytes(c.array().unwrap()))
        })
        .collect();

    let n = c.count(BLOB_BYTES).ok_or_else(|| malformed("blobs"))?;
    if n > lean_da::DA_MAX_ROWS {
        let message = format!("{n} blobs: expected 1 to {}", lean_da::DA_MAX_ROWS);
        return Err((Rejection::InvalidInput, message));
    }
    let blobs = (0..n)
        .map(|_| {
            let symbols = c.take(BLOB_BYTES).unwrap().as_chunks::<16>().0;
            std::array::from_fn(|i| u128::from_le_bytes(symbols[i]))
        })
        .collect();

    let output = match c.u8() {
        Some(0) => None,
        Some(1) => Some(c.claims().ok_or_else(|| malformed("output claims"))?),
        _ => return Err(malformed("output")),
    };
    Ok(Request { children, raw_lean_xmss, raw_lean_sphincs, blobs, output, log_inv_rate })
}

/// An `aggregate` response: status 0, the claims and the proof; or a
/// rejection's status and message.
pub fn encode_response(
    outcome: &Result<(AggregateClaims, Vec<u8>), (Rejection, String)>,
) -> Vec<u8> {
    let mut out = Vec::new();
    match outcome {
        Ok((claims, proof)) => {
            out.push(OK);
            put_bytes(&mut out, &claims.to_bytes());
            put_bytes(&mut out, proof);
        }
        Err((rejection, message)) => {
            out.push(*rejection as u8);
            put_bytes(&mut out, message.as_bytes());
        }
    }
    out
}

/// A `verify` response: status 0 and whether the proof is valid.
pub fn encode_verdict(valid: bool) -> [u8; 2] {
    [OK, valid.into()]
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend((bytes.len() as u64).to_le_bytes());
    out.extend(bytes);
}

/// Reads fields off the front of a body.
struct Cursor<'a>(&'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, n: usize) -> Option<&'a [u8]> {
        let (head, rest) = self.0.split_at_checked(n)?;
        self.0 = rest;
        Some(head)
    }

    fn array<const N: usize>(&mut self) -> Option<&'a [u8; N]> {
        self.take(N).map(|bytes| bytes.try_into().unwrap())
    }

    fn u8(&mut self) -> Option<u8> {
        self.array::<1>().map(|[x]| *x)
    }

    fn u64(&mut self) -> Option<u64> {
        self.array().map(|x| u64::from_le_bytes(*x))
    }

    fn bytes(&mut self) -> Option<&'a [u8]> {
        let len = usize::try_from(self.u64()?).ok()?;
        self.take(len)
    }

    fn claims(&mut self) -> Option<AggregateClaims> {
        AggregateClaims::from_bytes(self.bytes()?)
    }

    /// A count of records of `size` bytes each, all of which must follow: a
    /// forged count allocates nothing.
    fn count(&mut self, size: usize) -> Option<usize> {
        let n = usize::try_from(self.u64()?).ok()?;
        (n.checked_mul(size)? <= self.0.len()).then_some(n)
    }

    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

#[cfg(test)]
pub(crate) mod tests;
