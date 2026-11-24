use super::*;

/// The client's half, as the sidecar's own client wrote it, without the
/// frame's length.
pub(crate) fn encode_request(request: &Request) -> Vec<u8> {
    let mut out = vec![OP_AGGREGATE, request.log_inv_rate as u8];
    out.extend((request.children.len() as u64).to_le_bytes());
    for (claims, proof) in &request.children {
        put_bytes(&mut out, &claims.to_bytes());
        put_bytes(&mut out, proof);
    }
    out.extend((request.raw_lean_xmss.len() as u64).to_le_bytes());
    for (public_key, message, signature) in &request.raw_lean_xmss {
        out.extend(public_key.to_bytes());
        out.extend(message);
        out.extend(signature.to_bytes());
    }
    out.extend((request.raw_lean_sphincs.len() as u64).to_le_bytes());
    for (public_key, message, signature) in &request.raw_lean_sphincs {
        out.extend(public_key.to_bytes());
        out.extend(message);
        out.extend(signature.to_bytes());
    }
    out.extend((request.blobs.len() as u64).to_le_bytes());
    for blob in &request.blobs {
        for symbol in blob {
            out.extend(symbol.to_le_bytes());
        }
    }
    match &request.output {
        None => out.push(0),
        Some(claims) => {
            out.push(1);
            put_bytes(&mut out, &claims.to_bytes());
        }
    }
    out
}

fn request() -> Request {
    let xmss = (
        LeanXmssPublicKey::from_bytes(&[1; lean_xmss::PUB_KEY_SIZE]),
        [2; lean_xmss::MESSAGE_LEN],
        LeanXmssSignature::from_bytes(&std::array::from_fn(|i| i as u8)),
    );
    let sphincs = (
        LeanSphincsPublicKey::from_bytes(&[3; lean_sphincs::PUB_KEY_SIZE]),
        [4; lean_sphincs::MESSAGE_LEN],
        LeanSphincsSignature::from_bytes(&std::array::from_fn(|i| (i * 7) as u8)),
    );
    let only = AggregateClaims {
        lean_xmss: vec![(xmss.0, xmss.1)],
        lean_sphincs: vec![(sphincs.0, sphincs.1)],
        da_commitment: Some([5; 32]),
    };
    Request {
        children: vec![(only.clone(), vec![6, 7, 8])],
        raw_lean_xmss: vec![xmss],
        raw_lean_sphincs: vec![sphincs],
        blobs: vec![std::array::from_fn(|i| ((i as u128) << 100) | i as u128)],
        output: Some(only),
        log_inv_rate: 3,
    }
}

#[test]
fn a_request_decodes_to_what_was_encoded() {
    let request = request();
    let Ok(Call::Aggregate(decoded)) = decode_request(&encode_request(&request)) else {
        panic!("not an aggregate request")
    };
    assert_eq!(decoded, request);
}

/// Every truncation, and a trailing byte, is refused, never a panic.
#[test]
fn truncations_and_trailing_bytes_are_refused() {
    let bytes = encode_request(&request());
    assert!(!is_verify(&bytes));
    for len in 0..bytes.len() {
        assert!(decode_request(&bytes[..len]).is_err(), "a prefix of {len} bytes decoded");
    }
    assert!(decode_request(&[&bytes[..], &[0]].concat()).is_err());
}

#[test]
fn counts_past_the_body_or_the_bounds_are_refused() {
    let mut huge = vec![OP_AGGREGATE, 1];
    huge.extend(u64::MAX.to_le_bytes());
    assert!(matches!(decode_request(&huge), Err((Rejection::Malformed, _))));

    let mut many = request();
    many.children = vec![(AggregateClaims::default(), Vec::new()); MAX_RECURSIONS + 1];
    let refused = decode_request(&encode_request(&many));
    assert!(matches!(refused, Err((Rejection::InvalidInput, _))));

    let mut many = request();
    many.blobs = vec![[0; lean_da::BLOB_SYMBOLS]; lean_da::DA_MAX_ROWS + 1];
    let refused = decode_request(&encode_request(&many));
    assert!(matches!(refused, Err((Rejection::InvalidInput, _))));
}

#[test]
fn a_verify_request_decodes() {
    let claims = request().output.unwrap();
    let mut bytes = vec![OP_VERIFY];
    put_bytes(&mut bytes, &claims.to_bytes());
    put_bytes(&mut bytes, &[9, 9]);
    assert!(is_verify(&bytes));
    let Ok(Call::Verify(decoded, proof)) = decode_request(&bytes) else { panic!("not a verify") };
    assert_eq!((decoded, proof), (claims, vec![9, 9]));
    assert!(matches!(decode_request(&[7]), Err((Rejection::Malformed, _))));
}

#[test]
fn responses_carry_status_then_fields() {
    let claims = AggregateClaims::default();
    let mut expected = vec![OK];
    put_bytes(&mut expected, &claims.to_bytes());
    put_bytes(&mut expected, &[1, 2, 3]);
    assert_eq!(encode_response(&Ok((claims, vec![1, 2, 3]))), expected);

    let refused = encode_response(&Err((Rejection::InvalidInput, "no".into())));
    assert_eq!(refused, [&[1u8][..], &2u64.to_le_bytes(), b"no"].concat());
    assert_eq!(encode_verdict(true), [OK, 1]);
}
