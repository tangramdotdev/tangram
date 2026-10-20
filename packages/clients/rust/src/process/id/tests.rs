use {super::*, std::time::Duration};

#[test]
fn a_derived_id_is_a_uuidv7_that_its_proof_verifies() {
	let runner = tg::runner::Id::new();
	let (id, proof) = tg::process::Id::with_runner(&runner);
	let tg::id::Body::UuidV7(body) = id.body() else {
		panic!("expected a uuidv7 body");
	};
	assert_eq!(uuid::Uuid::from_bytes(*body).get_version_num(), 7);
	proof.verify(&id, &runner).unwrap();
}

#[test]
fn a_proof_does_not_verify_for_another_runner() {
	let runner = tg::runner::Id::new();
	let (id, proof) = tg::process::Id::with_runner(&runner);
	let other = tg::runner::Id::new();
	assert!(proof.verify(&id, &other).is_err());
}

#[test]
fn a_proof_does_not_verify_with_another_salt() {
	let runner = tg::runner::Id::new();
	let (id, _) = tg::process::Id::with_runner(&runner);
	let proof = Proof::V1(ProofV1 { salt: Salt::new() });
	assert!(proof.verify(&id, &runner).is_err());
}

#[test]
fn a_proof_does_not_verify_a_tampered_id() {
	let runner = tg::runner::Id::new();
	let (id, proof) = tg::process::Id::with_runner(&runner);
	let tg::id::Body::UuidV7(mut body) = id.body().clone() else {
		panic!("expected a uuidv7 body");
	};
	body[15] ^= 1;
	let id = tg::Id::new(tg::id::Kind::Process, tg::id::Body::UuidV7(body));
	let id = tg::process::Id::try_from(id).unwrap();
	assert!(proof.verify(&id, &runner).is_err());
}

#[test]
fn a_proof_does_not_verify_a_home_minted_id() {
	let runner = tg::runner::Id::new();
	let (_, proof) = tg::process::Id::with_runner(&runner);
	let id = tg::process::Id::new();
	assert!(proof.verify(&id, &runner).is_err());
}

#[test]
fn a_recent_proof_verifies_only_within_the_window() {
	let runner = tg::runner::Id::new();
	let (id, proof) = tg::process::Id::with_runner(&runner);
	let timestamp = id.timestamp().unwrap();
	let window = Duration::from_mins(1);
	for now in [timestamp, timestamp + 60_000, timestamp - 60_000] {
		proof.verify_recent(&id, &runner, now, window).unwrap();
	}
	for now in [timestamp + 60_001, timestamp - 60_001] {
		assert!(proof.verify_recent(&id, &runner, now, window).is_err());
	}
}

#[test]
fn a_proof_round_trips_through_json_and_tangram() {
	let runner = tg::runner::Id::new();
	let (id, proof) = tg::process::Id::with_runner(&runner);
	let json = serde_json::to_string(&proof).unwrap();
	let decoded: Proof = serde_json::from_str(&json).unwrap();
	decoded.verify(&id, &runner).unwrap();
	let bytes = tangram_serialize::to_vec(&proof).unwrap();
	let decoded: Proof = tangram_serialize::from_slice(&bytes).unwrap();
	decoded.verify(&id, &runner).unwrap();
}
