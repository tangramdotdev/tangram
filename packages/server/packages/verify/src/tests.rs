#[test]
fn exhaustion_preserves_partial_proofs_and_independent_batch_outcomes() {
	use {
		crate::Component,
		tangram_client as tg,
		tangram_server_core::authorization::{Outcome, Output},
	};
	let node = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::NODE,
	);
	let mut requested = node;
	requested.insert(tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::SUBTREE,
	));
	let storage = tg::storage::Set::Object(tg::object::storage::Set::NODE);
	let empty_storage = storage.empty_like();
	let partial = Output {
		expires_at: Some(42),
		outcome: Outcome::Unsatisfied,
		permissions: node,
	};
	let complete = Output {
		expires_at: None,
		outcome: Outcome::Satisfied,
		permissions: requested,
	};
	let requirements = [
		(requested, storage),
		(requested, empty_storage),
		(node, empty_storage),
	];
	let outputs = Component::verify_outputs(
		vec![Some(partial), Some(complete), None],
		vec![storage, empty_storage, empty_storage],
		&requirements,
		&[true, true, false],
	);
	assert_eq!(outputs[0].outcome, Outcome::Exhausted);
	assert_eq!(outputs[0].permissions, node);
	assert_eq!(outputs[0].storage, storage);
	assert_eq!(outputs[0].expires_at, Some(42));
	assert_eq!(outputs[1].outcome, Outcome::Satisfied);
	assert_eq!(outputs[2].outcome, Outcome::Unsatisfied);
	assert!(outputs[2].permissions.is_empty());
}

#[test]
fn permission_capture_preserves_partial_proofs_without_hiding_exhaustion() {
	use {
		crate::Component,
		tangram_client as tg,
		tangram_server_core::authorization::{Outcome, Output},
	};
	let node = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::NODE,
	);
	let storage = tg::storage::Set::Object(tg::object::storage::Set::empty());
	let partial = Output {
		expires_at: None,
		outcome: Outcome::Exhausted,
		permissions: node,
	};
	let mut requested = node;
	requested.insert(tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::SUBTREE,
	));
	let requirements = [(requested, storage); 2];
	let outputs = Component::verify_outputs(
		vec![None, Some(partial)],
		vec![storage; 2],
		&requirements,
		&[true; 2],
	);
	assert_eq!(outputs[0].outcome, Outcome::Exhausted);
	assert!(outputs[0].permissions.is_empty());
	assert_eq!(outputs[1].outcome, Outcome::Exhausted);
	assert_eq!(outputs[1].permissions, node);
}
