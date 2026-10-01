#[test]
fn storage_proofs_preserve_explicit_flags() {
	use tangram_client as tg;
	for storage in tg::process::storage::Set::all().iter() {
		let storage = tg::storage::Set::Process(tg::process::storage::Set::from_storage(storage));
		let permissions = tangram_index::verify::storage_permissions(
			storage,
			tg::authorization::permission::Set::Process(
				tg::authorization::permission::process::Set::empty(),
			),
		);
		assert_eq!(permissions.iter().count(), 1);
		assert_eq!(
			super::permission_storage(permissions.iter().next().unwrap()),
			Some(storage)
		);
	}
	for storage in [
		tg::object::storage::Set::NODE,
		tg::object::storage::Set::SUBTREE,
	] {
		let storage = tg::storage::Set::Object(storage);
		let permissions = tangram_index::verify::storage_permissions(
			storage,
			tg::authorization::permission::Set::Object(
				tg::authorization::permission::object::Set::empty(),
			),
		);
		assert_eq!(permissions.iter().count(), 1);
		assert_eq!(
			super::permission_storage(permissions.iter().next().unwrap()),
			Some(storage)
		);
	}
}

#[test]
fn exhaustion_preserves_partial_proofs_and_independent_batch_outcomes() {
	use {
		crate::{
			Session,
			authorization::{Outcome, Output},
		},
		tangram_client as tg,
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
	let outputs = Session::verify_outputs(
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
fn descendant_storage_waits_for_indexing_without_requerying_the_sync() {
	use {std::time::Duration, tokio::time::Instant};
	let mut entry = entry();
	let deadline = Instant::now() + Duration::from_secs(5);
	entry.await_indexing(deadline);
	assert!(entry.pending());
	entry.retry();
	let (abort, _) = futures::future::AbortHandle::new_pair();
	assert!(!entry.start(abort));
	assert_eq!(entry.deadline(), Some(deadline));
	entry.await_indexing(deadline + Duration::from_secs(5));
	assert_eq!(entry.deadline(), Some(deadline));
	entry.cancel();
	assert!(!entry.pending());
	assert_eq!(entry.deadline(), None);
}

#[test]
fn partial_responses_preserve_the_deadline_and_do_not_duplicate_in_flight_requests() {
	use {std::time::Duration, tokio::time::Instant};
	let mut entry = entry();
	let (abort, _) = futures::future::AbortHandle::new_pair();
	assert!(entry.start(abort));
	let deadline = Instant::now() + Duration::from_secs(5);
	entry.defer(deadline);
	entry.retry();
	let (abort, _) = futures::future::AbortHandle::new_pair();
	assert!(entry.start(abort));
	// A timer tick cannot queue a duplicate while a retry is in flight.
	entry.retry();
	let (abort, _) = futures::future::AbortHandle::new_pair();
	assert!(!entry.start(abort));
	entry.defer(deadline + Duration::from_secs(5));
	assert_eq!(entry.deadline(), Some(deadline));
	entry.cancel();
	entry.retry();
	let (abort, _) = futures::future::AbortHandle::new_pair();
	assert!(!entry.start(abort));
	assert!(!entry.pending());
}

#[test]
fn cancellation_aborts_the_request_and_prevents_retries() {
	let mut entry = entry();
	let (abort, _) = futures::future::AbortHandle::new_pair();
	assert!(entry.start(abort.clone()));
	entry.cancel();
	assert!(abort.is_aborted());
	assert!(!entry.pending());
	assert_eq!(entry.deadline(), None);
	let (abort, _) = futures::future::AbortHandle::new_pair();
	assert!(!entry.start(abort));
}

fn entry() -> super::Entry {
	use tangram_client as tg;
	let node = tg::blob::Id::new(b"descendant").into();
	let arg = tg::sync::control::VerifyClientRequestArg {
		node,
		permissions: tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::empty(),
		),
		storage: tg::storage::Set::Object(tg::object::storage::Set::SUBTREE),
	};
	let request = super::Request {
		arg,
		index_position: 0,
		position: 0,
		sync: tg::sync::Id::new(),
	};
	super::Entry {
		request,
		state: super::RequestState::Queued { deadline: None },
	}
}

#[test]
fn permission_capture_preserves_partial_proofs_without_hiding_exhaustion() {
	use {
		crate::{
			Session,
			authorization::{Outcome, Output},
		},
		tangram_client as tg,
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
	let outputs = Session::verify_outputs(
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
