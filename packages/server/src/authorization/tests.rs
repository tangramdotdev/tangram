use {
	super::{Outcome, Output, Proofs},
	tangram_client::prelude::*,
};

#[test]
fn merge_permissions_and_expirations() {
	let node = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::NODE,
	);
	let subtree = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::SUBTREE,
	);
	let mut requested = node;
	requested.insert(subtree);
	let mut proofs = Proofs::default();
	let output = Output {
		expires_at: Some(10),
		outcome: Outcome::Satisfied,
		permissions: node,
	};
	proofs.insert(requested, output);
	let output = Output {
		expires_at: Some(20),
		outcome: Outcome::Satisfied,
		permissions: subtree,
	};
	proofs.insert(requested, output);
	let output = proofs.output(requested).unwrap();
	assert!(output.permissions.contains(requested));
	assert_eq!(output.expires_at, Some(20));
}

#[test]
fn retain_independent_permissions() {
	let node = tg::authorization::permission::Set::Process(
		tg::authorization::permission::process::Set::NODE,
	);
	let parent = tg::authorization::permission::Set::Process(
		tg::authorization::permission::process::Set::PARENT,
	);
	let mut requested = node;
	requested.insert(parent);
	let mut proofs = Proofs::default();
	let output = Output {
		expires_at: None,
		outcome: Outcome::Satisfied,
		permissions: node,
	};
	proofs.insert(requested, output);
	let output = Output {
		expires_at: Some(10),
		outcome: Outcome::Satisfied,
		permissions: parent,
	};
	proofs.insert(requested, output);
	assert!(
		proofs
			.output(requested)
			.unwrap()
			.permissions
			.contains(requested)
	);
	let output = proofs.output(requested).unwrap();
	assert!(output.permissions.contains(node));
	assert!(output.permissions.contains(parent));
	assert_eq!(output.expires_at, Some(10));
}

#[test]
fn reject_unrelated_permission_kinds() {
	let requested = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::NODE,
	);
	let permissions = tg::authorization::permission::Set::Process(
		tg::authorization::permission::process::Set::NODE,
	);
	let output = Output {
		expires_at: None,
		outcome: Outcome::Satisfied,
		permissions,
	};
	let mut proofs = Proofs::default();
	proofs.insert(requested, output);
	assert!(proofs.output(requested).is_none());
}

#[test]
fn preserve_known_resource_denial() {
	let requested =
		tg::authorization::permission::Set::Group(tg::authorization::permission::group::Set::WRITE);
	let mut proofs = Proofs::default();
	assert!(proofs.output(requested).is_none());
	let output = Output {
		expires_at: None,
		outcome: Outcome::Satisfied,
		permissions: requested.empty_like(),
	};
	proofs.insert(requested, output);
	let output = proofs.output(requested).unwrap();
	assert!(output.permissions.is_empty());
	assert!(!output.permissions.contains(requested));
}

#[test]
fn exhaustion_is_distinct_from_denial_at_callers() {
	let permissions = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::empty(),
	);
	let denied = Output {
		expires_at: None,
		outcome: Outcome::Unsatisfied,
		permissions,
	};
	let exhausted = Output {
		outcome: Outcome::Exhausted,
		..denied
	};
	assert!(denied.check_exhaustion().is_ok());
	assert!(exhausted.check_exhaustion().is_err());
	assert!(super::check_exhaustion(&[denied]).is_ok());
	assert!(super::check_exhaustion(&[denied, exhausted]).is_err());
}
