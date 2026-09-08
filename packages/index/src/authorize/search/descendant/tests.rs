use super::*;

#[test]
fn principal_and_token_proofs_finish_before_public_enumeration() {
	let config = crate::authorize::SearchConfig {
		max_edges: 2,
		max_nodes: 3,
		..Default::default()
	};
	let parent = object_key(
		0,
		tg::authorization::permission::object::Permission::Subtree,
	);
	let child = object_key(1, tg::authorization::permission::object::Permission::Node);
	let body = tg::authorization::Body {
		expires_at: i64::MAX,
		permissions: vec![parent.1],
		resource: parent.0.clone(),
	};
	for token in [None, Some((&body, &parent.0))] {
		let principal = if token.is_some() {
			tg::Principal::Anonymous
		} else {
			tg::Principal::Runner(tg::runner::Id::new())
		};
		let mut state = State::default();
		let mut search = Search::new(config, &principal, &state, vec![child.clone()], token);
		if token.is_none() {
			let reads = search.take_reads(&mut state, 8);
			let [read] = reads.try_into().unwrap();
			let subject = principal.to_subject();
			assert!(
				matches!(&read, Read::SubjectGrants { subject: value, .. } if *value == subject)
			);
			let output = grants(&subject, std::slice::from_ref(&parent));
			search.apply(&mut state, read, output).unwrap();
		}

		let reads = search.take_reads(&mut state, 8);
		let [read] = reads.try_into().unwrap();
		assert!(matches!(&read, Read::DescendantChecks(_)));
		search
			.apply(&mut state, read, ReadOutput::Bools(vec![true]))
			.unwrap();

		assert!(search.take_reads(&mut state, 8).is_empty());
		assert_eq!(search.finish(&mut state), Outcome::Authorized);
		assert!(state.is_authorized(&child));
	}
}

#[test]
fn public_grants_have_an_independent_budget() {
	let config = crate::authorize::SearchConfig {
		max_edges: 1,
		max_nodes: 2,
		..Default::default()
	};
	let principal = tg::Principal::Runner(tg::runner::Id::new());
	let target = object_key(0, tg::authorization::permission::object::Permission::Node);
	let unrelated = object_key(1, tg::authorization::permission::object::Permission::Node);
	let mut state = State::default();
	let mut search = Search::new(config, &principal, &state, vec![target.clone()], None);

	let reads = search.take_reads(&mut state, 8);
	let [read] = reads.try_into().unwrap();
	let subject = principal.to_subject();
	assert!(matches!(&read, Read::SubjectGrants { subject: value, .. } if *value == subject));
	let output = grants(&subject, &[unrelated]);
	search.apply(&mut state, read, output).unwrap();

	let reads = search.take_reads(&mut state, 8);
	let [read] = reads.try_into().unwrap();
	let subject = tg::authorization::Subject::Public;
	assert!(matches!(&read, Read::SubjectGrants { subject: value, .. } if *value == subject));
	let output = grants(&subject, std::slice::from_ref(&target));
	search.apply(&mut state, read, output).unwrap();

	assert!(search.take_reads(&mut state, 8).is_empty());
	assert_eq!(search.finish(&mut state), Outcome::Authorized);
	assert!(state.is_authorized(&target));
}

#[test]
fn adding_targets_resumes_both_traversals() {
	let config = crate::authorize::SearchConfig {
		max_edges: 1,
		max_nodes: 2,
		..Default::default()
	};
	let principal = tg::Principal::Runner(tg::runner::Id::new());
	let private = [
		object_key(0, tg::authorization::permission::object::Permission::Node),
		object_key(1, tg::authorization::permission::object::Permission::Node),
	];
	let public = [
		object_key(2, tg::authorization::permission::object::Permission::Node),
		object_key(3, tg::authorization::permission::object::Permission::Node),
	];
	let mut state = State::default();
	let mut search = Search::new(config, &principal, &state, vec![private[1].clone()], None);

	for (subject, keys) in [
		(principal.to_subject(), &private),
		(tg::authorization::Subject::Public, &public),
	] {
		let reads = search.take_reads(&mut state, 8);
		let [read] = reads.try_into().unwrap();
		assert!(matches!(&read, Read::SubjectGrants { subject: value, .. } if *value == subject));
		let output = grants(&subject, keys);
		search.apply(&mut state, read, output).unwrap();
	}
	assert!(search.take_reads(&mut state, 8).is_empty());
	assert_eq!(search.finish(&mut state), Outcome::Exhausted);
	search.reset_visited_if_complete();

	search.add_targets(config, vec![public[1].clone()]);
	for (subject, keys) in [
		(principal.to_subject(), &private),
		(tg::authorization::Subject::Public, &public),
	] {
		let reads = search.take_reads(&mut state, 8);
		let [read] = reads.try_into().unwrap();
		assert!(matches!(&read, Read::SubjectGrants { subject: value, .. } if *value == subject));
		let output = grants(&subject, keys);
		search.apply(&mut state, read, output).unwrap();
	}

	assert!(search.take_reads(&mut state, 8).is_empty());
	assert_eq!(search.finish(&mut state), Outcome::Authorized);
	assert!(state.is_authorized(&private[1]));
	assert!(state.is_authorized(&public[1]));
}

fn grants(subject: &tg::authorization::Subject, keys: &[Key]) -> ReadOutput {
	let grants = keys
		.iter()
		.map(|(resource, permission)| super::super::Grant {
			creator: None,
			implicit: false,
			permission: *permission,
			resource: resource.clone(),
			subject: subject.clone(),
		})
		.collect();
	ReadOutput::Grants {
		after: None,
		grants,
	}
}

fn object_key(value: u8, permission: tg::authorization::permission::object::Permission) -> Key {
	let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![value].into());
	let permission = tg::authorization::Permission::Object(permission);
	(object.into(), permission)
}
