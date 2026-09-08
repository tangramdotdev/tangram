use {super::*, crate::authorize::SearchConfig};

#[test]
fn shorter_visits_reopen_pruned_dependencies_without_spending_another_node() {
	for limit in [1, 64] {
		for [root, middle, parent] in keys() {
			let mut state = State::default();
			let mut search = search(&root, &state);
			assert!(search.add_dependency(&mut state, &root, middle.clone(), 2));
			let facts = state.set_ancestor_facts(middle.0.clone(), AncestorNodeFacts::default());
			search.expand_node(&mut state, 2, &middle, &facts).unwrap();
			assert!(search.add_dependency(&mut state, &middle, parent.clone(), 3));
			assert_eq!(search.budget.nodes, 2);

			assert!(search.add_dependency(&mut state, &root, middle.clone(), 1));
			let reads = drain(&mut search, &mut state, limit, empty_output);

			assert!(reads.iter().any(|read| matches!(
				read,
				Read::AncestorNode { depth: 2, key, .. } if key == &parent
			)));
			assert_eq!(parent_reads(&reads, &middle), 1);
			assert_eq!(search.budget.nodes, 3);
			assert!(search.incomplete.is_empty());
			assert_eq!(state.ancestor_or_descendant(&root), Outcome::Denied);
		}
	}
}

#[test]
fn shorter_visits_reopen_grant_memberships() {
	for limit in [1, 64] {
		let [root, middle, _] = keys()[0].clone();
		let outer = tg::group::Id::new();
		let inner = tg::group::Id::new();
		let subject = tg::authorization::Subject::Group(outer.clone());
		let mut state = State::default();
		let mut search = search(&root, &state);
		search.budget.config.max_nodes = 4;
		assert!(search.add_dependency(&mut state, &root, middle.clone(), 2));
		let grant = super::super::Grant {
			creator: None,
			implicit: false,
			permission: middle.1,
			resource: middle.0.clone(),
			subject: subject.clone(),
		};
		let facts = AncestorNodeFacts {
			grants: vec![grant],
			..Default::default()
		};
		let facts = state.set_ancestor_facts(middle.0.clone(), facts);
		search.expand_node(&mut state, 2, &middle, &facts).unwrap();
		let page = MembershipPage {
			container: subject,
			continuation: None,
			members: vec![inner.clone().into()],
		};
		search.apply_members(&mut state, &middle, 2, page).unwrap();
		assert_eq!(search.budget.nodes, 3);

		assert!(search.add_dependency(&mut state, &root, middle.clone(), 1));
		let reads = drain(&mut search, &mut state, limit, |read| match read {
			Read::GroupMembers { group, .. } if group == &outer => ReadOutput::Ids {
				after: None,
				ids: vec![inner.clone().into()],
			},
			_ => empty_output(read),
		});

		assert!(reads.iter().any(|read| matches!(
			read,
			Read::GroupMembers { depth: 2, group, .. } if group == &inner
		)));
		assert_eq!(parent_reads(&reads, &middle), 1);
		assert_eq!(search.budget.nodes, 4);
		assert!(search.incomplete.is_empty());
	}
}

#[test]
fn parent_pages_wait_for_the_previous_read_batch() {
	for [root, parent, _] in keys() {
		let mut state = State::default();
		let mut search = search(&root, &state);
		loop {
			let reads = search.take_reads(&mut state, 64).unwrap();
			assert!(!reads.is_empty());
			let parents = parent_reads(&reads, &root) > 0;
			for read in reads {
				let output = match &read {
					Read::ObjectParents { .. } | Read::ProcessParents { .. } => ReadOutput::Ids {
						after: Some(vec![1]),
						ids: vec![parent.0.clone()],
					},
					_ => empty_output(&read),
				};
				search.apply(&mut state, read, output).unwrap();
			}
			if parents {
				break;
			}
		}

		let reads = search.take_reads(&mut state, 64).unwrap();
		assert!(!reads.is_empty());
		assert!(
			reads
				.iter()
				.all(|read| matches!(read, Read::AncestorNode { key, .. } if key == &parent))
		);
		for read in reads {
			let output = empty_output(&read);
			search.apply(&mut state, read, output).unwrap();
		}

		let reads = search.take_reads(&mut state, 64).unwrap();
		assert_eq!(reads.len(), 1);
		assert_eq!(parent_reads(&reads, &parent), 1);
		for read in reads {
			let output = empty_output(&read);
			search.apply(&mut state, read, output).unwrap();
		}

		let reads = search.take_reads(&mut state, 64).unwrap();
		assert_eq!(reads.len(), 1);
		assert!(matches!(
			&reads[0],
			Read::ObjectParents { after: Some(_), dependent, .. }
				| Read::ProcessParents { after: Some(_), dependent, .. } if dependent == &root
		));
	}
}

#[test]
fn pruned_visits_remain_incomplete_until_they_can_be_expanded() {
	for [root, middle, parent] in keys() {
		let mut state = State::default();
		let mut search = search(&root, &state);
		search.budget.config.max_nodes = 2;
		assert!(search.add_dependency(&mut state, &root, middle, 1));
		assert!(search.add_dependency(&mut state, &root, parent.clone(), 3));
		assert!(search.add_dependency(&mut state, &root, parent.clone(), 1));

		let reads = drain(&mut search, &mut state, 64, empty_output);

		assert!(
			!reads
				.iter()
				.any(|read| matches!(read, Read::AncestorNode { key, .. } if key == &parent))
		);
		assert_eq!(search.budget.nodes, 2);
		assert!(search.incomplete.contains(&root));
		assert_eq!(state.ancestor_or_descendant(&root), Outcome::Pending);
	}
}

#[test]
fn a_deferred_parent_page_uses_the_improved_depth() {
	for [root, middle, parent] in keys() {
		let mut state = State::default();
		let mut search = search(&root, &state);
		assert!(search.add_dependency(&mut state, &root, middle.clone(), 2));
		search.queue_parents(&mut state, 2, &middle).unwrap();
		let read = match &middle.1 {
			tg::authorization::Permission::Object(_) => Read::ObjectParents {
				after: Some(vec![1]),
				dependent: middle.clone(),
				depth: 2,
				limit: 1,
				object: middle.0.clone().try_into().unwrap(),
			},
			tg::authorization::Permission::Process(permission) => Read::ProcessParents {
				after: Some(vec![1]),
				dependent: middle.clone(),
				depth: 2,
				limit: 1,
				permission: *permission,
				process: middle.0.clone().try_into().unwrap(),
			},
			_ => unreachable!(),
		};
		assert!(search.add_dependency(&mut state, &root, middle, 1));
		let output = ReadOutput::Ids {
			after: None,
			ids: vec![parent.0.clone()],
		};
		search.apply(&mut state, read, output).unwrap();

		let reads = drain(&mut search, &mut state, 64, empty_output);

		assert!(reads.iter().any(|read| matches!(
			read,
			Read::AncestorNode { depth: 2, key, .. } if key == &parent
		)));
		assert!(search.incomplete.is_empty());
	}
}

fn drain(
	search: &mut Search,
	state: &mut State,
	limit: usize,
	mut output: impl FnMut(&Read) -> ReadOutput,
) -> Vec<Read> {
	let mut all_reads = Vec::new();
	for _ in 0..100 {
		let reads = search.take_reads(state, limit).unwrap();
		if reads.is_empty() {
			search.finish(state);
			return all_reads;
		}
		for read in reads {
			let output = output(&read);
			all_reads.push(read.clone());
			search.apply(state, read, output).unwrap();
		}
	}
	panic!("the ancestor search did not finish");
}

fn empty_output(read: &Read) -> ReadOutput {
	match read {
		Read::AncestorChecks(checks) => ReadOutput::Bools(vec![false; checks.candidates.len()]),
		Read::AncestorNode { read, .. } => match read {
			AncestorNodeRead::ObjectProcesses { .. } => ReadOutput::ObjectProcesses {
				after: None,
				processes: Vec::new(),
			},
			AncestorNodeRead::Process { .. } => ReadOutput::Process(None),
			AncestorNodeRead::ResourceGrants { .. } => ReadOutput::Grants {
				after: None,
				grants: Vec::new(),
			},
			AncestorNodeRead::TargetTags { .. } => ReadOutput::Tags {
				after: None,
				tags: Vec::new(),
			},
			_ => panic!("unexpected ancestor node read: {read:?}"),
		},
		Read::GroupMembers { .. }
		| Read::ObjectParents { .. }
		| Read::OrganizationMembers { .. }
		| Read::ProcessParents { .. } => ReadOutput::Ids {
			after: None,
			ids: Vec::new(),
		},
		_ => panic!("unexpected ancestor read: {read:?}"),
	}
}

fn keys() -> [[Key; 3]; 2] {
	let objects = [0, 1, 2].map(|value| {
		let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![value].into());
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		(object.into(), permission)
	});
	let processes = std::array::from_fn(|_| {
		let process = tg::process::Id::new();
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Subtree,
		);
		(process.into(), permission)
	});
	[objects, processes]
}

fn parent_reads(reads: &[Read], key: &Key) -> usize {
	reads
		.iter()
		.filter(|read| {
			matches!(
				read,
				Read::ObjectParents { dependent, .. } | Read::ProcessParents { dependent, .. }
					if dependent == key
			)
		})
		.count()
}

fn search(root: &Key, state: &State) -> Search {
	let config = SearchConfig {
		max_depth: 2,
		max_edges: 8,
		max_nodes: 3,
		page_size: 1,
	};
	Search::new(
		config,
		&tg::Principal::Anonymous,
		std::slice::from_ref(root),
		None,
		state,
	)
}
