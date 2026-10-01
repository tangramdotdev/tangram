use {
	super::super::Index,
	std::{collections::BTreeSet, time::Duration},
	tangram_client::prelude::*,
};

#[tokio::test]
async fn delegation_traversal_and_propagation() {
	let (_dir, index) = super::new_index();
	let root = object(0);
	let child = object(1);
	let process = tg::process::Id::new();
	let intermediate = tg::process::Id::new();
	let sync = tg::authorization::Subject::Sync(tg::sync::Id::new());
	let subject = tg::authorization::Subject::Process(process.clone());
	let middle = tg::authorization::Subject::Process(intermediate);
	let arg = tangram_index::batch::Arg {
		items: vec![
			put_object(&root, BTreeSet::from([child.clone()])),
			put_object(&child, BTreeSet::new()),
			delegation(&root, &middle, &sync),
			delegation(&root, &subject, &middle),
			permission(&root, &sync, false),
		],
	};
	index.batch(arg).await.unwrap();
	assert!(authorized(&index, &root, &process, false).await);
	assert!(!authorized(&index, &root, &process, true).await);
	assert!(!authorized(&index, &root, &tg::process::Id::new(), false).await);

	let arg = tangram_index::batch::Arg {
		items: vec![permission(&child, &sync, true)],
	};
	index.batch(arg).await.unwrap();
	assert!(authorized(&index, &root, &process, true).await);
	drain(&index).await;
	assert!(authorized(&index, &root, &process, true).await);

	// Attach the object after propagation to exercise the other insertion order.
	let arg = tangram_index::process::put::Arg {
		cached: false,
		children: None,
		command: Some(vec![root.clone()]),
		command_id: root.clone(),
		data: None,
		error: None,
		id: process.clone(),
		location: None,
		log: None,
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: None,
		parent: None,
		sandbox: None,
		storage: tangram_index::process::Storage::default(),
		time_to_touch: Duration::ZERO,
		touched_at: 0,
	};
	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutProcess(arg)],
		})
		.await
		.unwrap();
	drain(&index).await;
	let arg = tangram_index::clean::Arg {
		batch_size: 100,
		max_object_touched_at: i64::MIN,
		max_process_touched_at: i64::MIN,
		max_sandbox_touched_at: i64::MIN,
		now: 1000,
		partition_end: 1,
		partition_start: 0,
	};
	index.clean(arg).await.unwrap();
	drain(&index).await;
	assert!(authorized(&index, &root, &process, true).await);
	let transaction = index.env.read_txn().unwrap();
	let entries = Index::get_resource_permission_entries_for_subject_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&child.into(),
		&subject,
	)
	.unwrap();
	assert_eq!(entries.len(), 0);
}

#[tokio::test]
async fn expired_delegations_remove_temporary_permissions() {
	let (_dir, index) = super::new_index();
	let root = object(0);
	let process = tg::process::Id::new();
	let subject = tg::authorization::Subject::Process(process.clone());
	let source = tg::authorization::Subject::User(tg::user::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			put_object(&root, BTreeSet::new()),
			permission(&root, &source, true),
			delegation(&root, &subject, &source),
		],
	};
	index.batch(arg).await.unwrap();
	drain(&index).await;
	assert!(authorized(&index, &root, &process, true).await);
	let arg = tangram_index::clean::Arg {
		batch_size: 100,
		max_object_touched_at: i64::MIN,
		max_process_touched_at: i64::MIN,
		max_sandbox_touched_at: i64::MIN,
		now: 1000,
		partition_end: 1,
		partition_start: 0,
	};
	index.clean(arg).await.unwrap();
	drain(&index).await;
	assert!(!authorized(&index, &root, &process, true).await);
}

#[tokio::test]
async fn removing_a_source_permission_does_not_leave_a_materialized_subtree() {
	let (_dir, index) = super::new_index();
	let root = object(0);
	let process = tg::process::Id::new();
	let subject = tg::authorization::Subject::Process(process.clone());
	let source = tg::authorization::Subject::User(tg::user::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			put_object(&root, BTreeSet::new()),
			permission(&root, &source, false),
		],
	};
	index.batch(arg).await.unwrap();
	drain(&index).await;
	let arg = tangram_index::batch::Arg {
		items: vec![delegation(&root, &subject, &source)],
	};
	index.batch(arg).await.unwrap();
	drain(&index).await;
	assert!(authorized(&index, &root, &process, true).await);
	let arg = tangram_index::permission::delete::Arg {
		creator: None,
		permissions: tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		)
		.into(),
		resource: root.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(200),
		},
		subject: source,
	};
	index.delete_permissions(&[arg]).await.unwrap();
	drain(&index).await;
	assert!(!authorized(&index, &root, &process, true).await);
}

#[tokio::test]
async fn node_permissions_wait_for_object_children() {
	for has_child in [false, true] {
		let (_dir, index) = super::new_index();
		let root = object(0);
		let child = object(1);
		let process = tg::process::Id::new();
		let subject = tg::authorization::Subject::Process(process.clone());
		let source = tg::authorization::Subject::Sync(tg::sync::Id::new());
		let arg = tangram_index::batch::Arg {
			items: vec![
				process_object(&process, &root),
				permission(&root, &source, false),
				delegation(&root, &subject, &source),
			],
		};
		index.batch(arg).await.unwrap();
		drain(&index).await;
		assert!(!authorized(&index, &root, &process, true).await);

		let children = if has_child {
			BTreeSet::from([child.clone()])
		} else {
			BTreeSet::new()
		};
		let arg = tangram_index::batch::Arg {
			items: vec![
				put_object(&root, children),
				put_object(&child, BTreeSet::new()),
			],
		};
		index.batch(arg).await.unwrap();
		drain(&index).await;
		assert_eq!(authorized(&index, &root, &process, true).await, !has_child);
		if has_child {
			let arg = tangram_index::batch::Arg {
				items: vec![permission(&child, &source, true)],
			};
			index.batch(arg).await.unwrap();
			drain(&index).await;
		}
		clean(&index).await;
		assert!(authorized(&index, &root, &process, true).await);
	}
}

#[tokio::test]
async fn explicit_subtree_permissions_do_not_wait_for_indexing() {
	let (_dir, index) = super::new_index();
	let root = object(0);
	let process = tg::process::Id::new();
	let subject = tg::authorization::Subject::Process(process.clone());
	let source = tg::authorization::Subject::Sync(tg::sync::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			process_object(&process, &root),
			permission(&root, &source, true),
			delegation(&root, &subject, &source),
		],
	};
	index.batch(arg).await.unwrap();
	drain(&index).await;
	clean(&index).await;
	let arg = tangram_index::batch::Arg {
		items: vec![put_object(&root, BTreeSet::new())],
	};
	index.batch(arg).await.unwrap();
	assert!(authorized(&index, &root, &process, true).await);
}

#[tokio::test]
async fn process_parent_traverses_delegations_before_propagation() {
	for has_parent in [false, true] {
		for has_source in [false, true] {
			let (_dir, index) = super::new_index();
			let root = object(0);
			let process = tg::process::Id::new();
			let user = tg::user::Id::new();
			let recipient = tg::authorization::Subject::Process(process.clone());
			let source = tg::authorization::Subject::Sync(tg::sync::Id::new());
			let mut items = vec![
				put_object(&root, BTreeSet::new()),
				process_object(&process, &root),
				delegation(&root, &recipient, &source),
			];
			if has_source {
				items.push(permission(&root, &source, true));
			}
			if has_parent {
				let arg = tangram_index::permission::put::Arg {
					created_at: 0,
					creator: None,
					permissions: tg::authorization::Permission::Process(
						tg::authorization::permission::process::Permission::Parent,
					)
					.into(),
					resource: process.into(),
					source: tangram_index::permission::Source::Direct {
						expires_at: Some(200),
					},
					subject: tg::authorization::Subject::User(user.clone()),
					time_to_touch: None,
				};
				items.push(tangram_index::batch::Item::PutPermission(arg));
			}
			let arg = tangram_index::batch::Arg { items };
			index.batch(arg).await.unwrap();
			let permission = tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			);
			let arg = tangram_index::authorize::Arg {
				requested: permission.into(),
				required: permission.into(),
				resource: tg::Selector::Id(root.into()),
				tokens: Vec::new(),
			};
			let outputs = index
				.authorize_batch(
					&[arg],
					tangram_index::authorize::Config::default(),
					&tg::Principal::User(user),
				)
				.await
				.unwrap();
			let authorized = outputs[0]
				.output()
				.is_some_and(|output| output.permissions.contains(permission));
			assert_eq!(authorized, has_parent && has_source);
		}
	}
}

#[tokio::test]
async fn node_requests_resolve_delegated_subtree_derivation() {
	for has_child_permission in [false, true] {
		let (_dir, index) = super::new_index();
		let ancestor = object(0);
		let root = object(1);
		let process = tg::process::Id::new();
		let recipient = tg::authorization::Subject::Process(process.clone());
		let middle = tg::authorization::Subject::Process(tg::process::Id::new());
		let source = tg::authorization::Subject::Sync(tg::sync::Id::new());
		let mut items = vec![
			put_object(&ancestor, BTreeSet::from([root.clone()])),
			put_object(&root, BTreeSet::new()),
			permission(&ancestor, &source, false),
			delegation(&ancestor, &middle, &source),
			delegation(&root, &recipient, &middle),
		];
		if has_child_permission {
			items.push(permission(&root, &source, true));
		}
		let arg = tangram_index::batch::Arg { items };
		index.batch(arg).await.unwrap();
		for subtree in [false, true] {
			assert_eq!(
				authorized(&index, &root, &process, subtree).await,
				has_child_permission
			);
		}
	}
}

fn process_object(process: &tg::process::Id, root: &tg::object::Id) -> tangram_index::batch::Item {
	let arg = tangram_index::process::put::Arg {
		cached: false,
		children: None,
		command: Some(vec![root.clone()]),
		command_id: root.clone(),
		data: None,
		error: None,
		id: process.clone(),
		location: None,
		log: None,
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: None,
		parent: None,
		sandbox: None,
		storage: tangram_index::process::Storage::default(),
		time_to_touch: Duration::ZERO,
		touched_at: 0,
	};
	tangram_index::batch::Item::PutProcess(arg)
}
async fn clean(index: &Index) {
	let arg = tangram_index::clean::Arg {
		batch_size: 1000,
		max_object_touched_at: i64::MIN,
		max_process_touched_at: i64::MIN,
		max_sandbox_touched_at: i64::MIN,
		now: 1000,
		partition_end: 1,
		partition_start: 0,
	};
	index.clean(arg).await.unwrap();
	drain(index).await;
}

fn object(value: u8) -> tg::object::Id {
	tg::object::Id::new(tg::object::Kind::Blob, &vec![value].into())
}

fn put_object(
	id: &tg::object::Id,
	children: BTreeSet<tg::object::Id>,
) -> tangram_index::batch::Item {
	let arg = tangram_index::object::put::Arg {
		checkout: None,
		children,
		id: id.clone(),
		metadata: tg::object::Metadata::default(),
		put: [1; 16],
		storage: tangram_index::object::Storage::default(),
		time_to_touch: Duration::ZERO,
		touched_at: 0,
	};
	tangram_index::batch::Item::PutObject(arg)
}

fn delegation(
	resource: &tg::object::Id,
	subject: &tg::authorization::Subject,
	source: &tg::authorization::Subject,
) -> tangram_index::batch::Item {
	let arg = tangram_index::delegation::put::Arg {
		expires_at: 100,
		resource: resource.clone().into(),
		source: source.clone(),
		subject: subject.clone(),
	};
	tangram_index::batch::Item::PutDelegation(arg)
}

fn permission(
	resource: &tg::object::Id,
	subject: &tg::authorization::Subject,
	subtree: bool,
) -> tangram_index::batch::Item {
	let permission = tg::authorization::Permission::Object(if subtree {
		tg::authorization::permission::object::Permission::Subtree
	} else {
		tg::authorization::permission::object::Permission::Node
	});
	let arg = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: None,
		permissions: permission.into(),
		resource: resource.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(200),
		},
		subject: subject.clone(),
		time_to_touch: None,
	};
	tangram_index::batch::Item::PutPermission(arg)
}

async fn authorized(
	index: &Index,
	resource: &tg::object::Id,
	process: &tg::process::Id,
	subtree: bool,
) -> bool {
	let permission = tg::authorization::Permission::Object(if subtree {
		tg::authorization::permission::object::Permission::Subtree
	} else {
		tg::authorization::permission::object::Permission::Node
	});
	let arg = tangram_index::authorize::Arg {
		requested: permission.into(),
		required: permission.into(),
		resource: tg::Selector::Id(resource.clone().into()),
		tokens: Vec::new(),
	};
	let outputs = index
		.authorize_batch(
			&[arg],
			tangram_index::authorize::Config::default(),
			&tg::Principal::Process(process.clone()),
		)
		.await
		.unwrap();
	outputs[0]
		.output()
		.is_some_and(|output| output.permissions.contains(permission))
}

async fn drain(index: &Index) {
	for _ in 0..100 {
		if index
			.update_batch(tangram_index::update::Kind::Permission, 100)
			.await
			.unwrap()
			.count == 0
		{
			return;
		}
	}
	panic!("permission propagation did not finish");
}

#[tokio::test]
async fn graph_delegation_preserves_individual_descendant_permissions() {
	let (_dir, index) = super::new_index();
	let root = object(10);
	let file = object(11);
	let missing = object(12);
	let unrelated = object(13);
	let process = tg::process::Id::new();
	let recipient = tg::authorization::Subject::Process(process.clone());
	let middle = tg::authorization::Subject::Process(tg::process::Id::new());
	let source = tg::authorization::Subject::User(tg::user::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			put_object(&root, BTreeSet::from([file.clone(), missing])),
			put_object(&file, BTreeSet::new()),
			put_object(&unrelated, BTreeSet::new()),
			permission(&root, &source, false),
			permission(&file, &source, false),
			permission(&unrelated, &source, true),
			delegation(&root, &middle, &source),
			delegation(&root, &recipient, &middle),
		],
	};
	index.batch(arg).await.unwrap();
	for propagate in [false, true] {
		if propagate {
			drain(&index).await;
		}
		assert!(authorized(&index, &root, &process, false).await);
		assert!(authorized(&index, &file, &process, false).await);
		assert!(!authorized(&index, &root, &process, true).await);
		assert!(!authorized(&index, &unrelated, &process, false).await);
	}
	clean(&index).await;
	assert!(!authorized(&index, &file, &process, false).await);
}

#[tokio::test]
async fn shared_ancestor_paths_are_bounded_by_states() {
	let (_dir, index) = super::new_index();
	let file = object(30);
	let root = object(31);
	let missing = object(32);
	let process = tg::process::Id::new();
	let recipient = tg::authorization::Subject::Process(process.clone());
	let source = tg::authorization::Subject::User(tg::user::Id::new());
	let mut items = vec![put_object(&file, BTreeSet::new())];
	let mut children = BTreeSet::from([file.clone(), missing]);
	for level in 0..10 {
		let first = object(40 + level * 2);
		let second = object(41 + level * 2);
		items.push(put_object(&first, children.clone()));
		items.push(put_object(&second, children));
		children = BTreeSet::from([first, second]);
	}
	items.extend([
		put_object(&root, children),
		permission(&file, &source, false),
		delegation(&root, &recipient, &source),
	]);
	let arg = tangram_index::batch::Arg { items };
	index.batch(arg).await.unwrap();
	let permission = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Node,
	);
	let arg = tangram_index::authorize::Arg {
		requested: permission.into(),
		required: permission.into(),
		resource: tg::Selector::Id(file.into()),
		tokens: Vec::new(),
	};
	let mut config = tangram_index::authorize::Config::default();
	config.ancestor.max_nodes = 128;
	config.ancestor.max_edges = 256;
	let outputs = index
		.authorize_batch(&[arg], config, &tg::Principal::Process(process))
		.await
		.unwrap();
	assert!(matches!(
		outputs[0],
		tangram_index::authorize::Outcome::Authorized(_)
	));
}

#[tokio::test]
async fn subject_deletion_removes_all_delegation_indexes() {
	let (_dir, index) = super::new_index();
	let root = object(100);
	let source = tg::authorization::Subject::User(tg::user::Id::new());
	let first = tg::process::Id::new();
	let second = tg::process::Id::new();
	let subject = tg::authorization::Subject::Process(first.clone());
	let arg = tangram_index::batch::Arg {
		items: vec![
			put_object(&root, BTreeSet::new()),
			permission(&root, &source, true),
			delegation(&root, &subject, &source),
			delegation(
				&root,
				&tg::authorization::Subject::Process(second.clone()),
				&source,
			),
		],
	};
	index.batch(arg).await.unwrap();
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::DeleteDelegations(
			subject.clone(),
		)],
	};
	index.batch(arg).await.unwrap();
	drain(&index).await;
	assert!(!authorized(&index, &root, &first, true).await);
	assert!(authorized(&index, &root, &second, true).await);
	let transaction = index.env.read_txn().unwrap();
	for entry in index.db.iter(&transaction).unwrap() {
		let (key, _) = entry.unwrap();
		if let crate::Key::Delegation(key) = Index::unpack(&index.subspace, key).unwrap() {
			let recipient = match key {
				crate::delegation::Key::Delegation { subject, .. }
				| crate::delegation::Key::ExpiresAt { subject, .. }
				| crate::delegation::Key::Source { subject, .. }
				| crate::delegation::Key::Subject { subject, .. } => subject,
			};
			assert_ne!(recipient, subject);
		}
	}
}

#[tokio::test]
async fn process_output_token_uses_permissions_inherited_on_an_input_descendant() {
	let (_dir, index) = super::new_index();
	let directory = object(150);
	let file = object(151);
	let process = tg::process::Id::new();
	let parent = tg::authorization::Subject::User(tg::user::Id::new());
	let recipient = tg::authorization::Subject::Process(process.clone());
	let process_arg = tangram_index::process::put::Arg {
		cached: false,
		children: None,
		command: Some(vec![directory.clone()]),
		command_id: directory.clone(),
		data: None,
		error: None,
		id: process.clone(),
		location: None,
		log: None,
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: Some(Some(vec![file.clone()])),
		parent: None,
		sandbox: None,
		storage: tangram_index::process::Storage::default(),
		time_to_touch: Duration::ZERO,
		touched_at: 0,
	};
	let put = tangram_index::batch::Arg {
		items: vec![
			put_object(&directory, BTreeSet::from([file.clone()])),
			tangram_index::batch::Item::PutProcess(process_arg),
			delegation(&directory, &recipient, &parent),
		],
	};
	index.batch(put).await.unwrap();
	let subtree = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	);
	let output = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::NodeOutputObjects,
	);
	let arg = tangram_index::authorize::Arg {
		requested: subtree.into(),
		required: subtree.into(),
		resource: tg::Selector::Id(file.into()),

		tokens: vec![tg::authorization::Body {
			expires_at: 90,
			permissions: vec![output],
			resource: process.into(),
		}],
	};
	let config = tangram_index::authorize::Config::default();
	let results = index
		.authorize_batch(
			std::slice::from_ref(&arg),
			config,
			&tg::Principal::Anonymous,
		)
		.await
		.unwrap();
	assert!(matches!(
		results[0],
		tangram_index::authorize::Outcome::Denied(_)
	));
	let put = tangram_index::batch::Arg {
		items: vec![permission(&directory, &parent, true)],
	};
	index.batch(put).await.unwrap();
	let results = index
		.authorize_batch(&[arg], config, &tg::Principal::Anonymous)
		.await
		.unwrap();
	assert!(matches!(
		results[0],
		tangram_index::authorize::Outcome::Authorized(_)
	));
	assert_eq!(results[0].output().unwrap().expires_at, Some(90));
}

#[tokio::test]
async fn process_output_aspect_preserves_permanent_descendant_node_permissions() {
	let (_dir, index) = super::new_index();
	let directory = object(180);
	let file = object(181);
	let missing = object(182);
	let unrelated = object(183);
	let middle = object(184);
	let process = tg::process::Id::new();
	let subject = tg::authorization::Subject::Process(process.clone());
	let mut process_item = process_object(&process, &directory);
	let tangram_index::batch::Item::PutProcess(process_arg) = &mut process_item else {
		unreachable!()
	};
	process_arg.command = Some(vec![unrelated.clone()]);
	process_arg.output = Some(Some(vec![directory.clone()]));
	let mut items = vec![
		put_object(&directory, BTreeSet::from([middle.clone(), missing])),
		put_object(&middle, BTreeSet::from([file.clone()])),
		put_object(&file, BTreeSet::new()),
		put_object(&unrelated, BTreeSet::new()),
		process_item,
	];
	for object in [&directory, &file, &unrelated] {
		let mut item = permission(object, &subject, false);
		let tangram_index::batch::Item::PutPermission(arg) = &mut item else {
			unreachable!()
		};
		arg.source = tangram_index::permission::Source::Direct { expires_at: None };
		arg.creator = Some(tg::Principal::Process(process.clone()));
		items.push(item);
	}
	let put = tangram_index::batch::Arg { items };
	index.batch(put).await.unwrap();
	let node = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Node,
	);
	let output = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::NodeOutputObjects,
	);
	let arg = tangram_index::authorize::Arg {
		requested: node.into(),
		required: node.into(),
		resource: tg::Selector::Id(file.clone().into()),

		tokens: vec![tg::authorization::Body {
			expires_at: 1000,
			permissions: vec![output],
			resource: process.clone().into(),
		}],
	};
	for direction in 0..3 {
		let mut config = tangram_index::authorize::Config::default();
		if direction == 1 {
			config.descendant.max_nodes = 0;
		}
		if direction == 2 {
			config.ancestor.max_nodes = 0;
		}
		let result = index
			.authorize_batch(
				std::slice::from_ref(&arg),
				config,
				&tg::Principal::Anonymous,
			)
			.await
			.unwrap();
		assert!(
			matches!(result[0], tangram_index::authorize::Outcome::Authorized(_)),
			"direction {direction}: {:?}",
			result[0]
		);
		assert_eq!(result[0].output().unwrap().expires_at, Some(1000));
		for denied in [
			tangram_index::authorize::Arg {
				tokens: Vec::new(),
				..arg.clone()
			},
			tangram_index::authorize::Arg {
				resource: tg::Selector::Id(unrelated.clone().into()),
				..arg.clone()
			},
			tangram_index::authorize::Arg {
				requested: tg::authorization::permission::Set::Object(
					tg::authorization::permission::object::Set::SUBTREE,
				),
				required: tg::authorization::permission::Set::Object(
					tg::authorization::permission::object::Set::SUBTREE,
				),
				resource: tg::Selector::Id(directory.clone().into()),
				..arg.clone()
			},
		] {
			let result = index
				.authorize_batch(&[denied], config, &tg::Principal::Anonymous)
				.await
				.unwrap();
			assert!(
				!matches!(result[0], tangram_index::authorize::Outcome::Authorized(_)),
				"direction {direction}: {:?}",
				result[0]
			);
		}
	}
	// The same guard also follows the process's graph-scoped delegation before capture.
	let delete = tangram_index::permission::delete::Arg {
		creator: Some(tg::Principal::Process(process.clone())),
		permissions: node.into(),
		resource: file.clone().into(),
		source: tangram_index::permission::Source::Direct { expires_at: None },
		subject: subject.clone(),
	};
	index.delete_permissions(&[delete]).await.unwrap();
	let result = index
		.authorize_batch(
			std::slice::from_ref(&arg),
			tangram_index::authorize::Config::default(),
			&tg::Principal::Anonymous,
		)
		.await
		.unwrap();
	assert!(
		!matches!(result[0], tangram_index::authorize::Outcome::Authorized(_)),
		"{:?}",
		result[0]
	);
	let source = tg::authorization::Subject::User(tg::user::Id::new());
	let put = tangram_index::batch::Arg {
		items: vec![
			delegation(&directory, &subject, &source),
			permission(&file, &source, false),
		],
	};
	index.batch(put).await.unwrap();
	for descendant_enabled in [false, true] {
		let mut config = tangram_index::authorize::Config::default();
		if !descendant_enabled {
			config.descendant.max_nodes = 0;
		}
		let result = index
			.authorize_batch(
				std::slice::from_ref(&arg),
				config,
				&tg::Principal::Anonymous,
			)
			.await
			.unwrap();
		assert!(
			matches!(result[0], tangram_index::authorize::Outcome::Authorized(_)),
			"{:?}",
			result[0]
		);
		assert_eq!(result[0].output().unwrap().expires_at, Some(100));
	}
}
