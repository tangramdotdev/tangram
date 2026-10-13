use {
	super::super::{Index, Key},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

async fn drain_permission_updates(index: &Index) -> usize {
	let mut count = 0;
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Permission, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
		count += output.count;
	}
	count
}

#[tokio::test]
async fn direct_permissions_become_non_expiring_when_a_process_relationship_is_added() {
	let (_dir, index) = super::new_index();
	let command = object_id(0);
	let creator = tg::Principal::User(tg::user::Id::new());
	let process = tg::process::Id::new();
	let subject = tg::authorization::Subject::Process(process.clone());
	let subtree = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	);
	index
		.put_permissions(&[tangram_index::permission::put::Arg {
			created_at: 0,
			creator: Some(creator),
			permissions: subtree.into(),
			resource: command.clone().into(),
			source: tangram_index::permission::Source::Direct {
				expires_at: Some(10),
			},
			subject: subject.clone(),
			time_to_touch: None,
			version: None,
		}])
		.await
		.unwrap();
	drain_permission_updates(&index).await;

	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutProcess(
				tangram_index::process::put::Arg {
					cached: false,
					children: None,
					command: Some(vec![command.clone()]),
					command_id: command.clone(),
					data: None,
					error: None,
					id: process.clone(),
					location: None,
					log: None,
					metadata: tg::process::Metadata::default(),
					options: tg::referent::Options::default(),
					output: None,
					parent: None,
					permissions: Vec::new(),
					principal: tg::Principal::Process(process.clone()),
					sandbox: None,
					storage: tg::process::storage::Set::NODE,
					time_to_touch: std::time::Duration::ZERO,
					touched_at: 0,
				},
			)],
		})
		.await
		.unwrap()
		.unwrap();
	drain_permission_updates(&index).await;

	let value = process_direct_value(&index, &command, &process, subtree);
	assert_eq!(value.direct, Some(None));
}

fn object_id(n: usize) -> tg::object::Id {
	tg::object::Id::new(tg::object::Kind::Blob, &n.to_le_bytes().to_vec().into())
}

fn process_direct_value(
	index: &Index,
	object: &tg::object::Id,
	process: &tg::process::Id,
	permission: tg::authorization::Permission,
) -> super::super::permission::PermissionValue {
	let transaction = index.env.read_txn().unwrap();
	let key = Key::Permission(super::super::permission::Key::ResourcePermission {
		creator: Some(tg::Principal::Process(process.clone())),
		permission,
		resource: object.clone().into(),
		subject: tg::authorization::Subject::Process(process.clone()),
	});
	let key = Index::pack(&index.subspace, &key);
	let value = index.db.get(&transaction, &key).unwrap().unwrap();

	super::super::permission::PermissionValue::deserialize(value).unwrap()
}

fn direct_permission_expiration_exists(
	index: &Index,
	object: &tg::object::Id,
	process: &tg::process::Id,
	permission: tg::authorization::Permission,
	expires_at: i64,
) -> bool {
	let transaction = index.env.read_txn().unwrap();
	let key = Key::Permission(super::super::permission::Key::PermissionExpiresAt {
		creator: Some(tg::Principal::Process(process.clone())),
		expires_at,
		permission,
		resource: object.clone().into(),
		source: super::super::permission::PermissionSource::Direct,
		subject: tg::authorization::Subject::Process(process.clone()),
	});
	let key = Index::pack(&index.subspace, &key);

	index.db.get(&transaction, &key).unwrap().is_some()
}

#[tokio::test]
async fn process_permissions_promote_to_non_expiring_direct_permissions() {
	let (_dir, index) = super::new_index();
	let child = object_id(1);
	let process = tg::process::Id::new();
	let unrelated = object_id(2);
	let wrapper = object_id(0);
	let put_object = |id: tg::object::Id, children| {
		tangram_index::batch::Item::PutObject(tangram_index::object::put::Arg {
			checkout: None,
			children,
			id,
			metadata: tg::object::Metadata::default(),
			put: [1; 16],
			storage: tg::object::storage::Set::NODE,
			time_to_touch: std::time::Duration::ZERO,
			touched_at: 0,
		})
	};
	let node = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Node,
	);
	let subtree = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	);
	let subject = tg::authorization::Subject::Process(process.clone());
	let creator = tg::Principal::Process(process.clone());
	index
		.batch(tangram_index::batch::Arg {
			items: vec![
				put_object(child.clone(), BTreeSet::new()),
				put_object(unrelated.clone(), BTreeSet::new()),
				put_object(wrapper.clone(), BTreeSet::from([child.clone()])),
				tangram_index::batch::Item::PutProcess(tangram_index::process::put::Arg {
					cached: false,
					children: None,
					command: Some(vec![wrapper.clone()]),
					command_id: wrapper.clone(),
					data: None,
					error: None,
					id: process.clone(),
					location: None,
					log: None,
					metadata: tg::process::Metadata::default(),
					options: tg::referent::Options::default(),
					output: None,
					parent: None,
					permissions: Vec::new(),
					principal: tg::Principal::Process(process.clone()),
					sandbox: None,
					storage: tg::process::storage::Set::NODE,
					time_to_touch: std::time::Duration::ZERO,
					touched_at: 0,
				}),
				tangram_index::batch::Item::PutPermission(tangram_index::permission::put::Arg {
					created_at: 0,
					creator: Some(creator.clone()),
					permissions: node.into(),
					resource: wrapper.clone().into(),
					source: tangram_index::permission::Source::Direct { expires_at: None },
					subject: subject.clone(),
					time_to_touch: None,
					version: None,
				}),
			],
		})
		.await
		.unwrap()
		.unwrap();
	drain_permission_updates(&index).await;

	index
		.put_permissions(&[
			tangram_index::permission::put::Arg {
				created_at: 0,
				creator: Some(creator.clone()),
				permissions: subtree.into(),
				resource: child.clone().into(),
				source: tangram_index::permission::Source::Direct {
					expires_at: Some(10),
				},
				subject: subject.clone(),
				time_to_touch: None,
				version: None,
			},
			tangram_index::permission::put::Arg {
				created_at: 0,
				creator: Some(creator.clone()),
				permissions: subtree.into(),
				resource: unrelated.clone().into(),
				source: tangram_index::permission::Source::Direct {
					expires_at: Some(10),
				},
				subject: subject.clone(),
				time_to_touch: None,
				version: None,
			},
		])
		.await
		.unwrap();
	drain_permission_updates(&index).await;

	let child_value = process_direct_value(&index, &child, &process, subtree);
	assert_eq!(child_value.direct, Some(None));
	assert!(!direct_permission_expiration_exists(
		&index, &child, &process, subtree, 10,
	));
	let wrapper_value = process_direct_value(&index, &wrapper, &process, subtree);
	assert_eq!(wrapper_value.direct, Some(None));
	let unrelated_value = process_direct_value(&index, &unrelated, &process, subtree);
	assert_eq!(unrelated_value.direct, Some(Some(10)));
	assert!(direct_permission_expiration_exists(
		&index, &unrelated, &process, subtree, 10,
	));

	index
		.delete_permissions(&[tangram_index::permission::delete::Arg {
			creator: Some(creator),
			permissions: subtree.into(),
			resource: child.clone().into(),
			source: tangram_index::permission::Source::Direct {
				expires_at: Some(10),
			},
			subject,
		}])
		.await
		.unwrap();
	drain_permission_updates(&index).await;

	let child_value = process_direct_value(&index, &child, &process, subtree);
	assert_eq!(child_value.direct, Some(None));
}

#[tokio::test]
async fn separates_update_queues() {
	let (_dir, index) = super::new_index();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &vec![0].into());
	let id = tg::Either::Left(id);
	let user = tg::user::Id::new();

	let mut transaction = index.env.write_txn().unwrap();
	Index::enqueue_update_with_kind(
		&index.db,
		&index.subspace,
		&mut transaction,
		id.clone(),
		super::super::update::Kind::Permission(tg::authorization::Subject::User(user.clone())),
		super::super::update::Source::Put,
		None,
	)
	.unwrap();
	Index::enqueue_update_with_kind(
		&index.db,
		&index.subspace,
		&mut transaction,
		id.clone(),
		super::super::update::Kind::StorageAndMetadata,
		super::super::update::Source::Put,
		None,
	)
	.unwrap();
	Index::enqueue_update_with_kind(
		&index.db,
		&index.subspace,
		&mut transaction,
		id,
		super::super::update::Kind::Usage(super::super::update::UsageKind::Put {
			account: tangram_index::usage::Account::User(user),
			permissions: tg::authorization::permission::Set::Object(
				tg::authorization::permission::object::Set::SUBTREE,
			),
			touched_at: 0,
		}),
		super::super::update::Source::Put,
		None,
	)
	.unwrap();
	transaction.commit().unwrap();

	for kind in [
		tangram_index::update::Kind::Permission,
		tangram_index::update::Kind::StorageAndMetadata,
		tangram_index::update::Kind::Usage,
	] {
		assert!(
			index
				.try_get_oldest_update_transaction_id(kind)
				.await
				.unwrap()
				.is_some()
		);
	}

	let output = index
		.update_batch(tangram_index::update::Kind::StorageAndMetadata, 100)
		.await
		.unwrap();
	assert_eq!(output.count, 1);
	assert_eq!(
		index
			.try_get_oldest_update_transaction_id(tangram_index::update::Kind::StorageAndMetadata)
			.await
			.unwrap(),
		None
	);
	assert!(
		index
			.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Permission)
			.await
			.unwrap()
			.is_some()
	);
	assert!(
		index
			.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Usage)
			.await
			.unwrap()
			.is_some()
	);

	let output = index
		.update_batch(tangram_index::update::Kind::Permission, 100)
		.await
		.unwrap();
	assert_eq!(output.count, 1);
	assert_eq!(
		index
			.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Permission)
			.await
			.unwrap(),
		None
	);
	assert!(
		index
			.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Usage)
			.await
			.unwrap()
			.is_some()
	);

	let output = index
		.update_batch(tangram_index::update::Kind::Usage, 100)
		.await
		.unwrap();
	assert_eq!(output.count, 1);
	assert_eq!(
		index
			.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Usage)
			.await
			.unwrap(),
		None
	);
}

#[tokio::test]
async fn shared_log_objects_do_not_materialize_permissions_on_unrelated_processes() {
	let (_dir, index) = super::new_index();
	let log = object_id(0);
	put_log_object(&index, &log).await;

	// Add processes that share the log object, recording the permission updates that each one causes.
	let mut processes = Vec::new();
	let mut counts = Vec::new();
	for _ in 0..4 {
		let process = tg::process::Id::new();
		let items = put_process_with_log_items(&process, &log, None);
		index
			.batch(tangram_index::batch::Arg { items })
			.await
			.unwrap()
			.unwrap();
		counts.push(drain_permission_updates(&index).await);
		processes.push(process);
	}

	// Verify that each process materializes only its own log permission.
	for resource in &processes {
		for subject in &processes {
			let entries = process_permission_entries(&index, resource, subject);
			if resource == subject {
				assert!(has_materialized_node_log_objects(&entries));
			} else {
				assert_eq!(entries, Vec::new());
			}
		}
	}

	// Verify that the permission updates for each added process do not grow with the number of processes that share the log object.
	assert!(
		counts.windows(2).all(|window| window[0] == window[1]),
		"{counts:?}"
	);

	// Verify that an unrelated process is still authorized to read the shared log without a materialized permission.
	let permissions = tg::authorization::permission::Set::from(node_log_objects());
	let arg = tangram_index::verify::Arg {
		requested: permissions,
		required: permissions,
		resource: tg::Selector::Id(processes[0].clone().into()),
		storage: tg::storage::Set::Object(tg::object::storage::Set::empty()),
		subject: None,
		tokens: Vec::new(),
	};
	let principal = tg::Principal::Process(processes[3].clone());
	let output = index
		.verify_batch(&[arg], tangram_index::verify::Config::default(), &principal)
		.await
		.unwrap();
	assert!(output[0].permissions.contains(permissions));
}

#[tokio::test]
async fn shared_log_objects_materialize_permissions_for_a_parent_with_a_permission() {
	let (_dir, index) = super::new_index();
	let log = object_id(0);
	put_log_object(&index, &log).await;

	// Add a parent and a child that share the log object, and give the parent a permission on the child.
	let parent = tg::process::Id::new();
	let child = tg::process::Id::new();
	let items = put_process_with_log_items(&parent, &log, None);
	index
		.batch(tangram_index::batch::Arg { items })
		.await
		.unwrap()
		.unwrap();
	drain_permission_updates(&index).await;
	let mut items = put_process_with_log_items(&child, &log, Some(&parent));
	let permission = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(tg::Principal::Process(parent.clone())),
		permissions: tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		)
		.into(),
		resource: child.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(i64::MAX),
		},
		subject: tg::authorization::Subject::Process(parent.clone()),
		time_to_touch: None,
		version: None,
	};
	items.push(tangram_index::batch::Item::PutPermission(permission));
	index
		.batch(tangram_index::batch::Arg { items })
		.await
		.unwrap()
		.unwrap();
	drain_permission_updates(&index).await;

	// Verify that the parent materializes the child's log permission.
	let entries = process_permission_entries(&index, &child, &parent);
	assert!(has_materialized_node_log_objects(&entries));
}

#[tokio::test]
async fn object_permission_updates_propagate_only_to_parents_with_a_permission_or_delegation() {
	let (_dir, index) = super::new_index();
	let child = object_id(0);
	let parent = object_id(1);
	let items = [
		(child.clone(), BTreeSet::new()),
		(parent.clone(), BTreeSet::from([child.clone()])),
	]
	.into_iter()
	.map(|(id, children)| {
		tangram_index::batch::Item::PutObject(tangram_index::object::put::Arg {
			checkout: None,
			children,
			id,
			metadata: tg::object::Metadata::default(),
			put: [1; 16],
			storage: tg::object::storage::Set::NODE,
			time_to_touch: std::time::Duration::ZERO,
			touched_at: 0,
		})
	})
	.collect();
	index
		.batch(tangram_index::batch::Arg { items })
		.await
		.unwrap()
		.unwrap();
	drain_permission_updates(&index).await;

	// Verify that an update for a subject without a permission or a delegation on the parent does not propagate.
	let delegated = tg::authorization::Subject::User(tg::user::Id::new());
	assert!(!propagates_to_parent(&index, &child, &delegated).await);

	// Verify that an update for a subject with a delegation on the parent propagates.
	let delegation = tangram_index::delegation::put::Arg {
		expires_at: i64::MAX,
		resource: parent.into(),
		source: tg::authorization::Subject::User(tg::user::Id::new()),
		subject: delegated.clone(),
		version: None,
	};
	let mut transaction = index.env.write_txn().unwrap();
	Index::put_delegation_with_transaction(
		&index.db,
		&index.subspace,
		&mut transaction,
		&delegation,
	)
	.unwrap();
	transaction.commit().unwrap();
	drain_permission_updates(&index).await;
	assert!(propagates_to_parent(&index, &child, &delegated).await);
}

async fn propagates_to_parent(
	index: &Index,
	child: &tg::object::Id,
	subject: &tg::authorization::Subject,
) -> bool {
	let mut transaction = index.env.write_txn().unwrap();
	Index::enqueue_update_with_kind(
		&index.db,
		&index.subspace,
		&mut transaction,
		tg::Either::Left(child.clone()),
		super::super::update::Kind::Permission(subject.clone()),
		super::super::update::Source::Put,
		None,
	)
	.unwrap();
	transaction.commit().unwrap();
	let output = index
		.update_batch(tangram_index::update::Kind::Permission, 1)
		.await
		.unwrap();
	assert_eq!(output.count, 1);
	let oldest = index
		.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Permission)
		.await
		.unwrap();
	drain_permission_updates(index).await;

	oldest.is_some()
}

fn node_log_objects() -> tg::authorization::Permission {
	tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::NodeLogObjects,
	)
}

async fn put_log_object(index: &Index, log: &tg::object::Id) {
	let object = tangram_index::object::put::Arg {
		checkout: None,
		children: BTreeSet::new(),
		id: log.clone(),
		metadata: tg::object::Metadata::default(),
		put: [1; 16],
		storage: tg::object::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	};
	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutObject(object)],
		})
		.await
		.unwrap()
		.unwrap();
	drain_permission_updates(index).await;
}

fn put_process_with_log_items(
	process: &tg::process::Id,
	log: &tg::object::Id,
	parent: Option<&tg::process::Id>,
) -> Vec<tangram_index::batch::Item> {
	let subject = tg::authorization::Subject::Process(process.clone());
	let creator = tg::Principal::Process(process.clone());
	let put_process = tangram_index::process::put::Arg {
		cached: false,
		children: None,
		command: None,
		command_id: log.clone(),
		data: None,
		error: None,
		id: process.clone(),
		location: None,
		log: Some(Some(log.clone())),
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: None,
		parent: parent.cloned(),
		permissions: Vec::new(),
		principal: creator.clone(),
		sandbox: None,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	};
	let put_node_permission = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(creator.clone()),
		permissions: tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		)
		.into(),
		resource: process.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(i64::MAX),
		},
		subject: subject.clone(),
		time_to_touch: None,
		version: None,
	};
	let put_log_permission = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(creator),
		permissions: tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		)
		.into(),
		resource: log.clone().into(),
		source: tangram_index::permission::Source::Direct { expires_at: None },
		subject,
		time_to_touch: None,
		version: None,
	};
	vec![
		tangram_index::batch::Item::PutProcess(put_process),
		tangram_index::batch::Item::PutPermission(put_node_permission),
		tangram_index::batch::Item::PutPermission(put_log_permission),
	]
}

fn process_permission_entries(
	index: &Index,
	resource: &tg::process::Id,
	subject: &tg::process::Id,
) -> Vec<super::super::permission::PermissionEntry> {
	let transaction = index.env.read_txn().unwrap();
	Index::get_resource_permission_entries_for_subject_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&resource.clone().into(),
		&tg::authorization::Subject::Process(subject.clone()),
	)
	.unwrap()
}

fn has_materialized_node_log_objects(
	entries: &[super::super::permission::PermissionEntry],
) -> bool {
	entries
		.iter()
		.any(|entry| entry.permission == node_log_objects() && entry.materialized.is_some())
}
