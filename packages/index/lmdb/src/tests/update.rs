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

	// Verify that no process materializes a log permission on itself, whose permissions it has inherently, or on an unrelated process.
	for resource in &processes {
		for subject in &processes {
			let entries = process_permission_entries(&index, resource, subject);
			if resource == subject {
				assert!(!has_materialized_node_log_objects(&entries));
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
async fn shared_log_objects_do_not_materialize_implied_permissions_for_a_parent() {
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

	// Verify that the parent does not materialize the child's log permission, which its parent permission implies.
	let entries = process_permission_entries(&index, &child, &parent);
	assert!(!has_materialized_node_log_objects(&entries));

	// Verify that the parent is still authorized to read the child's log.
	let permissions = tg::authorization::permission::Set::from(node_log_objects());
	let arg = tangram_index::verify::Arg {
		requested: permissions,
		required: permissions,
		resource: tg::Selector::Id(child.clone().into()),
		storage: tg::storage::Set::Object(tg::object::storage::Set::empty()),
		subject: None,
		tokens: Vec::new(),
	};
	let principal = tg::Principal::Process(parent.clone());
	let output = index
		.verify_batch(&[arg], tangram_index::verify::Config::default(), &principal)
		.await
		.unwrap();
	assert!(output[0].permissions.contains(permissions));
}

#[tokio::test]
async fn object_aspect_permissions_are_not_materialized_for_a_subject_with_a_parent_permission() {
	let (_dir, index) = super::new_index();
	let log = object_id(0);
	put_log_object(&index, &log).await;

	// Add a process on which a user has a parent permission, and give the user a subtree permission on the log object.
	let process = tg::process::Id::new();
	let subject = tg::authorization::Subject::User(tg::user::Id::new());
	let mut items = put_process_with_log_items(&process, &log, None);
	items.truncate(1);
	let parent = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(subject.try_to_principal().unwrap()),
		permissions: tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
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
	items.push(tangram_index::batch::Item::PutPermission(parent));
	let permission = subtree_permission_arg(&log, &subject);
	items.push(tangram_index::batch::Item::PutPermission(permission));
	index
		.batch(tangram_index::batch::Arg { items })
		.await
		.unwrap()
		.unwrap();
	drain_permission_updates(&index).await;

	// Verify that the user does not materialize the log permission, which the parent permission implies.
	let entries = object_permission_entries_for_resource(&index, &process.clone().into(), &subject);
	assert!(!has_materialized_node_log_objects(&entries));
}

#[tokio::test]
async fn shared_child_objects_do_not_enqueue_permission_updates_for_unrelated_parents() {
	let (_dir, index) = super::new_index();
	let child = object_id(0);
	put_log_object(&index, &child).await;

	// Add parents that share the child object, each put by a different subject, recording the permission updates that each subject's child permission causes.
	let mut parents = Vec::new();
	let mut subjects = Vec::new();
	let mut counts = Vec::new();
	for n in 1..=4 {
		let parent = object_id(n);
		let subject = tg::authorization::Subject::User(tg::user::Id::new());
		let items = put_parent_object_items(&parent, &child, &subject);
		index
			.batch(tangram_index::batch::Arg { items })
			.await
			.unwrap()
			.unwrap();
		drain_permission_updates(&index).await;
		let permission = subtree_permission_arg(&child, &subject);
		index.put_permissions(&[permission]).await.unwrap();
		counts.push(drain_permission_updates(&index).await);
		parents.push(parent);
		subjects.push(subject);
	}

	// Verify that each subject materializes a subtree permission only on its own parent.
	for (resource, owner) in parents.iter().zip(&subjects) {
		for subject in &subjects {
			let entries = object_permission_entries(&index, resource, subject);
			if subject == owner {
				assert!(entries.iter().any(|entry| {
					entry.permission == object_subtree() && entry.materialized.is_some()
				}));
			} else {
				assert_eq!(entries, Vec::new());
			}
		}
	}

	// Verify that the permission updates for each added parent do not grow with the number of parents that share the child object.
	assert!(
		counts.windows(2).all(|window| window[0] == window[1]),
		"{counts:?}"
	);
}

#[tokio::test]
async fn object_aspect_permissions_materialize_the_longest_cover() {
	let (_dir, index) = super::new_index();
	let log = object_id(0);
	put_log_object(&index, &log).await;

	// Add a process whose log object the subject can read through two permissions with different expirations.
	let process = tg::process::Id::new();
	let subject = tg::authorization::Subject::User(tg::user::Id::new());
	let mut items = put_process_with_log_items(&process, &log, None);
	items.truncate(1);
	let node = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(subject.try_to_principal().unwrap()),
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
	items.push(tangram_index::batch::Item::PutPermission(node));
	for expires_at in [100, 200] {
		let permission = tangram_index::permission::put::Arg {
			created_at: 0,
			creator: Some(tg::Principal::User(tg::user::Id::new())),
			permissions: object_subtree().into(),
			resource: log.clone().into(),
			source: tangram_index::permission::Source::Direct {
				expires_at: Some(expires_at),
			},
			subject: subject.clone(),
			time_to_touch: None,
			version: None,
		};
		items.push(tangram_index::batch::Item::PutPermission(permission));
	}
	index
		.batch(tangram_index::batch::Arg { items })
		.await
		.unwrap()
		.unwrap();
	drain_permission_updates(&index).await;

	// Verify that repeated updates keep the longest cover.
	for _ in 0..3 {
		enqueue_permission_update(&index, &process, &subject);
		drain_permission_updates(&index).await;
		let entries =
			object_permission_entries_for_resource(&index, &process.clone().into(), &subject);
		let materialized = entries
			.iter()
			.filter(|entry| entry.permission == node_log_objects())
			.filter_map(|entry| entry.materialized)
			.collect::<Vec<_>>();
		assert_eq!(materialized, vec![Some(200)]);
	}
}

#[tokio::test]
async fn process_subtree_permissions_materialize_the_longest_cover() {
	let (_dir, index) = super::new_index();
	let log = object_id(0);
	put_log_object(&index, &log).await;

	// Add a process on which the subject has two node permissions with different expirations.
	let process = tg::process::Id::new();
	let subject = tg::authorization::Subject::User(tg::user::Id::new());
	let mut items = put_process_with_log_items(&process, &log, None);
	items.truncate(1);
	for expires_at in [100, 200] {
		let permission = tangram_index::permission::put::Arg {
			created_at: 0,
			creator: Some(tg::Principal::User(tg::user::Id::new())),
			permissions: tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Node,
			)
			.into(),
			resource: process.clone().into(),
			source: tangram_index::permission::Source::Direct {
				expires_at: Some(expires_at),
			},
			subject: subject.clone(),
			time_to_touch: None,
			version: None,
		};
		items.push(tangram_index::batch::Item::PutPermission(permission));
	}
	index
		.batch(tangram_index::batch::Arg { items })
		.await
		.unwrap()
		.unwrap();
	drain_permission_updates(&index).await;

	// Verify that repeated updates keep the longest cover.
	let subtree = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Subtree,
	);
	for _ in 0..3 {
		enqueue_permission_update(&index, &process, &subject);
		drain_permission_updates(&index).await;
		let entries =
			object_permission_entries_for_resource(&index, &process.clone().into(), &subject);
		let materialized = entries
			.iter()
			.filter(|entry| entry.permission == subtree)
			.filter_map(|entry| entry.materialized)
			.collect::<Vec<_>>();
		assert_eq!(materialized, vec![Some(200)]);
	}
}

#[tokio::test]
async fn permission_updates_reach_a_shared_object_process_through_the_subject_permissions() {
	let (_dir, index) = super::new_index();
	let log = object_id(0);
	put_log_object(&index, &log).await;

	// Add processes that share the log object.
	let mut processes = Vec::new();
	for _ in 0..5 {
		let process = tg::process::Id::new();
		let items = put_process_with_log_items(&process, &log, None);
		index
			.batch(tangram_index::batch::Arg { items })
			.await
			.unwrap()
			.unwrap();
		drain_permission_updates(&index).await;
		processes.push(process);
	}

	// Give a user a node permission on one process, and then a subtree permission on the log object.
	let subject = tg::authorization::Subject::User(tg::user::Id::new());
	let permission = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(subject.try_to_principal().unwrap()),
		permissions: tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		)
		.into(),
		resource: processes[2].clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(i64::MAX),
		},
		subject: subject.clone(),
		time_to_touch: None,
		version: None,
	};
	index.put_permissions(&[permission]).await.unwrap();
	drain_permission_updates(&index).await;
	let permission = subtree_permission_arg(&log, &subject);
	index.put_permissions(&[permission]).await.unwrap();
	drain_permission_updates(&index).await;

	// Verify that the user materializes the log permission only on the process with the node permission.
	for (n, process) in processes.iter().enumerate() {
		let entries =
			object_permission_entries_for_resource(&index, &process.clone().into(), &subject);
		assert_eq!(has_materialized_node_log_objects(&entries), n == 2);
	}
}

#[tokio::test]
async fn permission_updates_reach_a_parent_object_for_a_subject_with_many_permissions() {
	let (_dir, index) = super::new_index();
	let child = object_id(0);
	put_log_object(&index, &child).await;

	// Give a user subtree permissions on unrelated objects.
	let subject = tg::authorization::Subject::User(tg::user::Id::new());
	for n in 10..20 {
		let object = object_id(n);
		put_log_object(&index, &object).await;
		let permission = subtree_permission_arg(&object, &subject);
		index.put_permissions(&[permission]).await.unwrap();
		drain_permission_updates(&index).await;
	}

	// Add a parent of the child with a node permission for the user, and then give the user a subtree permission on the child.
	let parent = object_id(1);
	let items = put_parent_object_items(&parent, &child, &subject);
	index
		.batch(tangram_index::batch::Arg { items })
		.await
		.unwrap()
		.unwrap();
	drain_permission_updates(&index).await;
	let permission = subtree_permission_arg(&child, &subject);
	index.put_permissions(&[permission]).await.unwrap();
	drain_permission_updates(&index).await;

	// Verify that the user materializes a subtree permission on the parent.
	let entries = object_permission_entries(&index, &parent, &subject);
	assert!(
		entries
			.iter()
			.any(|entry| entry.permission == object_subtree() && entry.materialized.is_some())
	);
}

#[tokio::test]
async fn materialized_permissions_are_rewritten_only_when_their_expiration_moves_beyond_the_time_to_touch()
 {
	let (_dir, index) =
		super::new_index_with_permission_time_to_touch(std::time::Duration::from_secs(100));
	let child = object_id(0);
	put_log_object(&index, &child).await;
	let parent = object_id(1);
	let subject = tg::authorization::Subject::User(tg::user::Id::new());
	let put_object = tangram_index::object::put::Arg {
		checkout: None,
		children: BTreeSet::from([child.clone()]),
		id: parent.clone(),
		metadata: tg::object::Metadata::default(),
		put: [1; 16],
		storage: tg::object::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	};
	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutObject(put_object)],
		})
		.await
		.unwrap()
		.unwrap();

	// Renew the subject's permissions on the parent and the child, recording the materialized subtree expiration on the parent after each renewal.
	let mut expirations = Vec::new();
	for expires_at in [1000, 1050, 1200] {
		let node = tangram_index::permission::put::Arg {
			created_at: 0,
			creator: Some(subject.try_to_principal().unwrap()),
			permissions: tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			)
			.into(),
			resource: parent.clone().into(),
			source: tangram_index::permission::Source::Direct {
				expires_at: Some(expires_at),
			},
			subject: subject.clone(),
			time_to_touch: None,
			version: None,
		};
		let subtree = tangram_index::permission::put::Arg {
			created_at: 0,
			creator: Some(subject.try_to_principal().unwrap()),
			permissions: object_subtree().into(),
			resource: child.clone().into(),
			source: tangram_index::permission::Source::Direct {
				expires_at: Some(expires_at),
			},
			subject: subject.clone(),
			time_to_touch: None,
			version: None,
		};
		index.put_permissions(&[node, subtree]).await.unwrap();
		drain_permission_updates(&index).await;
		let entries = object_permission_entries(&index, &parent, &subject);
		let expiration = entries
			.iter()
			.filter(|entry| entry.permission == object_subtree())
			.find_map(|entry| entry.materialized);
		expirations.push(expiration);
	}

	// Verify that the renewal within the time to touch keeps the earlier expiration.
	assert_eq!(
		expirations,
		vec![Some(Some(1000)), Some(Some(1000)), Some(Some(1200))]
	);
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

fn put_parent_object_items(
	parent: &tg::object::Id,
	child: &tg::object::Id,
	subject: &tg::authorization::Subject,
) -> Vec<tangram_index::batch::Item> {
	let creator = subject.try_to_principal().unwrap();
	let put_object = tangram_index::object::put::Arg {
		checkout: None,
		children: BTreeSet::from([child.clone()]),
		id: parent.clone(),
		metadata: tg::object::Metadata::default(),
		put: [1; 16],
		storage: tg::object::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	};
	let put_node_permission = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(creator),
		permissions: tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		)
		.into(),
		resource: parent.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(i64::MAX),
		},
		subject: subject.clone(),
		time_to_touch: None,
		version: None,
	};
	vec![
		tangram_index::batch::Item::PutObject(put_object),
		tangram_index::batch::Item::PutPermission(put_node_permission),
	]
}

fn subtree_permission_arg(
	resource: &tg::object::Id,
	subject: &tg::authorization::Subject,
) -> tangram_index::permission::put::Arg {
	tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(subject.try_to_principal().unwrap()),
		permissions: object_subtree().into(),
		resource: resource.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(i64::MAX),
		},
		subject: subject.clone(),
		time_to_touch: None,
		version: None,
	}
}

fn object_permission_entries(
	index: &Index,
	resource: &tg::object::Id,
	subject: &tg::authorization::Subject,
) -> Vec<super::super::permission::PermissionEntry> {
	object_permission_entries_for_resource(index, &resource.clone().into(), subject)
}

fn object_permission_entries_for_resource(
	index: &Index,
	resource: &tg::Id,
	subject: &tg::authorization::Subject,
) -> Vec<super::super::permission::PermissionEntry> {
	let transaction = index.env.read_txn().unwrap();
	Index::get_resource_permission_entries_for_subject_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		resource,
		subject,
	)
	.unwrap()
}

fn object_subtree() -> tg::authorization::Permission {
	tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	)
}

fn enqueue_permission_update(
	index: &Index,
	process: &tg::process::Id,
	subject: &tg::authorization::Subject,
) {
	let mut transaction = index.env.write_txn().unwrap();
	Index::enqueue_update_with_kind(
		&index.db,
		&index.subspace,
		&mut transaction,
		tg::Either::Right(process.clone()),
		super::super::update::Kind::Permission(subject.clone()),
		super::super::update::Source::Propagate,
		None,
	)
	.unwrap();
	transaction.commit().unwrap();
}
