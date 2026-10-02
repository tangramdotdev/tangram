use {
	super::super::{Config, Index},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_index::Index as _,
};

fn object_id(value: u64) -> tg::object::Id {
	tg::object::Id::new(tg::object::Kind::Blob, &value.to_le_bytes().to_vec().into())
}

fn object_arg(
	id: tg::object::Id,
	children: impl IntoIterator<Item = tg::object::Id>,
	size: u64,
) -> tangram_index::object::put::Arg {
	tangram_index::object::put::Arg {
		checkout: None,
		children: children.into_iter().collect::<BTreeSet<_>>(),
		id,
		metadata: tg::object::Metadata {
			node: tg::object::metadata::Node {
				size,
				..Default::default()
			},
			..Default::default()
		},
		put: [1; 16],
		storage: tg::object::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 1,
	}
}

fn process_arg(
	id: tg::process::Id,
	children: Vec<tg::process::Id>,
	command: tg::object::Id,
) -> tangram_index::process::put::Arg {
	let children = children
		.into_iter()
		.map(|child| tg::process::data::Child {
			cached: false,
			process: tg::Referent::with_node(child),
		})
		.collect();
	tangram_index::process::put::Arg {
		cached: false,
		children: Some(children),
		command: Some(vec![command.clone()]),
		command_id: command,
		data: None,
		error: Some(None),
		id,
		location: None,
		log: Some(None),
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: Some(None),
		parent: None,
		sandbox: None,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 1,
	}
}

fn new_index(usage_partition_total: u64) -> (tempfile::TempDir, Index) {
	let dir = tempfile::TempDir::new().unwrap();
	let index = Index::new(&Config {
		map_size: 1 << 30,
		max_process_depth: None,
		path: dir.path().join("index"),
		posix_sem_prefix: None,
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total,
		write_operation_batch_size: 100_000,
	})
	.unwrap();
	let mut transaction = index.env.write_txn().unwrap();
	let key = Index::pack(
		&index.subspace,
		&crate::Key::Usage(crate::usage::Key::Started),
	);
	let value = tangram_index::usage::serialize_timestamp(i64::MIN);
	index.db.put(&mut transaction, &key, &value).unwrap();
	transaction.commit().unwrap();
	(dir, index)
}

#[tokio::test]
async fn command_objects_can_be_empty_or_multiple_without_a_stored_command() {
	for count in [0, 2] {
		let (_dir, index) = new_index(1);
		let id = tg::process::Id::new();
		let command_id: tg::object::Id = tg::command::Id::new(b"unstored command").into();
		let objects = (0..count).map(object_id).collect::<Vec<_>>();
		let mut process = process_arg(id.clone(), Vec::new(), command_id.clone());
		process.command = Some(objects.clone());
		let mut items = objects
			.iter()
			.map(|id| {
				let mut object = object_arg(id.clone(), [], 1);
				object.storage.insert(tg::object::storage::Set::SUBTREE);
				tangram_index::batch::Item::PutObject(object)
			})
			.collect::<Vec<_>>();
		items.push(tangram_index::batch::Item::PutProcess(process));
		let arg = tangram_index::batch::Arg { items };
		index.batch(arg).await.unwrap();
		loop {
			let output = index
				.update_batch(tangram_index::update::Kind::StorageAndMetadata, 100)
				.await
				.unwrap();
			if output.count == 0 {
				break;
			}
		}
		let process = index
			.try_get_processes(std::slice::from_ref(&id))
			.await
			.unwrap()
			.pop()
			.flatten()
			.unwrap();
		assert_eq!(process.command_id, command_id);
		assert!(process.set.command_objects);
		assert!(
			process
				.storage
				.contains(tg::process::storage::Set::NODE_COMMAND_OBJECTS)
		);
		assert_eq!(process.metadata.node.command_objects.count, Some(count));
		assert_eq!(process.metadata.node.command_objects.size, Some(count));
		assert!(index.try_get_object(&command_id).await.unwrap().is_none());
		let transaction = index.env.read_txn().unwrap();
		let relationships = Index::get_process_objects_with_transaction(
			&index.db,
			&index.subspace,
			&transaction,
			&id,
		)
		.unwrap();
		assert_eq!(relationships.len(), objects.len());
		assert!(
			relationships
				.iter()
				.all(|(object, kind)| objects.contains(object) && kind.is_command())
		);
	}
}

#[tokio::test]
async fn cleans_command_cache_key_without_command_objects() {
	let (_dir, index) = new_index(1);
	let id = tg::process::Id::new();
	let command: tg::object::Id = tg::command::Id::new(b"unstored command").into();
	let mut process = process_arg(id.clone(), Vec::new(), command.clone());
	process.command = Some(Vec::new());
	process.data = Some(
		serde_json::from_value(serde_json::json!({
			"cacheable": true,
			"command": {"node": command.to_string()},
			"created_at": 0,
			"host": "x86_64-linux",
			"sandbox": tg::sandbox::Id::new(),
			"status": "finished"
		}))
		.unwrap(),
	);
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(process)],
	};
	index.batch(arg).await.unwrap();
	assert_eq!(
		index
			.try_get_cached_processes(&command)
			.await
			.unwrap()
			.len(),
		1
	);
	let key = crate::Key::Process(crate::process::Key::CommandCacheableProcess {
		command: command.clone(),
		process: id.clone(),
	});
	let key = Index::pack(&index.subspace, &key);
	for _ in 0..100 {
		let arg = tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: i64::MIN,
			max_process_touched_at: 1,
			max_sandbox_touched_at: i64::MIN,
			now: 1,
			partition_end: 1,
			partition_start: 0,
		};
		if index.clean(arg).await.unwrap().done {
			break;
		}
	}
	assert!(index.try_get_processes(&[id]).await.unwrap()[0].is_none());
	let transaction = index.env.read_txn().unwrap();
	assert!(index.db.get(&transaction, &key).unwrap().is_none());
}

#[tokio::test]
async fn object_put_touched_at_does_not_regress() {
	let (_dir, index) = new_index(1);
	let id = object_id(0);
	for touched_at in [10, 5, 11] {
		let mut arg = object_arg(id.clone(), [], 1);
		arg.touched_at = touched_at;
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutObject(arg)],
		};
		index.batch(arg).await.unwrap();
		let object = index.try_get_object(&id).await.unwrap().unwrap();
		assert_eq!(object.touched_at, touched_at.max(10));
	}
}

fn now() -> i64 {
	i64::try_from(
		std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_secs(),
	)
	.unwrap()
}

async fn get_usage(
	index: &Index,
	account: &tangram_index::usage::Account,
) -> tangram_index::usage::Aggregate {
	let now = jiff::Timestamp::new(now(), 0).unwrap();
	let period =
		tangram_index::usage::Period::containing(tangram_index::usage::PeriodKind::Hour, now);

	index.get_usage(account, period, now).await.unwrap()
}

#[tokio::test]
async fn account_storage_deduplicates_a_diamond_and_cleans() {
	let (_dir, index) = new_index(4);
	let a = object_id(1);
	let b = object_id(2);
	let c = object_id(3);
	let d = object_id(4);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutObject(object_arg(d.clone(), [], 4)),
			tangram_index::batch::Item::PutObject(object_arg(b.clone(), [d.clone()], 3)),
			tangram_index::batch::Item::PutObject(object_arg(c.clone(), [d], 2)),
			tangram_index::batch::Item::PutObject(object_arg(a.clone(), [b, c], 1)),
			tangram_index::batch::Item::PutAccountObject(
				tangram_index::usage::storage::put::ObjectArg {
					account: account.clone(),
					object: a,
					touched_at: 1,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Usage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 4);
	assert_eq!(usage.object_size, 10);
	assert_eq!(usage.process_count, 0);

	for _ in 0..8 {
		let output = index
			.clean(tangram_index::clean::Arg {
				batch_size: 100,
				max_object_touched_at: i64::MAX,
				max_process_touched_at: i64::MAX,
				max_sandbox_touched_at: 1,
				now: now(),
				partition_end: 1,
				partition_start: 0,
			})
			.await
			.unwrap();
		if output.done {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage, tangram_index::usage::Aggregate::default());
}

#[tokio::test]
async fn account_storage_traverses_process_relationships() {
	let (_dir, index) = new_index(1);
	let command = object_id(10);
	let child = tg::process::Id::new();
	let root = tg::process::Id::new();
	let account = tangram_index::usage::Account::Organization(tg::organization::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutObject(object_arg(command.clone(), [], 7)),
			tangram_index::batch::Item::PutProcess(process_arg(
				child.clone(),
				Vec::new(),
				command.clone(),
			)),
			tangram_index::batch::Item::PutProcess(process_arg(root.clone(), vec![child], command)),
			tangram_index::batch::Item::PutAccountProcess(
				tangram_index::usage::storage::put::ProcessArg {
					account: account.clone(),
					process: root,
					touched_at: 1,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Usage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 1);
	assert_eq!(usage.object_size, 7);
	assert_eq!(usage.process_count, 2);
}

#[tokio::test]
async fn account_storage_traverses_new_process_relationships() {
	let (_dir, index) = new_index(1);
	let command = object_id(15);
	let child = tg::process::Id::new();
	let root = tg::process::Id::new();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let partial_root = tangram_index::process::put::Arg {
		cached: false,
		children: None,
		command: Some(vec![command.clone()]),
		command_id: command.clone(),
		data: None,
		error: None,
		id: root.clone(),
		location: None,
		log: None,
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: None,
		parent: None,
		sandbox: None,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 1,
	};
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutObject(object_arg(command.clone(), [], 7)),
			tangram_index::batch::Item::PutProcess(partial_root),
			tangram_index::batch::Item::PutAccountProcess(
				tangram_index::usage::storage::put::ProcessArg {
					account: account.clone(),
					process: root.clone(),
					touched_at: 1,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Usage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}

	let mut root_arg = process_arg(root, vec![child.clone()], command.clone());
	root_arg.error = None;
	root_arg.log = None;
	root_arg.output = None;
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutProcess(process_arg(child, Vec::new(), command)),
			tangram_index::batch::Item::PutProcess(root_arg),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Usage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 1);
	assert_eq!(usage.object_size, 7);
	assert_eq!(usage.process_count, 2);
}

#[tokio::test]
async fn account_storage_traverses_objects_indexed_after_their_parents() {
	let (_dir, index) = new_index(1);
	let child = object_id(17);
	let parent = object_id(16);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutObject(object_arg(parent.clone(), [child.clone()], 3)),
			tangram_index::batch::Item::PutAccountObject(
				tangram_index::usage::storage::put::ObjectArg {
					account: account.clone(),
					object: parent,
					touched_at: 1,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Usage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 1);
	assert_eq!(usage.object_size, 3);

	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutObject(object_arg(
				child,
				[],
				5,
			))],
		})
		.await
		.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Usage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 2);
	assert_eq!(usage.object_size, 8);
}

#[tokio::test]
async fn account_storage_traverses_a_tagged_process_log_indexed_later() {
	let (_dir, index) = new_index(1);
	let command = object_id(21);
	let log = object_id(22);
	let process = tg::process::Id::new();
	let tag = tg::tag::Id::new();
	let user = tg::user::Id::new();
	let account = tangram_index::usage::Account::User(user.clone());
	let mut process_arg = process_arg(process.clone(), Vec::new(), command.clone());
	process_arg.log = Some(Some(log.clone()));
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutUser(tangram_index::user::put::Arg {
				billing_ready: None,
				id: user.clone(),
				specifier: "user".parse().unwrap(),
			}),
			tangram_index::batch::Item::PutObject(object_arg(command, [], 7)),
			tangram_index::batch::Item::PutProcess(process_arg),
			tangram_index::batch::Item::PutTag(tangram_index::tag::put::Arg {
				touched_at: 0,
				version: "initial".into(),
				account: Some(account.clone()),
				id: tag.clone(),
				name: "tag".to_owned(),
				parent: Some(user.into()),
				specifier: "user/tag".parse().unwrap(),
				target: tg::Either::Right(process.clone()),
			}),
			tangram_index::batch::Item::PutPermission(tangram_index::permission::put::Arg {
				created_at: 0,
				creator: None,
				permissions: tg::authorization::permission::Set::Process(
					vec![
						tg::authorization::permission::process::Permission::Subtree,
						tg::authorization::permission::process::Permission::SubtreeCommandObjects,
						tg::authorization::permission::process::Permission::SubtreeLogObjects,
					]
					.into(),
				),
				resource: process.clone().into(),
				source: tangram_index::permission::Source::Direct { expires_at: None },
				subject: tg::authorization::Subject::Tag(tag),
				time_to_touch: None,
				version: Some("initial".into()),
			}),
			tangram_index::batch::Item::PutAccountProcess(
				tangram_index::usage::storage::put::ProcessArg {
					account: account.clone(),
					process,
					touched_at: 1,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Usage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}

	for _ in 0..8 {
		let output = index
			.clean(tangram_index::clean::Arg {
				batch_size: 100,
				max_object_touched_at: i64::MAX,
				max_process_touched_at: i64::MAX,
				max_sandbox_touched_at: 1,
				now: now(),
				partition_end: 1,
				partition_start: 0,
			})
			.await
			.unwrap();
		if output.done {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 1);
	assert_eq!(usage.object_size, 7);
	assert_eq!(usage.process_count, 1);

	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutObject(object_arg(
				log,
				[],
				5,
			))],
		})
		.await
		.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Usage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 2);
	assert_eq!(usage.object_size, 12);
	assert_eq!(usage.process_count, 1);
}

#[tokio::test]
async fn account_storage_traverses_processes_indexed_after_their_parents() {
	let (_dir, index) = new_index(1);
	let command = object_id(18);
	let child = tg::process::Id::new();
	let parent = tg::process::Id::new();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutObject(object_arg(command.clone(), [], 7)),
			tangram_index::batch::Item::PutProcess(process_arg(
				parent.clone(),
				vec![child.clone()],
				command.clone(),
			)),
			tangram_index::batch::Item::PutAccountProcess(
				tangram_index::usage::storage::put::ProcessArg {
					account: account.clone(),
					process: parent,
					touched_at: 1,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Usage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 1);
	assert_eq!(usage.process_count, 1);

	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutProcess(process_arg(
				child,
				Vec::new(),
				command,
			))],
		})
		.await
		.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::Usage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 1);
	assert_eq!(usage.process_count, 2);
}

#[tokio::test]
async fn account_storage_is_retained_by_a_tag() {
	let (_dir, index) = new_index(1);
	let object = object_id(20);
	let user = tg::user::Id::new();
	let account = tangram_index::usage::Account::User(user.clone());
	let tag = tg::tag::Id::new();
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutUser(tangram_index::user::put::Arg {
				billing_ready: None,
				id: user.clone(),
				specifier: "user".parse().unwrap(),
			}),
			tangram_index::batch::Item::PutObject(object_arg(object.clone(), [], 5)),
			tangram_index::batch::Item::PutTag(tangram_index::tag::put::Arg {
				touched_at: 0,
				version: "initial".into(),
				account: Some(account.clone()),
				id: tag.clone(),
				name: "tag".to_owned(),
				parent: Some(user.into()),
				specifier: "user/tag".parse().unwrap(),
				target: tg::Either::Left(object.clone()),
			}),
			tangram_index::batch::Item::PutPermission(tangram_index::permission::put::Arg {
				created_at: 0,
				creator: None,
				permissions: tg::authorization::permission::Set::Object(
					tg::authorization::permission::object::Set::SUBTREE,
				),
				resource: object.clone().into(),
				source: tangram_index::permission::Source::Direct { expires_at: None },
				subject: tg::authorization::Subject::Tag(tag.clone()),
				time_to_touch: None,
				version: Some("initial".into()),
			}),
			tangram_index::batch::Item::PutAccountObject(
				tangram_index::usage::storage::put::ObjectArg {
					account: account.clone(),
					object,
					touched_at: 1,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	let output = index
		.clean(tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: i64::MAX,
			max_process_touched_at: i64::MAX,
			max_sandbox_touched_at: 1,
			now: now(),
			partition_end: 1,
			partition_start: 0,
		})
		.await
		.unwrap();
	assert!(!output.done);
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 1);
	assert_eq!(usage.object_size, 5);

	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::DeleteTag(tag)],
		})
		.await
		.unwrap();
	for _ in 0..4 {
		let output = index
			.clean(tangram_index::clean::Arg {
				batch_size: 100,
				max_object_touched_at: i64::MAX,
				max_process_touched_at: i64::MAX,
				max_sandbox_touched_at: 1,
				now: now(),
				partition_end: 1,
				partition_start: 0,
			})
			.await
			.unwrap();
		if output.done {
			break;
		}
	}
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage, tangram_index::usage::Aggregate::default());
}

#[tokio::test]
async fn account_storage_is_not_retained_by_a_tag_without_permissions() {
	for is_process in [false, true] {
		for has_permissions in [true, false] {
			let (_dir, index) = new_index(1);
			let account = tangram_index::usage::Account::User(tg::user::Id::new());
			let object = object_id(21);
			let process = tg::process::Id::new();
			let tag = tg::tag::Id::new();
			let created_at = 3600;

			let (target, resource, permissions, stored, associated, key) = if is_process {
				(
					tg::Either::Right(process.clone()),
					tg::Id::from(process.clone()),
					tg::authorization::permission::Set::Process(
						tg::authorization::permission::process::Set::SUBTREE,
					),
					tangram_index::batch::Item::PutProcess(process_arg(
						process.clone(),
						Vec::new(),
						object,
					)),
					tangram_index::batch::Item::PutAccountProcess(
						tangram_index::usage::storage::put::ProcessArg {
							account: account.clone(),
							process: process.clone(),
							touched_at: created_at,
						},
					),
					crate::usage::Key::AccountProcess {
						account: account.clone(),
						process,
					},
				)
			} else {
				(
					tg::Either::Left(object.clone()),
					tg::Id::from(object.clone()),
					tg::authorization::permission::Set::Object(
						tg::authorization::permission::object::Set::SUBTREE,
					),
					tangram_index::batch::Item::PutObject(object_arg(object.clone(), [], 5)),
					tangram_index::batch::Item::PutAccountObject(
						tangram_index::usage::storage::put::ObjectArg {
							account: account.clone(),
							object: object.clone(),
							touched_at: created_at,
						},
					),
					crate::usage::Key::AccountObject {
						account: account.clone(),
						object,
					},
				)
			};

			// Create the tag before the account independently stores its target.
			let mut items = vec![
				stored,
				tangram_index::batch::Item::PutTag(tangram_index::tag::put::Arg {
					account: Some(account.clone()),
					id: tag.clone(),
					name: "tag".into(),
					parent: None,
					specifier: "tag".parse().unwrap(),
					target,
					touched_at: created_at,
					version: "initial".into(),
				}),
			];
			if has_permissions {
				let arg = tangram_index::permission::put::Arg {
					created_at,
					creator: None,
					permissions,
					resource,
					source: tangram_index::permission::Source::Direct { expires_at: None },
					subject: tg::authorization::Subject::Tag(tag),
					time_to_touch: None,
					version: Some("initial".into()),
				};
				items.push(tangram_index::batch::Item::PutPermission(arg));
			}
			let arg = tangram_index::batch::Arg { items };
			index.batch(arg).await.unwrap();
			while index
				.update_batch(tangram_index::update::Kind::Usage, 100)
				.await
				.unwrap()
				.count != 0
			{}
			let key = Index::pack(&index.subspace, &crate::Key::Usage(key));
			let transaction = index.env.read_txn().unwrap();
			assert_eq!(
				index.db.get(&transaction, &key).unwrap().is_some(),
				has_permissions
			);
			drop(transaction);

			// Account for an independent push of the target.
			let arg = tangram_index::batch::Arg {
				items: vec![associated],
			};
			index.batch(arg).await.unwrap();

			// Expire the push retention after a two-hour TTL.
			for _ in 0..4 {
				let arg = tangram_index::clean::Arg {
					batch_size: 100,
					max_object_touched_at: created_at,
					max_process_touched_at: created_at,
					max_sandbox_touched_at: 0,
					now: created_at + 2 * 3600,
					partition_end: 1,
					partition_start: 0,
				};
				if index.clean(arg).await.unwrap().done {
					break;
				}
			}
			let transaction = index.env.read_txn().unwrap();
			assert_eq!(
				index.db.get(&transaction, &key).unwrap().is_some(),
				has_permissions,
				"only a tag with target permissions should retain the account storage entry"
			);
		}
	}
}

#[tokio::test]
async fn touching_does_not_create_a_storage_entry() {
	let (_dir, index) = new_index(1);
	let object = object_id(30);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutObject(object_arg(
			object.clone(),
			[],
			5,
		))],
	};
	index.batch(arg).await.unwrap();
	index
		.touch_objects_with_account(
			std::slice::from_ref(&object),
			Some(&account),
			2,
			std::time::Duration::ZERO,
		)
		.await
		.unwrap();
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage, tangram_index::usage::Aggregate::default());
}

#[tokio::test]
async fn touching_an_object_with_its_account_updates_both_lifetimes() {
	let (_dir, index) = new_index(1);
	let object = object_id(31);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutObject(object_arg(object.clone(), [], 5)),
			tangram_index::batch::Item::PutAccountObject(
				tangram_index::usage::storage::put::ObjectArg {
					account: account.clone(),
					object: object.clone(),
					touched_at: 1,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	let objects = index
		.touch_objects_with_account(
			std::slice::from_ref(&object),
			Some(&account),
			10,
			std::time::Duration::ZERO,
		)
		.await
		.unwrap();
	assert_eq!(objects[0].as_ref().unwrap().touched_at, 10);

	index
		.clean(tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: 5,
			max_process_touched_at: 5,
			max_sandbox_touched_at: 5,
			now: now(),
			partition_end: 1,
			partition_start: 0,
		})
		.await
		.unwrap();
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.object_count, 1);
	assert_eq!(usage.object_size, 5);
}

#[tokio::test]
async fn touching_a_process_with_its_account_updates_both_lifetimes() {
	let (_dir, index) = new_index(1);
	let command = object_id(32);
	let process = tg::process::Id::new();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutObject(object_arg(command.clone(), [], 5)),
			tangram_index::batch::Item::PutProcess(process_arg(
				process.clone(),
				Vec::new(),
				command,
			)),
			tangram_index::batch::Item::PutAccountProcess(
				tangram_index::usage::storage::put::ProcessArg {
					account: account.clone(),
					process: process.clone(),
					touched_at: 1,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	let processes = index
		.touch_processes_with_account(
			std::slice::from_ref(&process),
			Some(&account),
			10,
			std::time::Duration::ZERO,
		)
		.await
		.unwrap();
	assert_eq!(processes[0].as_ref().unwrap().touched_at, 10);

	index
		.clean(tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: 5,
			max_process_touched_at: 5,
			max_sandbox_touched_at: 5,
			now: now(),
			partition_end: 1,
			partition_start: 0,
		})
		.await
		.unwrap();
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage.process_count, 1);
}

#[tokio::test]
async fn touching_with_an_account_honors_time_to_touch() {
	let (_dir, index) = new_index(1);
	let object = object_id(33);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutObject(object_arg(object.clone(), [], 5)),
			tangram_index::batch::Item::PutAccountObject(
				tangram_index::usage::storage::put::ObjectArg {
					account: account.clone(),
					object: object.clone(),
					touched_at: 1,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	index
		.touch_objects_with_account(
			std::slice::from_ref(&object),
			Some(&account),
			10,
			std::time::Duration::from_secs(100),
		)
		.await
		.unwrap();

	index
		.clean(tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: 5,
			max_process_touched_at: 5,
			max_sandbox_touched_at: 5,
			now: now(),
			partition_end: 1,
			partition_start: 0,
		})
		.await
		.unwrap();
	let usage = get_usage(&index, &account).await;
	assert_eq!(usage, tangram_index::usage::Aggregate::default());
}

#[test]
fn rejects_zero_usage_partitions() {
	let dir = tempfile::TempDir::new().unwrap();
	let result = Index::new(&Config {
		map_size: 1 << 30,
		max_process_depth: None,
		path: dir.path().join("index"),
		posix_sem_prefix: None,
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total: 0,
		write_operation_batch_size: 100_000,
	});
	assert!(result.is_err());
}

#[tokio::test]
async fn subtree_storage_requires_node_storage() {
	let (_dir, index) = new_index(1);
	let id = object_id(900);
	let mut object = object_arg(id.clone(), [], 1);
	object.storage = tg::object::storage::Set::empty();
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutObject(object.clone())],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::StorageAndMetadata, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
	assert!(
		index
			.try_get_object(&id)
			.await
			.unwrap()
			.unwrap()
			.storage
			.is_empty()
	);
	object.storage = tg::object::storage::Set::NODE;
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutObject(object)],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(tangram_index::update::Kind::StorageAndMetadata, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
	let storage = index.try_get_object(&id).await.unwrap().unwrap().storage;
	assert!(storage.contains(tg::object::storage::Set::NODE | tg::object::storage::Set::SUBTREE));
}

#[tokio::test]
async fn tag_account_transfer_uses_held_permissions_without_incoming_proofs() {
	for is_process in [false, true] {
		for has_permissions in [false, true] {
			let (_dir, index) = new_index(1);
			let first_account = tangram_index::usage::Account::User(tg::user::Id::new());
			let second_account = tangram_index::usage::Account::User(tg::user::Id::new());
			let tag_id = tg::tag::Id::new();
			let created_at = now() - 3;
			let object = object_id(1000);
			let process = tg::process::Id::new();
			let (target, resource, permissions, stored) = if is_process {
				(
					tg::Either::Right(process.clone()),
					tg::Id::from(process.clone()),
					tg::authorization::permission::Set::Process(
						tg::authorization::permission::process::Set::SUBTREE,
					),
					tangram_index::batch::Item::PutProcess(process_arg(
						process,
						Vec::new(),
						object,
					)),
				)
			} else {
				(
					tg::Either::Left(object.clone()),
					tg::Id::from(object.clone()),
					tg::authorization::permission::Set::Object(
						tg::authorization::permission::object::Set::SUBTREE,
					),
					tangram_index::batch::Item::PutObject(object_arg(object, [], 5)),
				)
			};
			let mut tag = tangram_index::tag::put::Arg {
				account: Some(first_account.clone()),
				id: tag_id.clone(),
				name: "tag".into(),
				parent: None,
				specifier: "tag".parse().unwrap(),
				target,
				touched_at: created_at,
				version: "initial".into(),
			};
			let mut items = vec![stored, tangram_index::batch::Item::PutTag(tag.clone())];
			if has_permissions {
				let arg = tangram_index::permission::put::Arg {
					created_at,
					creator: None,
					permissions,
					resource: resource.clone(),
					source: tangram_index::permission::Source::Direct { expires_at: None },
					subject: tg::authorization::Subject::Tag(tag_id.clone()),
					time_to_touch: None,
					version: Some("initial".into()),
				};
				items.push(tangram_index::batch::Item::PutPermission(arg));
			}
			index
				.batch(tangram_index::batch::Arg { items })
				.await
				.unwrap();
			while index
				.update_batch(tangram_index::update::Kind::Usage, 100)
				.await
				.unwrap()
				.count != 0
			{}
			let first_usage = get_usage(&index, &first_account).await;
			assert_eq!(
				first_usage.object_count + first_usage.process_count,
				u64::from(has_permissions)
			);

			// Changing only the account retains the tag's permissions and supplies no new capture evidence.
			tag.account = Some(second_account.clone());
			tag.touched_at = created_at + 1;
			index.put_tags(&[tag]).await.unwrap();
			while index
				.update_batch(tangram_index::update::Kind::Usage, 100)
				.await
				.unwrap()
				.count != 0
			{}
			let transaction = index.env.read_txn().unwrap();
			let key = if is_process {
				crate::Key::Usage(crate::usage::Key::AccountProcess {
					account: second_account.clone(),
					process: resource.clone().try_into().unwrap(),
				})
			} else {
				crate::Key::Usage(crate::usage::Key::AccountObject {
					account: second_account.clone(),
					object: resource.clone().try_into().unwrap(),
				})
			};
			let value = index
				.db
				.get(&transaction, &Index::pack(&index.subspace, &key))
				.unwrap();
			assert_eq!(value.is_some(), has_permissions);
			if let Some(value) = value {
				let entry = tangram_index::usage::storage::Entry::deserialize(value).unwrap();
				assert_eq!(entry.touched_at, created_at + 1);
			}
			drop(transaction);

			// The old account is released while the new tag account retains the target.
			for _ in 0..4 {
				let arg = tangram_index::clean::Arg {
					batch_size: 100,
					max_object_touched_at: i64::MAX,
					max_process_touched_at: i64::MAX,
					max_sandbox_touched_at: i64::MIN,
					now: created_at + 2,
					partition_end: 1,
					partition_start: 0,
				};
				if index.clean(arg).await.unwrap().done {
					break;
				}
			}
			let first_usage = get_usage(&index, &first_account).await;
			assert_eq!(first_usage.object_count + first_usage.process_count, 0);
		}
	}
}

#[tokio::test]
async fn tag_storage_follows_captured_object_permissions() {
	for capture_target in [false, true] {
		for subtree in [false, true] {
			let (_dir, index) = new_index(1);
			let account = tangram_index::usage::Account::User(tg::user::Id::new());
			let tag = tg::tag::Id::new();
			let root = object_id(1100);
			let captured = object_id(1101);
			let child = object_id(1102);
			let inaccessible = object_id(1103);
			let created_at = 3600;
			let arg = tangram_index::batch::Arg {
				items: vec![
					tangram_index::batch::Item::PutObject(object_arg(
						root.clone(),
						[captured.clone(), inaccessible.clone()],
						1,
					)),
					tangram_index::batch::Item::PutObject(object_arg(
						captured.clone(),
						[child.clone()],
						2,
					)),
					tangram_index::batch::Item::PutObject(object_arg(child.clone(), [], 3)),
					tangram_index::batch::Item::PutObject(object_arg(inaccessible.clone(), [], 4)),
					tangram_index::batch::Item::PutTag(tangram_index::tag::put::Arg {
						account: Some(account.clone()),
						id: tag.clone(),
						name: "tag".into(),
						parent: None,
						specifier: "tag".parse().unwrap(),
						target: tg::Either::Left(root.clone()),
						touched_at: created_at,
						version: "initial".into(),
					}),
				],
			};
			index.batch(arg).await.unwrap();
			for (object, permission) in [
				(
					root.clone(),
					tg::authorization::permission::object::Permission::Node,
				),
				(
					captured.clone(),
					if subtree {
						tg::authorization::permission::object::Permission::Subtree
					} else {
						tg::authorization::permission::object::Permission::Node
					},
				),
			]
			.into_iter()
			.filter(|(object, _)| capture_target || object != &root)
			{
				let arg = tangram_index::permission::put::Arg {
					created_at,
					creator: None,
					permissions: tg::authorization::Permission::Object(permission).into(),
					resource: object.into(),
					source: tangram_index::permission::Source::Direct { expires_at: None },
					subject: tg::authorization::Subject::Tag(tag.clone()),
					time_to_touch: None,
					version: Some("initial".into()),
				};
				index.put_permissions(&[arg]).await.unwrap();
			}
			while index
				.update_batch(tangram_index::update::Kind::Usage, 100)
				.await
				.unwrap()
				.count != 0
			{}
			for (object, expected) in [
				(root.clone(), capture_target),
				(captured.clone(), true),
				(child.clone(), subtree),
				(inaccessible.clone(), false),
			] {
				let key = crate::Key::Usage(crate::usage::Key::AccountObject {
					account: account.clone(),
					object,
				});
				let key = Index::pack(&index.subspace, &key);
				let transaction = index.env.read_txn().unwrap();
				assert_eq!(
					index.db.get(&transaction, &key).unwrap().is_some(),
					expected,
					"storage must follow each captured permission"
				);
			}
			// Transfer every captured resource to the new account.
			let previous_account = account;
			let account = tangram_index::usage::Account::User(tg::user::Id::new());
			let arg = tangram_index::tag::put::Arg {
				account: Some(account.clone()),
				id: tag.clone(),
				name: "tag".into(),
				parent: None,
				specifier: "tag".parse().unwrap(),
				target: tg::Either::Left(root.clone()),
				touched_at: created_at,
				version: "initial".into(),
			};
			index.put_tags(&[arg]).await.unwrap();
			while index
				.update_batch(tangram_index::update::Kind::Usage, 100)
				.await
				.unwrap()
				.count != 0
			{}
			for _ in 0..8 {
				let arg = tangram_index::clean::Arg {
					batch_size: 100,
					max_object_touched_at: created_at,
					max_process_touched_at: created_at,
					max_sandbox_touched_at: 0,
					now: created_at + 2 * 3600,
					partition_end: 1,
					partition_start: 0,
				};
				if index.clean(arg).await.unwrap().done {
					break;
				}
			}
			for (object, expected) in [
				(root, capture_target),
				(captured, true),
				(child, subtree),
				(inaccessible, false),
			] {
				for (account, expected) in [(&previous_account, false), (&account, expected)] {
					let key = crate::Key::Usage(crate::usage::Key::AccountObject {
						account: account.clone(),
						object: object.clone(),
					});
					let key = Index::pack(&index.subspace, &key);
					let transaction = index.env.read_txn().unwrap();
					assert_eq!(
						index.db.get(&transaction, &key).unwrap().is_some(),
						expected,
						"account transfer must preserve the captured permissions"
					);
				}
			}

			index.delete_tags(&[tag]).await.unwrap();
			for _ in 0..8 {
				let arg = tangram_index::clean::Arg {
					batch_size: 100,
					max_object_touched_at: created_at,
					max_process_touched_at: created_at,
					max_sandbox_touched_at: 0,
					now: created_at + 2 * 3600,
					partition_end: 1,
					partition_start: 0,
				};
				if index.clean(arg).await.unwrap().done {
					break;
				}
			}
			let now = jiff::Timestamp::new(created_at + 2 * 3600, 0).unwrap();
			let period = tangram_index::usage::Period::containing(
				tangram_index::usage::PeriodKind::Hour,
				now,
			);
			assert_eq!(
				index.get_usage(&account, period, now).await.unwrap(),
				tangram_index::usage::Aggregate::default()
			);
		}
	}
}

#[tokio::test]
async fn tag_storage_follows_captured_process_permissions() {
	for kind in [
		tangram_index::process::object::Kind::Command,
		tangram_index::process::object::Kind::Error,
		tangram_index::process::object::Kind::Log,
		tangram_index::process::object::Kind::Output,
	] {
		let (node, subtree) = match kind {
			tangram_index::process::object::Kind::Command => (
				tg::authorization::permission::process::Permission::NodeCommandObjects,
				tg::authorization::permission::process::Permission::SubtreeCommandObjects,
			),
			tangram_index::process::object::Kind::Error => (
				tg::authorization::permission::process::Permission::NodeErrorObjects,
				tg::authorization::permission::process::Permission::SubtreeErrorObjects,
			),
			tangram_index::process::object::Kind::Log => (
				tg::authorization::permission::process::Permission::NodeLogObjects,
				tg::authorization::permission::process::Permission::SubtreeLogObjects,
			),
			tangram_index::process::object::Kind::Output => (
				tg::authorization::permission::process::Permission::NodeOutputObjects,
				tg::authorization::permission::process::Permission::SubtreeOutputObjects,
			),
		};
		for (case, permission) in [
			(0, tg::authorization::permission::process::Permission::Node),
			(1, node),
			(
				2,
				tg::authorization::permission::process::Permission::Subtree,
			),
			(3, subtree),
		] {
			let (_dir, index) = new_index(1);
			let account = tangram_index::usage::Account::User(tg::user::Id::new());
			let tag = tg::tag::Id::new();
			let root = tg::process::Id::new();
			let child = tg::process::Id::new();
			let command = object_id(1200);
			let output = object_id(1201);
			let output_child = object_id(1202);
			let child_output = object_id(1203);
			let created_at = 3600;
			let mut root_arg = process_arg(root.clone(), vec![child.clone()], command.clone());
			let mut child_arg = process_arg(child, Vec::new(), command.clone());
			match kind {
				tangram_index::process::object::Kind::Command => {
					root_arg.command = Some(vec![output.clone()]);
					child_arg.command = Some(vec![child_output.clone()]);
				},
				tangram_index::process::object::Kind::Error => {
					root_arg.error = Some(Some(vec![output.clone()]));
					child_arg.error = Some(Some(vec![child_output.clone()]));
				},
				tangram_index::process::object::Kind::Log => {
					root_arg.log = Some(Some(output.clone()));
					child_arg.log = Some(Some(child_output.clone()));
				},
				tangram_index::process::object::Kind::Output => {
					root_arg.output = Some(Some(vec![output.clone()]));
					child_arg.output = Some(Some(vec![child_output.clone()]));
				},
			}
			let arg = tangram_index::batch::Arg {
				items: vec![
					tangram_index::batch::Item::PutObject(object_arg(command, [], 1)),
					tangram_index::batch::Item::PutObject(object_arg(child_output, [], 4)),
					tangram_index::batch::Item::PutProcess(root_arg),
					tangram_index::batch::Item::PutProcess(child_arg),
					tangram_index::batch::Item::PutTag(tangram_index::tag::put::Arg {
						account: Some(account.clone()),
						id: tag.clone(),
						name: "tag".into(),
						parent: None,
						specifier: "tag".parse().unwrap(),
						target: tg::Either::Right(root.clone()),
						touched_at: created_at,
						version: "initial".into(),
					}),
				],
			};
			index.batch(arg).await.unwrap();
			let arg = tangram_index::permission::put::Arg {
				created_at,
				creator: None,
				permissions: tg::authorization::Permission::Process(permission).into(),
				resource: root.into(),
				source: tangram_index::permission::Source::Direct { expires_at: None },
				subject: tg::authorization::Subject::Tag(tag.clone()),
				time_to_touch: None,
				version: Some("initial".into()),
			};
			index.put_permissions(&[arg]).await.unwrap();
			while index
				.update_batch(tangram_index::update::Kind::Usage, 100)
				.await
				.unwrap()
				.count != 0
			{}

			// Index the output subtree after capturing the process permissions.
			let arg = tangram_index::batch::Arg {
				items: vec![
					tangram_index::batch::Item::PutObject(object_arg(
						output,
						[output_child.clone()],
						2,
					)),
					tangram_index::batch::Item::PutObject(object_arg(output_child, [], 3)),
				],
			};
			index.batch(arg).await.unwrap();
			while index
				.update_batch(tangram_index::update::Kind::Usage, 100)
				.await
				.unwrap()
				.count != 0
			{}
			let now = jiff::Timestamp::new(created_at, 0).unwrap();
			let period = tangram_index::usage::Period::containing(
				tangram_index::usage::PeriodKind::Hour,
				now,
			);
			let usage = index.get_usage(&account, period, now).await.unwrap();
			let (object_count, object_size, process_count) = match case {
				0 => (0, 0, 1),
				1 => (2, 5, 0),
				2 => (0, 0, 2),
				3 => (3, 9, 0),
				_ => unreachable!(),
			};
			assert_eq!(usage.object_count, object_count, "{permission:?}");
			assert_eq!(usage.object_size, object_size, "{permission:?}");
			assert_eq!(usage.process_count, process_count, "{permission:?}");

			index.delete_tags(&[tag]).await.unwrap();
			for _ in 0..8 {
				let arg = tangram_index::clean::Arg {
					batch_size: 100,
					max_object_touched_at: created_at,
					max_process_touched_at: created_at,
					max_sandbox_touched_at: 0,
					now: created_at + 2 * 3600,
					partition_end: 1,
					partition_start: 0,
				};
				if index.clean(arg).await.unwrap().done {
					break;
				}
			}
			let now = jiff::Timestamp::new(created_at + 2 * 3600, 0).unwrap();
			let period = tangram_index::usage::Period::containing(
				tangram_index::usage::PeriodKind::Hour,
				now,
			);
			assert_eq!(
				index.get_usage(&account, period, now).await.unwrap(),
				tangram_index::usage::Aggregate::default()
			);
		}
	}
}

#[tokio::test]
async fn tag_storage_updates_permissions_after_push_retention_and_capture() {
	let (_dir, index) = new_index(1);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let root = object_id(1300);
	let child = object_id(1301);
	let tag = tg::tag::Id::new();
	let created_at = 3600;
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutObject(object_arg(root.clone(), [child.clone()], 1)),
			tangram_index::batch::Item::PutObject(object_arg(child.clone(), [], 2)),
			tangram_index::batch::Item::PutTag(tangram_index::tag::put::Arg {
				account: Some(account.clone()),
				id: tag.clone(),
				name: "tag".into(),
				parent: None,
				specifier: "tag".parse().unwrap(),
				target: tg::Either::Left(root.clone()),
				touched_at: created_at,
				version: "initial".into(),
			}),
			tangram_index::batch::Item::PutAccountObject(
				tangram_index::usage::storage::put::ObjectArg {
					account: account.clone(),
					object: root.clone(),
					touched_at: created_at,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	let mut permission = tangram_index::permission::put::Arg {
		created_at,
		creator: None,
		permissions: tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		)
		.into(),
		resource: root.into(),
		source: tangram_index::permission::Source::Direct { expires_at: None },
		subject: tg::authorization::Subject::Tag(tag),
		time_to_touch: None,
		version: Some("initial".into()),
	};
	index.put_permissions(&[permission.clone()]).await.unwrap();
	while index
		.update_batch(tangram_index::update::Kind::Usage, 100)
		.await
		.unwrap()
		.count != 0
	{}

	// Retain only the captured node after the independent push expires.
	for _ in 0..8 {
		let arg = tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: created_at,
			max_process_touched_at: created_at,
			max_sandbox_touched_at: 0,
			now: created_at + 2 * 3600,
			partition_end: 1,
			partition_start: 0,
		};
		if index.clean(arg).await.unwrap().done {
			break;
		}
	}
	let now = jiff::Timestamp::new(created_at + 2 * 3600, 0).unwrap();
	let period =
		tangram_index::usage::Period::containing(tangram_index::usage::PeriodKind::Hour, now);
	let usage = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(usage.object_count, 1);
	assert_eq!(usage.object_size, 1);

	// Capture subtree permissions after the original propagation has completed.
	permission.created_at = now.as_second();
	permission.permissions = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	)
	.into();
	index.put_permissions(&[permission]).await.unwrap();
	while index
		.update_batch(tangram_index::update::Kind::Usage, 100)
		.await
		.unwrap()
		.count != 0
	{}
	let usage = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(usage.object_count, 2);
	assert_eq!(usage.object_size, 3);
}

#[tokio::test]
async fn account_storage_uses_cached_references_until_a_parent_changes() {
	let (_dir, index) = new_index(1);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let root = object_id(1400);
	let child = object_id(1401);
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutObject(object_arg(root.clone(), [child.clone()], 1)),
			tangram_index::batch::Item::PutObject(object_arg(child.clone(), [], 2)),
			tangram_index::batch::Item::PutAccountObject(
				tangram_index::usage::storage::put::ObjectArg {
					account: account.clone(),
					object: root.clone(),
					touched_at: 3600,
				},
			),
		],
	};
	index.batch(arg).await.unwrap();
	while index
		.update_batch(tangram_index::update::Kind::Usage, 100)
		.await
		.unwrap()
		.count != 0
	{}
	let mut transaction = index.env.write_txn().unwrap();
	let key = super::super::Key::Usage(super::super::usage::Key::AccountObject {
		account: account.clone(),
		object: child.clone(),
	});
	let key = Index::pack(&index.subspace, &key);
	Index::clean_account_object_entry(
		&index.db,
		&index.subspace,
		&mut transaction,
		&account,
		&child,
		10800,
		3600,
		1,
	)
	.unwrap();
	let entry = tangram_index::usage::storage::Entry::deserialize(
		index.db.get(&transaction, &key).unwrap().unwrap(),
	)
	.unwrap();
	assert_eq!(entry.reference_count, 1);

	// A queued permission addition must not read the parent value, even for a node-only entry.
	let mut entry = entry;
	entry.permissions = tg::authorization::permission::Set::Object(
		tg::authorization::permission::object::Set::NODE,
	);
	index
		.db
		.put(&mut transaction, &key, &entry.serialize().unwrap())
		.unwrap();

	let parent_key = super::super::Key::Usage(super::super::usage::Key::AccountObject {
		account: account.clone(),
		object: root.clone(),
	});
	let parent_key = Index::pack(&index.subspace, &parent_key);
	let parent_value = index
		.db
		.get(&transaction, &parent_key)
		.unwrap()
		.unwrap()
		.to_vec();
	index.db.put(&mut transaction, &parent_key, &[255]).unwrap();
	let arg = tangram_index::usage::storage::put::ObjectArg {
		account: account.clone(),
		object: child.clone(),
		touched_at: 3600,
	};
	Index::put_account_object(
		&index.db,
		&index.subspace,
		&mut transaction,
		&arg,
		1,
		Some(tg::authorization::permission::Set::Object(
			tg::authorization::permission::object::Set::SUBTREE,
		)),
		None,
	)
	.unwrap();
	index
		.db
		.put(&mut transaction, &parent_key, &parent_value)
		.unwrap();

	// An expansion invalidates the references until cleanup confirms the retaining parent.
	let entry = tangram_index::usage::storage::Entry::deserialize(
		index.db.get(&transaction, &key).unwrap().unwrap(),
	)
	.unwrap();
	assert_eq!(entry.reference_count, 0);
	Index::clean_account_object_entry(
		&index.db,
		&index.subspace,
		&mut transaction,
		&account,
		&child,
		10800,
		3600,
		1,
	)
	.unwrap();
	let entry = tangram_index::usage::storage::Entry::deserialize(
		index.db.get(&transaction, &key).unwrap().unwrap(),
	)
	.unwrap();
	assert_eq!(entry.reference_count, 1);

	// Touching a retained entry does not create another cleanup key.
	let arg = tangram_index::usage::storage::put::ObjectArg {
		touched_at: 4000,
		..arg
	};
	Index::touch_account_object(
		&index.db,
		&index.subspace,
		&mut transaction,
		&arg,
		std::time::Duration::ZERO,
	)
	.unwrap();
	let clean_key = super::super::Key::Clean(super::super::clean::Key::AccountObject {
		account: account.clone(),
		object: child.clone(),
		touched_at: 4000,
	});
	let clean_key = Index::pack(&index.subspace, &clean_key);
	assert!(index.db.get(&transaction, &clean_key).unwrap().is_none());

	// Deleting the parent invalidates the cache and schedules the retained child.
	Index::clean_account_object_entry(
		&index.db,
		&index.subspace,
		&mut transaction,
		&account,
		&root,
		10800,
		3600,
		1,
	)
	.unwrap();
	let entry = tangram_index::usage::storage::Entry::deserialize(
		index.db.get(&transaction, &key).unwrap().unwrap(),
	)
	.unwrap();
	assert_eq!(entry.reference_count, 0);
	assert!(index.db.get(&transaction, &clean_key).unwrap().is_some());

	// A subsequent touch preserves the pending cleanup at the new timestamp.
	let arg = tangram_index::usage::storage::put::ObjectArg {
		touched_at: 5000,
		..arg
	};
	Index::touch_account_object(
		&index.db,
		&index.subspace,
		&mut transaction,
		&arg,
		std::time::Duration::ZERO,
	)
	.unwrap();
	Index::clean_account_object_entry(
		&index.db,
		&index.subspace,
		&mut transaction,
		&account,
		&child,
		10800,
		5000,
		1,
	)
	.unwrap();
	assert!(index.db.get(&transaction, &key).unwrap().is_none());
}

#[tokio::test]
async fn tag_storage_merges_queued_permissions_and_retains_the_other_tag() {
	let (_dir, index) = new_index(1);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let process = tg::process::Id::new();
	let command = object_id(1500);
	let log = object_id(1501);
	let output = object_id(1502);
	let log_tag = tg::tag::Id::new();
	let output_tag = tg::tag::Id::new();
	let created_at = 3600;
	let mut process_arg = process_arg(process.clone(), Vec::new(), command.clone());
	process_arg.log = Some(Some(log.clone()));
	process_arg.output = Some(Some(vec![output.clone()]));
	let mut items = vec![
		tangram_index::batch::Item::PutObject(object_arg(command, [], 1)),
		tangram_index::batch::Item::PutObject(object_arg(log, [], 2)),
		tangram_index::batch::Item::PutObject(object_arg(output, [], 3)),
		tangram_index::batch::Item::PutProcess(process_arg),
	];
	for (id, name) in [(&log_tag, "log"), (&output_tag, "output")] {
		let arg = tangram_index::tag::put::Arg {
			account: Some(account.clone()),
			id: id.clone(),
			name: name.into(),
			parent: None,
			specifier: name.parse().unwrap(),
			target: tg::Either::Right(process.clone()),
			touched_at: created_at,
			version: "initial".into(),
		};
		items.push(tangram_index::batch::Item::PutTag(arg));
	}
	let arg = tangram_index::batch::Arg { items };
	index.batch(arg).await.unwrap();

	// Different permission additions at the same timestamp must both survive queueing.
	for (tag, permission) in [
		(
			&log_tag,
			tg::authorization::permission::process::Permission::NodeLogObjects,
		),
		(
			&output_tag,
			tg::authorization::permission::process::Permission::NodeOutputObjects,
		),
	] {
		let arg = tangram_index::permission::put::Arg {
			created_at,
			creator: None,
			permissions: tg::authorization::Permission::Process(permission).into(),
			resource: process.clone().into(),
			source: tangram_index::permission::Source::Direct { expires_at: None },
			subject: tg::authorization::Subject::Tag(tag.clone()),
			time_to_touch: None,
			version: Some("initial".into()),
		};
		index.put_permissions(&[arg]).await.unwrap();
	}
	while index
		.update_batch(tangram_index::update::Kind::Usage, 100)
		.await
		.unwrap()
		.count != 0
	{}
	let now = jiff::Timestamp::new(created_at, 0).unwrap();
	let period =
		tangram_index::usage::Period::containing(tangram_index::usage::PeriodKind::Hour, now);
	let usage = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(usage.object_count, 2);
	assert_eq!(usage.object_size, 5);
	assert_eq!(usage.process_count, 0);

	// Deleting one tag removes only the storage retained by its permissions.
	index.delete_tags(&[log_tag]).await.unwrap();
	for _ in 0..8 {
		let arg = tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: created_at,
			max_process_touched_at: created_at,
			max_sandbox_touched_at: 0,
			now: created_at + 2 * 3600,
			partition_end: 1,
			partition_start: 0,
		};
		if index.clean(arg).await.unwrap().done {
			break;
		}
	}
	let now = jiff::Timestamp::new(created_at + 2 * 3600, 0).unwrap();
	let period =
		tangram_index::usage::Period::containing(tangram_index::usage::PeriodKind::Hour, now);
	let usage = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(usage.object_count, 1);
	assert_eq!(usage.object_size, 3);
	assert_eq!(usage.process_count, 0);
}

#[tokio::test]
async fn tag_storage_expands_permissions_during_propagation() {
	let (_dir, index) = new_index(1);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let root = tg::process::Id::new();
	let children = std::array::from_fn::<_, 3, _>(|_| tg::process::Id::new());
	let leaves = std::array::from_fn::<_, 3, _>(|_| tg::process::Id::new());
	let processes = std::iter::once(&root)
		.chain(&children)
		.chain(&leaves)
		.collect::<Vec<_>>();
	let logs = (0..processes.len())
		.map(|index| object_id(1700 + index as u64))
		.collect::<Vec<_>>();
	let command = object_id(1800);
	let tag = tg::tag::Id::new();
	let mut items = Vec::new();
	for (position, (id, log)) in processes.iter().zip(&logs).enumerate() {
		let children = match position {
			0 => children.to_vec(),
			1..=3 => vec![leaves[position - 1].clone()],
			_ => Vec::new(),
		};
		let mut arg = process_arg((*id).clone(), children, command.clone());
		arg.log = Some(Some(log.clone()));
		items.push(tangram_index::batch::Item::PutProcess(arg));
		items.push(tangram_index::batch::Item::PutObject(object_arg(
			log.clone(),
			[],
			position as u64 + 1,
		)));
	}
	let arg = tangram_index::tag::put::Arg {
		account: Some(account.clone()),
		id: tag.clone(),
		name: "tag".into(),
		parent: None,
		specifier: "tag".parse().unwrap(),
		target: tg::Either::Right(root.clone()),
		touched_at: 3600,
		version: "initial".into(),
	};
	items.push(tangram_index::batch::Item::PutTag(arg));
	let arg = tangram_index::batch::Arg { items };
	index.batch(arg).await.unwrap();
	let mut permission = tangram_index::permission::put::Arg {
		created_at: 3600,
		creator: None,
		permissions: tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::SubtreeLogObjects,
		)
		.into(),
		resource: root.clone().into(),
		source: tangram_index::permission::Source::Direct { expires_at: None },
		subject: tg::authorization::Subject::Tag(tag),
		time_to_touch: None,
		version: Some("initial".into()),
	};
	index.put_permissions(&[permission.clone()]).await.unwrap();
	let entry = |process: &tg::process::Id| {
		let transaction = index.env.read_txn().unwrap();
		let key = super::super::Key::Usage(super::super::usage::Key::AccountProcess {
			account: account.clone(),
			process: process.clone(),
		});
		index
			.db
			.get(&transaction, &Index::pack(&index.subspace, &key))
			.unwrap()
			.map(|value| tangram_index::usage::storage::Entry::deserialize(value).unwrap())
	};

	// Stop after one child has propagated log permissions, with the other branches pending.
	for _ in 0..16 {
		if children.iter().any(|process| entry(process).is_some()) {
			break;
		}
		assert_eq!(
			index
				.update_batch(tangram_index::update::Kind::Usage, 1)
				.await
				.unwrap()
				.count,
			1
		);
	}
	let visited = children
		.iter()
		.filter(|process| entry(process).is_some())
		.collect::<Vec<_>>();
	assert_eq!(visited.len(), 1);
	assert!(!entry(visited[0]).unwrap().stores_node());
	assert!(leaves.iter().all(|process| entry(process).is_none()));
	assert!(
		index
			.try_get_oldest_update_transaction_id(tangram_index::update::Kind::Usage)
			.await
			.unwrap()
			.is_some()
	);

	// Add process storage while the log-only traversal is still queued.
	permission.created_at = 7200;
	permission.permissions = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Subtree,
	)
	.into();
	index.put_permissions(&[permission]).await.unwrap();
	let mut drained = false;
	for _ in 0..128 {
		if index
			.update_batch(tangram_index::update::Kind::Usage, 1)
			.await
			.unwrap()
			.count == 0
		{
			drained = true;
			break;
		}
	}
	assert!(drained, "the usage updates must finish");

	// The expansion must revisit existing entries and cover every pending branch without duplicate charges.
	for process in &processes {
		let entry = entry(process).unwrap();
		assert!(entry.stores_node());
		assert!(
			entry
				.permissions
				.contains(tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Subtree
				))
		);
		assert!(
			entry
				.permissions
				.contains(tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::SubtreeLogObjects
				))
		);
	}
	for log in &logs {
		let transaction = index.env.read_txn().unwrap();
		let key = super::super::Key::Usage(super::super::usage::Key::AccountObject {
			account: account.clone(),
			object: log.clone(),
		});
		assert!(
			index
				.db
				.get(&transaction, &Index::pack(&index.subspace, &key))
				.unwrap()
				.is_some()
		);
	}
	let now = jiff::Timestamp::new(7200, 0).unwrap();
	let period =
		tangram_index::usage::Period::containing(tangram_index::usage::PeriodKind::Hour, now);
	let usage = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(usage.object_count, 7);
	assert_eq!(usage.object_size, 28);
	assert_eq!(usage.process_count, 7);
}

#[tokio::test]
async fn tag_storage_propagates_permissions_when_cleanup_precedes_the_queued_put() {
	for processes in [false, true] {
		tag_storage_with_cleanup_before_a_queued_permission_addition(processes, false).await;
	}
}

#[tokio::test]
async fn tag_storage_cleans_a_queued_permission_addition_after_tag_deletion() {
	for processes in [false, true] {
		tag_storage_with_cleanup_before_a_queued_permission_addition(processes, true).await;
	}
}

async fn tag_storage_with_cleanup_before_a_queued_permission_addition(
	processes: bool,
	delete_tag: bool,
) {
	let (_dir, index) = new_index(1);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let tag = tg::tag::Id::new();
	let added_tag = if delete_tag {
		tg::tag::Id::new()
	} else {
		tag.clone()
	};
	let (root, child, leaf, mut items) = if processes {
		let root = tg::process::Id::new();
		let child = tg::process::Id::new();
		let leaf = tg::process::Id::new();
		let command = object_id(1603);
		let items = vec![
			tangram_index::batch::Item::PutProcess(process_arg(
				root.clone(),
				vec![child.clone()],
				command.clone(),
			)),
			tangram_index::batch::Item::PutProcess(process_arg(
				child.clone(),
				vec![leaf.clone()],
				command.clone(),
			)),
			tangram_index::batch::Item::PutProcess(process_arg(leaf.clone(), vec![], command)),
		];
		(
			tg::Either::Right(root),
			tg::Either::Right(child),
			tg::Either::Right(leaf),
			items,
		)
	} else {
		let root = object_id(1600);
		let child = object_id(1601);
		let leaf = object_id(1602);
		let items = vec![
			tangram_index::batch::Item::PutObject(object_arg(root.clone(), [child.clone()], 1)),
			tangram_index::batch::Item::PutObject(object_arg(child.clone(), [leaf.clone()], 2)),
			tangram_index::batch::Item::PutObject(object_arg(leaf.clone(), [], 3)),
		];
		(
			tg::Either::Left(root),
			tg::Either::Left(child),
			tg::Either::Left(leaf),
			items,
		)
	};
	let tags = if delete_tag {
		vec![(&tag, "node"), (&added_tag, "subtree")]
	} else {
		vec![(&tag, "node")]
	};
	for (id, name) in tags {
		let arg = tangram_index::tag::put::Arg {
			account: Some(account.clone()),
			id: id.clone(),
			name: name.into(),
			parent: None,
			specifier: name.parse().unwrap(),
			target: root.clone(),
			touched_at: 3600,
			version: "initial".into(),
		};
		items.push(tangram_index::batch::Item::PutTag(arg));
	}
	let arg = tangram_index::batch::Arg { items };
	index.batch(arg).await.unwrap();
	let node = if processes {
		tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		)
	} else {
		tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		)
	};
	for resource in [&root, &child] {
		let resource = match resource {
			tg::Either::Left(object) => tg::Id::from(object.clone()),
			tg::Either::Right(process) => tg::Id::from(process.clone()),
		};
		let arg = tangram_index::permission::put::Arg {
			created_at: 3600,
			creator: None,
			permissions: node.into(),
			resource,
			source: tangram_index::permission::Source::Direct { expires_at: None },
			subject: tg::authorization::Subject::Tag(tag.clone()),
			time_to_touch: None,
			version: Some("initial".into()),
		};
		index.put_permissions(&[arg]).await.unwrap();
	}
	while index
		.update_batch(tangram_index::update::Kind::Usage, 100)
		.await
		.unwrap()
		.count != 0
	{}

	// Capture an addition while existing entries are already eligible for cleanup.
	let resource = match &root {
		tg::Either::Left(object) => tg::Id::from(object.clone()),
		tg::Either::Right(process) => tg::Id::from(process.clone()),
	};
	let arg = tangram_index::permission::put::Arg {
		created_at: 10800,
		creator: None,
		permissions: node.subtree().into(),
		resource,
		source: tangram_index::permission::Source::Direct { expires_at: None },
		subject: tg::authorization::Subject::Tag(added_tag.clone()),
		time_to_touch: None,
		version: Some("initial".into()),
	};
	index.put_permissions(&[arg]).await.unwrap();
	let resource = if delete_tag {
		index.delete_tags(&[added_tag]).await.unwrap();
		&root
	} else {
		assert_eq!(
			index
				.update_batch(tangram_index::update::Kind::Usage, 1)
				.await
				.unwrap()
				.count,
			1
		);
		&child
	};

	// Run cleanup before the queued addition to this entry is processed.
	let mut transaction = index.env.write_txn().unwrap();
	match resource {
		tg::Either::Left(object) => Index::clean_account_object_entry(
			&index.db,
			&index.subspace,
			&mut transaction,
			&account,
			object,
			10800,
			3600,
			1,
		)
		.unwrap(),
		tg::Either::Right(process) => Index::clean_account_process_entry(
			&index.db,
			&index.subspace,
			&mut transaction,
			&account,
			process,
			10800,
			3600,
			1,
		)
		.unwrap(),
	}
	transaction.commit().unwrap();
	while index
		.update_batch(tangram_index::update::Kind::Usage, 100)
		.await
		.unwrap()
		.count != 0
	{}

	// Deleted captures must be removed even when their puts run after the deletion cleanup.
	if delete_tag {
		let transaction = index.env.read_txn().unwrap();
		let (entry_key, clean_key) = match &root {
			tg::Either::Left(object) => (
				super::super::Key::Usage(super::super::usage::Key::AccountObject {
					account: account.clone(),
					object: object.clone(),
				}),
				super::super::Key::Clean(super::super::clean::Key::AccountObject {
					account: account.clone(),
					object: object.clone(),
					touched_at: 3600,
				}),
			),
			tg::Either::Right(process) => (
				super::super::Key::Usage(super::super::usage::Key::AccountProcess {
					account: account.clone(),
					process: process.clone(),
				}),
				super::super::Key::Clean(super::super::clean::Key::AccountProcess {
					account: account.clone(),
					process: process.clone(),
					touched_at: 3600,
				}),
			),
		};
		let entry = tangram_index::usage::storage::Entry::deserialize(
			index
				.db
				.get(&transaction, &Index::pack(&index.subspace, &entry_key))
				.unwrap()
				.unwrap(),
		)
		.unwrap();
		assert_eq!(entry.reference_count, 0);
		assert_eq!(entry.touched_at, 3600);
		assert!(
			index
				.db
				.get(&transaction, &Index::pack(&index.subspace, &clean_key))
				.unwrap()
				.is_some()
		);
		drop(transaction);

		for _ in 0..8 {
			let arg = tangram_index::clean::Arg {
				batch_size: 100,
				max_object_touched_at: 10800,
				max_process_touched_at: 10800,
				max_sandbox_touched_at: 0,
				now: 18000,
				partition_end: 1,
				partition_start: 0,
			};
			if index.clean(arg).await.unwrap().done {
				break;
			}
		}
	}
	let transaction = index.env.read_txn().unwrap();
	let key = match leaf {
		tg::Either::Left(object) => super::super::usage::Key::AccountObject {
			account: account.clone(),
			object,
		},
		tg::Either::Right(process) => super::super::usage::Key::AccountProcess {
			account: account.clone(),
			process,
		},
	};
	let key = super::super::Key::Usage(key);
	let stored = index
		.db
		.get(&transaction, &Index::pack(&index.subspace, &key))
		.unwrap()
		.is_some();
	assert_eq!(
		stored, !delete_tag,
		"the leaf's storage must match the surviving captures"
	);
	let now = jiff::Timestamp::new(18000, 0).unwrap();
	let period =
		tangram_index::usage::Period::containing(tangram_index::usage::PeriodKind::Hour, now);
	let usage = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(
		usage.object_count,
		if processes {
			0
		} else if delete_tag {
			2
		} else {
			3
		}
	);
	assert_eq!(
		usage.object_size,
		if processes {
			0
		} else if delete_tag {
			3
		} else {
			6
		}
	);
	assert_eq!(
		usage.process_count,
		if processes {
			if delete_tag { 2 } else { 3 }
		} else {
			0
		}
	);
}
