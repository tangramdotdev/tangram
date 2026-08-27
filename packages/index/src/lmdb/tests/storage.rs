use {
	super::super::{Config, Index},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

fn object_id(value: u64) -> tg::object::Id {
	tg::object::Id::new(tg::object::Kind::Blob, &value.to_le_bytes().to_vec().into())
}

fn object_arg(
	id: tg::object::Id,
	children: impl IntoIterator<Item = tg::object::Id>,
	size: u64,
) -> crate::object::put::Arg {
	crate::object::put::Arg {
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
		storage: crate::object::Storage::default(),
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 1,
	}
}

fn process_arg(
	id: tg::process::Id,
	children: Vec<tg::process::Id>,
	command: tg::object::Id,
) -> crate::process::put::Arg {
	let children = children
		.into_iter()
		.map(|child| tg::process::data::Child {
			cached: false,
			process: tg::Referent::with_node(child),
		})
		.collect();
	crate::process::put::Arg {
		cached: false,
		children: Some(children),
		command,
		data: None,
		error: Some(None),
		id,
		log: Some(None),
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: Some(None),
		parent: None,
		sandbox: None,
		storage: crate::process::Storage::default(),
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 1,
	}
}

fn new_index(usage_partition_total: u64) -> (tempfile::TempDir, Index) {
	let dir = tempfile::TempDir::new().unwrap();
	let index = Index::new(&Config {
		authorize: super::super::AuthorizeConfig {
			process_object_grant: crate::authorize::Config::default(),
		},
		map_size: 1 << 30,
		max_process_depth: None,
		path: dir.path().join("index"),
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total,
		write_operation_batch_size: 100_000,
	})
	.unwrap();
	let mut transaction = index.env.write_txn().unwrap();
	let key = Index::pack(
		&index.subspace,
		&crate::lmdb::Key::Usage(crate::lmdb::usage::Key::Started),
	);
	let value = crate::usage::serialize_timestamp(i64::MIN);
	index.db.put(&mut transaction, &key, &value).unwrap();
	transaction.commit().unwrap();
	(dir, index)
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

async fn get_usage(index: &Index, account: &crate::usage::Account) -> crate::usage::Aggregate {
	let now = jiff::Timestamp::new(now(), 0).unwrap();
	let period = crate::usage::Period::containing(crate::usage::PeriodKind::Hour, now);

	index.get_usage(account, period, now).await.unwrap()
}

#[tokio::test]
async fn account_storage_deduplicates_a_diamond_and_cleans() {
	let (_dir, index) = new_index(4);
	let a = object_id(1);
	let b = object_id(2);
	let c = object_id(3);
	let d = object_id(4);
	let account = crate::usage::Account::User(tg::user::Id::new());
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutObject(object_arg(d.clone(), [], 4)),
			crate::batch::Item::PutObject(object_arg(b.clone(), [d.clone()], 3)),
			crate::batch::Item::PutObject(object_arg(c.clone(), [d], 2)),
			crate::batch::Item::PutObject(object_arg(a.clone(), [b, c], 1)),
			crate::batch::Item::PutAccountObject(crate::usage::storage::put::ObjectArg {
				account: account.clone(),
				object: a,
				touched_at: 1,
			}),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(crate::update::Kind::Storage, 100)
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
			.clean(crate::clean::Arg {
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
	assert_eq!(usage, crate::usage::Aggregate::default());
}

#[tokio::test]
async fn account_storage_traverses_process_relationships() {
	let (_dir, index) = new_index(1);
	let command = object_id(10);
	let child = tg::process::Id::new();
	let root = tg::process::Id::new();
	let account = crate::usage::Account::Organization(tg::organization::Id::new());
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutObject(object_arg(command.clone(), [], 7)),
			crate::batch::Item::PutProcess(process_arg(child.clone(), Vec::new(), command.clone())),
			crate::batch::Item::PutProcess(process_arg(root.clone(), vec![child], command)),
			crate::batch::Item::PutAccountProcess(crate::usage::storage::put::ProcessArg {
				account: account.clone(),
				process: root,
				touched_at: 1,
			}),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(crate::update::Kind::Storage, 100)
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
	let account = crate::usage::Account::User(tg::user::Id::new());
	let partial_root = crate::process::put::Arg {
		cached: false,
		children: None,
		command: command.clone(),
		data: None,
		error: None,
		id: root.clone(),
		log: None,
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: None,
		parent: None,
		sandbox: None,
		storage: crate::process::Storage::default(),
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 1,
	};
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutObject(object_arg(command.clone(), [], 7)),
			crate::batch::Item::PutProcess(partial_root),
			crate::batch::Item::PutAccountProcess(crate::usage::storage::put::ProcessArg {
				account: account.clone(),
				process: root.clone(),
				touched_at: 1,
			}),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(crate::update::Kind::Storage, 100)
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
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutProcess(process_arg(child, Vec::new(), command)),
			crate::batch::Item::PutProcess(root_arg),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(crate::update::Kind::Storage, 100)
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
	let account = crate::usage::Account::User(tg::user::Id::new());
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutObject(object_arg(parent.clone(), [child.clone()], 3)),
			crate::batch::Item::PutAccountObject(crate::usage::storage::put::ObjectArg {
				account: account.clone(),
				object: parent,
				touched_at: 1,
			}),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(crate::update::Kind::Storage, 100)
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
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::PutObject(object_arg(child, [], 5))],
		})
		.await
		.unwrap();
	loop {
		let output = index
			.update_batch(crate::update::Kind::Storage, 100)
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
	let account = crate::usage::Account::User(user.clone());
	let mut process_arg = process_arg(process.clone(), Vec::new(), command.clone());
	process_arg.log = Some(Some(log.clone()));
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutUser(crate::user::put::Arg {
				billing: None,
				id: user.clone(),
				specifier: "user".parse().unwrap(),
			}),
			crate::batch::Item::PutObject(object_arg(command, [], 7)),
			crate::batch::Item::PutProcess(process_arg),
			crate::batch::Item::PutTag(crate::tag::put::Arg {
				account: Some(account.clone()),
				id: tag,
				name: "tag".to_owned(),
				parent: Some(user.into()),
				permissions: Vec::new(),
				specifier: "user/tag".parse().unwrap(),
				target: tg::Either::Right(process.clone()),
			}),
			crate::batch::Item::PutAccountProcess(crate::usage::storage::put::ProcessArg {
				account: account.clone(),
				process,
				touched_at: 1,
			}),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(crate::update::Kind::Storage, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}

	for _ in 0..8 {
		let output = index
			.clean(crate::clean::Arg {
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
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::PutObject(object_arg(log, [], 5))],
		})
		.await
		.unwrap();
	loop {
		let output = index
			.update_batch(crate::update::Kind::Storage, 100)
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
	let account = crate::usage::Account::User(tg::user::Id::new());
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutObject(object_arg(command.clone(), [], 7)),
			crate::batch::Item::PutProcess(process_arg(
				parent.clone(),
				vec![child.clone()],
				command.clone(),
			)),
			crate::batch::Item::PutAccountProcess(crate::usage::storage::put::ProcessArg {
				account: account.clone(),
				process: parent,
				touched_at: 1,
			}),
		],
	};
	index.batch(arg).await.unwrap();
	loop {
		let output = index
			.update_batch(crate::update::Kind::Storage, 100)
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
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::PutProcess(process_arg(
				child,
				Vec::new(),
				command,
			))],
		})
		.await
		.unwrap();
	loop {
		let output = index
			.update_batch(crate::update::Kind::Storage, 100)
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
	let account = crate::usage::Account::User(user.clone());
	let tag = tg::tag::Id::new();
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutUser(crate::user::put::Arg {
				billing: None,
				id: user.clone(),
				specifier: "user".parse().unwrap(),
			}),
			crate::batch::Item::PutObject(object_arg(object.clone(), [], 5)),
			crate::batch::Item::PutTag(crate::tag::put::Arg {
				account: Some(account.clone()),
				id: tag.clone(),
				name: "tag".to_owned(),
				parent: Some(user.into()),
				permissions: Vec::new(),
				specifier: "user/tag".parse().unwrap(),
				target: tg::Either::Left(object.clone()),
			}),
			crate::batch::Item::PutAccountObject(crate::usage::storage::put::ObjectArg {
				account: account.clone(),
				object,
				touched_at: 1,
			}),
		],
	};
	index.batch(arg).await.unwrap();
	let output = index
		.clean(crate::clean::Arg {
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
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::DeleteTag(tag)],
		})
		.await
		.unwrap();
	for _ in 0..4 {
		let output = index
			.clean(crate::clean::Arg {
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
	assert_eq!(usage, crate::usage::Aggregate::default());
}

#[tokio::test]
async fn touching_does_not_create_a_storage_entry() {
	let (_dir, index) = new_index(1);
	let object = object_id(30);
	let account = crate::usage::Account::User(tg::user::Id::new());
	let arg = crate::batch::Arg {
		items: vec![crate::batch::Item::PutObject(object_arg(
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
	assert_eq!(usage, crate::usage::Aggregate::default());
}

#[tokio::test]
async fn touching_an_object_with_its_account_updates_both_lifetimes() {
	let (_dir, index) = new_index(1);
	let object = object_id(31);
	let account = crate::usage::Account::User(tg::user::Id::new());
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutObject(object_arg(object.clone(), [], 5)),
			crate::batch::Item::PutAccountObject(crate::usage::storage::put::ObjectArg {
				account: account.clone(),
				object: object.clone(),
				touched_at: 1,
			}),
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
		.clean(crate::clean::Arg {
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
	let account = crate::usage::Account::User(tg::user::Id::new());
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutObject(object_arg(command.clone(), [], 5)),
			crate::batch::Item::PutProcess(process_arg(process.clone(), Vec::new(), command)),
			crate::batch::Item::PutAccountProcess(crate::usage::storage::put::ProcessArg {
				account: account.clone(),
				process: process.clone(),
				touched_at: 1,
			}),
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
		.clean(crate::clean::Arg {
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
	let account = crate::usage::Account::User(tg::user::Id::new());
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutObject(object_arg(object.clone(), [], 5)),
			crate::batch::Item::PutAccountObject(crate::usage::storage::put::ObjectArg {
				account: account.clone(),
				object: object.clone(),
				touched_at: 1,
			}),
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
		.clean(crate::clean::Arg {
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
	assert_eq!(usage, crate::usage::Aggregate::default());
}

#[test]
fn rejects_zero_usage_partitions() {
	let dir = tempfile::TempDir::new().unwrap();
	let result = Index::new(&Config {
		authorize: super::super::AuthorizeConfig {
			process_object_grant: crate::authorize::Config::default(),
		},
		map_size: 1 << 30,
		max_process_depth: None,
		path: dir.path().join("index"),
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total: 0,
		write_operation_batch_size: 100_000,
	});
	assert!(result.is_err());
}
