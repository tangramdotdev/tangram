use {
	super::{super::Index, new_index},
	std::str::FromStr as _,
	tangram_client::prelude::*,
	tangram_index::Index as _,
};

fn try_get_group(index: &Index, id: &tg::group::Id) -> Option<tangram_index::group::Group> {
	let transaction = index.env.read_txn().unwrap();
	Index::try_get_group_with_transaction(&index.db, &index.subspace, &transaction, id).unwrap()
}

fn try_get_organization(
	index: &Index,
	id: &tg::organization::Id,
) -> Option<tangram_index::organization::Organization> {
	let transaction = index.env.read_txn().unwrap();
	Index::try_get_organization_with_transaction(&index.db, &index.subspace, &transaction, id)
		.unwrap()
}

fn try_get_user(index: &Index, id: &tg::user::Id) -> Option<tangram_index::user::User> {
	let transaction = index.env.read_txn().unwrap();
	Index::try_get_user_with_transaction(&index.db, &index.subspace, &transaction, id).unwrap()
}

fn process_arg(
	id: tg::process::Id,
	status: tg::process::Status,
) -> tangram_index::process::put::Arg {
	let command = tg::command::Id::new(b"command");
	let finished = status.is_finished();
	let data = tg::process::Data {
		actual_checksum: None,
		cacheable: false,
		children: None,
		command: tg::Referent::with_node(tg::Either::Right(command.clone())),
		created_at: 0,
		debug: None,
		error: None,
		exit: finished.then_some(0),
		expected_checksum: None,
		finished_at: finished.then_some(0),
		host: String::new(),
		log: None,
		output: None,
		retry: false,
		sandbox: Some(tg::sandbox::Id::new()),
		started_at: Some(0),
		status,
		stderr: tg::process::Stdio::default(),
		stdin: tg::process::Stdio::default(),
		stdout: tg::process::Stdio::default(),
		tty: None,
	};
	tangram_index::process::put::Arg {
		cached: false,
		children: None,
		command: Some(vec![command.clone().into()]),
		command_id: command.into(),
		data: Some(data),
		error: None,
		id: id.clone(),
		location: None,
		log: None,
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: None,
		parent: None,
		permissions: Vec::new(),
		principal: tg::Principal::Process(id),
		sandbox: None,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	}
}

fn sandbox_arg(
	id: tg::sandbox::Id,
	status: tg::sandbox::Status,
) -> tangram_index::sandbox::put::Arg {
	let data = tg::sandbox::get::Output {
		data: tg::sandbox::Data {
			cpu: None,
			creator: None,
			hostname: None,
			id: id.clone(),
			isolation: None,
			memory: None,
			mounts: Vec::new(),
			network: None,
			owner: None,
			status,
			ttl: None,
			usage: None,
		},
		location: None,
		tokens: tg::authorization::Tokens::default(),
	};
	tangram_index::sandbox::put::Arg {
		account: None,
		created_at: 0,
		data: Some(data),
		id: id.clone(),
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Sandbox(id),
		processes: None,
		runner: None,
		touched_at: 0,
	}
}

#[tokio::test]
async fn partial_account_updates_preserve_billing() {
	let (_dir, index) = new_index();
	let new_organization = tg::organization::Id::new();
	let new_user = tg::user::Id::new();
	let organization = tg::organization::Id::new();
	let user = tg::user::Id::new();
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutOrganization(tangram_index::organization::put::Arg {
				billing_ready: None,
				id: new_organization.clone(),
				specifier: tg::Specifier::from_str("new_organization").unwrap(),
			}),
			tangram_index::batch::Item::PutOrganization(tangram_index::organization::put::Arg {
				billing_ready: Some(true),
				id: organization.clone(),
				specifier: tg::Specifier::from_str("organization").unwrap(),
			}),
			tangram_index::batch::Item::PutUser(tangram_index::user::put::Arg {
				billing_ready: None,
				id: new_user.clone(),
				specifier: tg::Specifier::from_str("new_user").unwrap(),
			}),
			tangram_index::batch::Item::PutUser(tangram_index::user::put::Arg {
				billing_ready: Some(true),
				id: user.clone(),
				specifier: tg::Specifier::from_str("user").unwrap(),
			}),
		],
	};
	index.batch(arg).await.unwrap().unwrap();

	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutOrganization(tangram_index::organization::put::Arg {
				billing_ready: None,
				id: organization.clone(),
				specifier: tg::Specifier::from_str("organization").unwrap(),
			}),
			tangram_index::batch::Item::PutUser(tangram_index::user::put::Arg {
				billing_ready: None,
				id: user.clone(),
				specifier: tg::Specifier::from_str("user").unwrap(),
			}),
		],
	};
	index.batch(arg).await.unwrap().unwrap();

	assert!(
		!try_get_organization(&index, &new_organization)
			.unwrap()
			.billing_ready
	);
	assert!(
		try_get_organization(&index, &organization)
			.unwrap()
			.billing_ready
	);
	assert!(!try_get_user(&index, &new_user).unwrap().billing_ready);
	assert!(try_get_user(&index, &user).unwrap().billing_ready);
}

#[tokio::test]
async fn process_children_are_stored_separately_from_data() {
	let (_dir, index) = new_index();
	let mut ids = (0..3).map(|_| tg::process::Id::new()).collect::<Vec<_>>();
	ids.sort();
	ids.reverse();
	let expected = ids
		.into_iter()
		.enumerate()
		.map(|(index, id)| tg::process::data::Child {
			cached: index == 0,
			process: tg::Referent::with_node(id),
		})
		.collect::<Vec<_>>();
	let command = tg::command::Id::new(b"command");
	let missing = tg::process::Id::new();
	let process = tg::process::Id::new();
	let data = tg::process::Data {
		actual_checksum: None,
		cacheable: false,
		children: Some(expected.clone()),
		command: tg::Referent::with_node(tg::Either::Right(command.clone())),
		created_at: 0,
		debug: None,
		error: None,
		exit: Some(0),
		expected_checksum: None,
		finished_at: Some(0),
		host: String::new(),
		log: None,
		output: None,
		retry: false,
		sandbox: Some(tg::sandbox::Id::new()),
		started_at: Some(0),
		status: tg::process::Status::Finished,
		stderr: tg::process::Stdio::default(),
		stdin: tg::process::Stdio::default(),
		stdout: tg::process::Stdio::default(),
		tty: None,
	};
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(
			tangram_index::process::put::Arg {
				cached: false,
				children: Some(expected.clone()),
				command: Some(vec![command.clone().into()]),
				command_id: command.into(),
				data: Some(data),
				error: Some(None),
				id: process.clone(),
				location: None,
				log: Some(None),
				metadata: tg::process::Metadata::default(),
				options: tg::referent::Options::default(),
				output: Some(None),
				parent: None,
				permissions: Vec::new(),
				principal: tg::Principal::Process(process.clone()),
				sandbox: None,
				storage: tg::process::storage::Set::NODE,
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 0,
			},
		)],
	};
	index.batch(arg).await.unwrap().unwrap();

	let indexed = index
		.try_get_processes(std::slice::from_ref(&process))
		.await
		.unwrap()
		.pop()
		.unwrap()
		.unwrap();
	assert!(indexed.data.unwrap().children.is_none());
	assert!(indexed.set.children);
	assert_eq!(
		index
			.try_get_process_children_count(&process)
			.await
			.unwrap(),
		Some(expected.len() as u64)
	);
	let children = index
		.try_get_process_children(&process, std::io::SeekFrom::Start(0), 10)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(children.len(), expected.len());
	for (actual, expected) in std::iter::zip(&children, &expected) {
		assert_eq!(actual.cached, expected.cached);
		assert_eq!(actual.process.node, expected.process.node);
	}
	let children = index
		.try_get_process_children(&process, std::io::SeekFrom::End(-2), 1)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(children.len(), 1);
	assert_eq!(children[0].cached, expected[1].cached);
	assert_eq!(children[0].process.node, expected[1].process.node);
	let children = index
		.try_get_process_children(&process, std::io::SeekFrom::Start(1), 2)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(
		children
			.iter()
			.map(|child| &child.process.node)
			.collect::<Vec<_>>(),
		expected[1..]
			.iter()
			.map(|child| &child.process.node)
			.collect::<Vec<_>>(),
	);
	let children = index
		.try_get_process_children(&process, std::io::SeekFrom::End(-2), 2)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(
		children
			.iter()
			.map(|child| &child.process.node)
			.collect::<Vec<_>>(),
		expected[1..]
			.iter()
			.map(|child| &child.process.node)
			.collect::<Vec<_>>(),
	);
	let children = index
		.try_get_process_children(&missing, std::io::SeekFrom::Start(0), 10)
		.await
		.unwrap();
	assert!(children.is_none());
}

#[tokio::test]
async fn incomplete_process_children_have_values() {
	let (_dir, index) = new_index();
	let child = tg::process::Id::new();
	let command = tg::command::Id::new(b"command");
	let parent = tg::process::Id::new();
	let entry = tg::referent::Options {
		name: Some("child".into()),
		..Default::default()
	};
	let child_data = tg::process::data::Child {
		cached: true,
		process: tg::Referent::new(child.clone(), entry),
	};
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutProcess(tangram_index::process::put::Arg {
				cached: false,
				children: None,
				command: Some(vec![command.clone().into()]),
				command_id: command.clone().into(),
				data: None,
				error: None,
				id: parent.clone(),
				location: None,
				log: None,
				metadata: tg::process::Metadata::default(),
				options: tg::referent::Options::default(),
				output: None,
				parent: None,
				permissions: Vec::new(),
				principal: tg::Principal::Process(parent.clone()),
				sandbox: None,
				storage: tg::process::storage::Set::NODE,
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 0,
			}),
			tangram_index::batch::Item::PutProcess(tangram_index::process::put::Arg {
				cached: child_data.cached,
				children: None,
				command: Some(vec![command.clone().into()]),
				command_id: command.into(),
				data: None,
				error: None,
				id: child.clone(),
				location: None,
				log: None,
				metadata: tg::process::Metadata::default(),
				options: child_data.process.options.clone(),
				output: None,
				parent: Some(parent.clone()),
				permissions: Vec::new(),
				principal: tg::Principal::Process(child.clone()),
				sandbox: None,
				storage: tg::process::storage::Set::NODE,
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 0,
			}),
		],
	};
	index.batch(arg).await.unwrap().unwrap();

	let process = index
		.try_get_processes(std::slice::from_ref(&parent))
		.await
		.unwrap()
		.pop()
		.unwrap()
		.unwrap();
	assert!(!process.set.children);
	let children = index
		.try_get_process_children(&parent, std::io::SeekFrom::Start(0), 1)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(children.len(), 1);
	assert!(children[0].cached);
	assert_eq!(children[0].process.node, child);
	assert_eq!(children[0].process.options.name.as_deref(), Some("child"));
}

#[tokio::test]
async fn process_children_must_be_unique() {
	let (_dir, index) = new_index();
	let child = tg::process::data::Child {
		cached: false,
		process: tg::Referent::with_node(tg::process::Id::new()),
	};
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(
			tangram_index::process::put::Arg {
				cached: false,
				children: Some(vec![child.clone(), child]),
				command: Some(vec![tg::command::Id::new(b"command").into()]),
				command_id: tg::command::Id::new(b"command").into(),
				data: None,
				error: None,
				id: tg::process::Id::new(),
				location: None,
				log: None,
				metadata: tg::process::Metadata::default(),
				options: tg::referent::Options::default(),
				output: None,
				parent: None,
				permissions: Vec::new(),
				principal: tg::Principal::Root,
				sandbox: None,
				storage: tg::process::storage::Set::NODE,
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 0,
			},
		)],
	};
	let error = index.batch(arg).await.unwrap().unwrap_err();
	assert!(
		error
			.to_string()
			.contains("process children must be unique")
	);
}

#[tokio::test]
async fn process_status_does_not_regress() {
	let (_dir, index) = new_index();
	let id = tg::process::Id::new();
	let parent = tg::process::Id::new();
	for status in [tg::process::Status::Finished, tg::process::Status::Started] {
		let mut process = process_arg(id.clone(), status);
		if status.is_started() {
			process.parent = Some(parent.clone());
		}
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutProcess(process)],
		};
		index.batch(arg).await.unwrap().unwrap();
	}

	let process = index
		.try_get_process(&id)
		.await
		.unwrap()
		.unwrap()
		.data
		.unwrap();
	assert!(process.status.is_finished());
	let transaction = index.env.read_txn().unwrap();
	let parents =
		Index::get_process_parents_with_transaction(&index.db, &index.subspace, &transaction, &id)
			.unwrap();
	assert_eq!(parents, vec![parent]);
}

#[tokio::test]
async fn sandbox_status_does_not_regress() {
	let (_dir, index) = new_index();
	let id = tg::sandbox::Id::new();
	let location = tg::Location::Local(tg::location::Local {
		region: Some("test".to_owned()),
	});
	for status in [tg::sandbox::Status::Destroyed, tg::sandbox::Status::Started] {
		let mut sandbox = sandbox_arg(id.clone(), status);
		sandbox.location = Some(location.clone());
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(sandbox)],
		};
		index.batch(arg).await.unwrap().unwrap();
	}

	let sandbox = index.try_get_sandbox(&id).await.unwrap().unwrap();
	assert_eq!(sandbox.location, Some(location.clone()));
	let sandbox = sandbox.data.unwrap();
	assert!(sandbox.data.status.is_destroyed());
	assert_eq!(sandbox.location, Some(location));
}

#[tokio::test]
async fn preserves_order_and_transaction_boundary() {
	let (_dir, index) = new_index();
	let id = tg::group::Id::new();
	let put_arg = tangram_index::group::put::Arg {
		id: id.clone(),
		parent: None,
		specifier: tg::Specifier::from_str("test").unwrap(),
	};

	let before = index.get_transaction_id().await.unwrap();
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutGroup(put_arg.clone()),
			tangram_index::batch::Item::DeleteGroup(id.clone()),
		],
	};
	index.batch(arg).await.unwrap().unwrap();
	let after = index.get_transaction_id().await.unwrap();
	assert_eq!(after, before + 1);
	assert!(try_get_group(&index, &id).is_none());

	let before = index.get_transaction_id().await.unwrap();
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::DeleteGroup(id.clone()),
			tangram_index::batch::Item::PutGroup(put_arg),
		],
	};
	index.batch(arg).await.unwrap().unwrap();
	let after = index.get_transaction_id().await.unwrap();
	assert_eq!(after, before + 1);
	assert!(try_get_group(&index, &id).is_some());
}

#[tokio::test]
async fn sandbox_processes_are_ordered_and_stored_separately() {
	let (_dir, index) = new_index();
	let sandbox = tg::sandbox::Id::new();
	let first = tg::process::Id::new();
	let second = tg::process::Id::new();
	let mut first_arg = process_arg(first.clone(), tg::process::Status::Started);
	first_arg.sandbox = None;
	first_arg.data.as_mut().unwrap().sandbox = Some(sandbox.clone());
	let mut second_arg = process_arg(second.clone(), tg::process::Status::Started);
	second_arg.sandbox = None;
	second_arg.data.as_mut().unwrap().sandbox = Some(sandbox.clone());
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutSandbox(sandbox_arg(
				sandbox.clone(),
				tg::sandbox::Status::Started,
			)),
			tangram_index::batch::Item::PutProcess(second_arg),
			tangram_index::batch::Item::PutProcess(first_arg.clone()),
			tangram_index::batch::Item::PutProcess(first_arg),
		],
	};
	index.batch(arg).await.unwrap().unwrap();
	let processes = index
		.try_get_sandbox_processes(&sandbox, std::io::SeekFrom::Start(0), 10)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(processes, Vec::new());
	assert_eq!(
		index
			.try_get_sandbox_processes_count(&sandbox)
			.await
			.unwrap(),
		Some(0)
	);

	// Permission spawn updates append once in indexed order without finalizing the list.
	let mut first_membership = process_arg(first.clone(), tg::process::Status::Started);
	first_membership.data = None;
	first_membership.sandbox = Some(sandbox.clone());
	let mut second_membership = first_membership.clone();
	second_membership.id = second.clone();
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutProcess(second_membership),
			tangram_index::batch::Item::PutProcess(first_membership.clone()),
			tangram_index::batch::Item::PutProcess(first_membership),
		],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert_eq!(
		index
			.get_sandbox_processes(&sandbox, std::io::SeekFrom::Start(0), 10)
			.await
			.unwrap(),
		[second.clone(), first.clone()]
	);
	assert!(
		!index
			.try_get_sandbox(&sandbox)
			.await
			.unwrap()
			.unwrap()
			.set
			.processes
	);

	// A data-only snapshot does not claim to contain a complete process history.
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutSandbox(sandbox_arg(
			sandbox.clone(),
			tg::sandbox::Status::Destroyed,
		))],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert!(
		!index
			.try_get_sandbox(&sandbox)
			.await
			.unwrap()
			.unwrap()
			.set
			.processes
	);

	// A final runner snapshot establishes runner order without growing the sandbox record.
	let processes = [first.clone(), second.clone()]
		.into_iter()
		.chain((0..3998).map(|_| tg::process::Id::new()))
		.collect::<Vec<_>>();
	let mut final_arg = sandbox_arg(sandbox.clone(), tg::sandbox::Status::Destroyed);
	final_arg.processes = Some(processes.clone());
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutSandbox(final_arg)],
	};
	index.batch(arg).await.unwrap().unwrap();
	let indexed = index.try_get_sandbox(&sandbox).await.unwrap().unwrap();
	assert!(indexed.serialize().unwrap().len() < 1000);
	assert_eq!(
		index
			.try_get_sandbox_processes_count(&sandbox)
			.await
			.unwrap(),
		Some(4000)
	);
	assert_eq!(
		index
			.try_get_sandbox_processes(&sandbox, std::io::SeekFrom::End(-2), 2)
			.await
			.unwrap()
			.unwrap(),
		processes[3998..]
	);
	// Both APIs return the ordered list, including processes that are not stored locally.
	assert_eq!(
		index
			.get_sandbox_processes(&sandbox, std::io::SeekFrom::Start(0), 4000)
			.await
			.unwrap(),
		processes
	);
	let missing = tg::sandbox::Id::new();
	assert!(
		index
			.try_get_sandbox_processes(&missing, std::io::SeekFrom::Start(0), 1)
			.await
			.unwrap()
			.is_none()
	);
	assert!(
		index
			.get_sandbox_processes(&missing, std::io::SeekFrom::Start(0), 1)
			.await
			.is_err()
	);

	// Replayed complete lists must not change finalized ordering.
	let mut replay = sandbox_arg(sandbox.clone(), tg::sandbox::Status::Destroyed);
	replay.processes = Some(vec![second.clone(), first.clone()]);
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutSandbox(replay)],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert_eq!(
		index
			.try_get_sandbox_processes(&sandbox, std::io::SeekFrom::Start(0), 4000)
			.await
			.unwrap()
			.unwrap(),
		processes
	);

	// A delayed spawn update must not change the finalized process list.
	let mut membership = process_arg(tg::process::Id::new(), tg::process::Status::Started);
	membership.data = None;
	membership.sandbox = Some(sandbox.clone());
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(membership)],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert_eq!(
		index
			.get_sandbox_processes(&sandbox, std::io::SeekFrom::Start(0), 4000)
			.await
			.unwrap(),
		processes
	);

	// The sandbox retains its processes even when the processes are eligible for cleanup.
	loop {
		let arg = tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: i64::MIN,
			max_process_touched_at: i64::MAX,
			max_sandbox_touched_at: i64::MIN,
			now: 1,
			partition_end: 1,
			partition_start: 0,
		};
		if index.clean(arg).await.unwrap().done {
			break;
		}
	}
	assert!(index.try_get_process(&first).await.unwrap().is_some());
	assert!(index.try_get_process(&second).await.unwrap().is_some());

	assert_eq!(
		index
			.try_get_sandbox_processes(&sandbox, std::io::SeekFrom::Start(0), 4000)
			.await
			.unwrap()
			.unwrap(),
		processes
	);

	// Cleaning the sandbox releases its processes so cleanup can collect them too.
	loop {
		let arg = tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: i64::MIN,
			max_process_touched_at: i64::MAX,
			max_sandbox_touched_at: i64::MAX,
			now: 2,
			partition_end: 1,
			partition_start: 0,
		};
		if index.clean(arg).await.unwrap().done {
			break;
		}
	}
	assert!(index.try_get_sandbox(&sandbox).await.unwrap().is_none());
	assert!(index.try_get_process(&first).await.unwrap().is_none());
	assert!(index.try_get_process(&second).await.unwrap().is_none());
	index
		.delete_sandboxes(std::slice::from_ref(&sandbox))
		.await
		.unwrap();
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutSandbox(sandbox_arg(
			sandbox.clone(),
			tg::sandbox::Status::Started,
		))],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert_eq!(
		index
			.try_get_sandbox_processes_count(&sandbox)
			.await
			.unwrap(),
		Some(0)
	);
	// An ordinary process write must not recreate either sandbox relationship.
	let mut arg = process_arg(first.clone(), tg::process::Status::Started);
	arg.sandbox = None;
	arg.data.as_mut().unwrap().sandbox = Some(sandbox.clone());
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(arg)],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert_eq!(
		index
			.try_get_sandbox_processes(&sandbox, std::io::SeekFrom::Start(0), 1)
			.await
			.unwrap()
			.unwrap(),
		Vec::<tg::process::Id>::new()
	);
}

#[tokio::test]
async fn ordinary_process_writes_do_not_create_sandbox_relationships() {
	let (_dir, index) = new_index();
	let sandbox = tg::sandbox::Id::new();
	let process = tg::process::Id::new();
	let mut process_arg = process_arg(process.clone(), tg::process::Status::Finished);
	process_arg.sandbox = None;
	process_arg.data.as_mut().unwrap().sandbox = Some(sandbox.clone());

	// A process-only write preserves its sandbox field without establishing membership.
	for touched_at in [0, 1] {
		process_arg.touched_at = touched_at;
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutProcess(process_arg.clone())],
		};
		index.batch(arg).await.unwrap().unwrap();
		assert!(index.try_get_sandbox(&sandbox).await.unwrap().is_none());
		let indexed = index.try_get_process(&process).await.unwrap().unwrap();
		assert_eq!(indexed.sandbox, Some(sandbox.clone()));
		let transaction = index.env.read_txn().unwrap();
		for key in [
			crate::Key::Sandbox(crate::sandbox::Key::SandboxProcess {
				position: 0,
				process: process.clone(),
				sandbox: sandbox.clone(),
			}),
			crate::Key::Process(crate::process::Key::ProcessSandbox {
				process: process.clone(),
				sandbox: sandbox.clone(),
			}),
		] {
			let key = Index::pack(&index.subspace, &key);
			assert!(index.db.get(&transaction, &key).unwrap().is_none());
		}
	}

	// Only the sandbox write establishes membership.
	let mut membership = sandbox_arg(sandbox.clone(), tg::sandbox::Status::Destroyed);
	membership.processes = Some(vec![process.clone()]);
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutSandbox(membership)],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert_eq!(
		index
			.get_sandbox_processes(&sandbox, std::io::SeekFrom::Start(0), 10)
			.await
			.unwrap(),
		std::slice::from_ref(&process)
	);

	// A process update must not recreate membership in a deleted sandbox.
	index
		.delete_sandboxes(std::slice::from_ref(&sandbox))
		.await
		.unwrap();
	let mut membership = process_arg.clone();
	membership.data = None;
	membership.id = process.clone();
	membership.sandbox = Some(sandbox.clone());
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(membership)],
	};
	index.batch(arg).await.unwrap().unwrap();

	process_arg.touched_at = 2;
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(process_arg)],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert!(index.try_get_sandbox(&sandbox).await.unwrap().is_none());
	let transaction = index.env.read_txn().unwrap();
	let key = crate::Key::Process(crate::process::Key::ProcessSandbox { process, sandbox });
	let key = Index::pack(&index.subspace, &key);
	assert!(index.db.get(&transaction, &key).unwrap().is_none());
}

#[tokio::test]
async fn root_can_write_and_finish_existing_processes() {
	let (_dir, index) = new_index();
	let id = tg::process::Id::new();
	let mut arg = process_arg(id.clone(), tg::process::Status::Started);
	arg.principal = tg::Principal::Root;
	put_process(&index, arg.clone()).await.unwrap();
	arg.children = Some(Vec::new());
	let data = arg.data.as_mut().unwrap();
	data.exit = Some(1);
	data.finished_at = Some(1);
	data.status = tg::process::Status::Finished;
	put_process(&index, arg.clone()).await.unwrap();
	let stored = index.try_get_process(&id).await.unwrap().unwrap();
	let data = stored.data.unwrap();
	assert_eq!(data.status, tg::process::Status::Finished);
	assert_eq!(data.exit, Some(1));
	assert!(stored.set.children);

	// Root must still submit unique children.
	let child = tg::process::data::Child {
		cached: false,
		process: tg::Referent::with_node(tg::process::Id::new()),
	};
	arg.children = Some(vec![child.clone(), child]);
	assert!(put_process(&index, arg).await.is_err());
}

#[tokio::test]
async fn root_can_write_and_destroy_existing_sandboxes() {
	let (_dir, index) = new_index();
	let id = tg::sandbox::Id::new();
	let mut arg = sandbox_arg(id.clone(), tg::sandbox::Status::Started);
	arg.principal = tg::Principal::Root;
	put_sandbox(&index, arg.clone()).await.unwrap();
	arg.processes = Some(Vec::new());
	let data = &mut arg.data.as_mut().unwrap().data;
	data.hostname = Some("updated".into());
	data.status = tg::sandbox::Status::Destroyed;
	put_sandbox(&index, arg.clone()).await.unwrap();
	let stored = index.try_get_sandbox(&id).await.unwrap().unwrap();
	let data = stored.data.unwrap().data;
	assert_eq!(data.status, tg::sandbox::Status::Destroyed);
	assert_eq!(data.hostname.as_deref(), Some("updated"));
	assert!(stored.set.processes);

	// Root must still submit unique processes.
	let process = tg::process::Id::new();
	arg.processes = Some(vec![process.clone(), process]);
	assert!(put_sandbox(&index, arg).await.is_err());
}

#[tokio::test]
async fn process_put_preserves_existing_contents() {
	let (_dir, index) = new_index();
	let id = tg::process::Id::new();
	let mut arg = process_arg(id.clone(), tg::process::Status::Finished);
	arg.children = Some(Vec::new());
	arg.principal = tg::Principal::User(tg::user::Id::new());
	put_process(&index, arg.clone()).await.unwrap();

	// A different command location does not make the process data conflict.
	let mut identical = arg.clone();
	identical.data.as_mut().unwrap().command.options.location =
		Some(tg::Location::Local(tg::location::Local::default()));
	put_process(&index, identical).await.unwrap();

	let mut children = arg.clone();
	children.children = Some(vec![tg::process::data::Child {
		cached: false,
		process: tg::Referent::with_node(tg::process::Id::new()),
	}]);
	assert!(put_process(&index, children).await.is_err());

	let mut changed = arg.clone();
	changed.data.as_mut().unwrap().exit = Some(1);
	assert!(put_process(&index, changed.clone()).await.is_err());
	let stored = index.try_get_process(&id).await.unwrap().unwrap();
	assert_eq!(stored.data.unwrap().exit, Some(0));

	// A regular user cannot change data.log from None to a blob ID.
	let mut with_log = arg.clone();
	with_log.data.as_mut().unwrap().log = Some(tg::Referent::with_node(tg::blob::Id::new(b"log")));
	assert!(put_process(&index, with_log.clone()).await.is_err());
	with_log.principal = tg::Principal::Process(id.clone());
	put_process(&index, with_log).await.unwrap();
	changed.principal = tg::Principal::Process(id.clone());
	put_process(&index, changed).await.unwrap();
	let stored = index.try_get_process(&id).await.unwrap().unwrap();
	assert_eq!(stored.data.unwrap().exit, Some(1));
}

#[tokio::test]
async fn process_log_updates_are_explicit_and_authorized() {
	for root in [false, true] {
		let (_dir, index) = new_index();
		let id = tg::process::Id::new();
		let log = tg::blob::Id::new(b"log");
		let mut arg = process_arg(id.clone(), tg::process::Status::Finished);
		arg.data.as_mut().unwrap().log = Some(tg::Referent::with_node(log.clone()));
		arg.log = Some(Some(log.into()));
		put_process(&index, arg.clone()).await.unwrap();

		// The finished log cannot be removed by an ordinary user.
		arg.data.as_mut().unwrap().log = None;
		arg.log = Some(None);
		arg.principal = tg::Principal::User(tg::user::Id::new());
		assert!(put_process(&index, arg.clone()).await.is_err());
		assert!(
			index
				.try_get_process(&id)
				.await
				.unwrap()
				.unwrap()
				.data
				.unwrap()
				.log
				.is_some()
		);

		// A privileged write replaces the log like any other process field.
		arg.principal = if root {
			tg::Principal::Root
		} else {
			tg::Principal::Process(id.clone())
		};
		put_process(&index, arg).await.unwrap();
		let process = index.try_get_process(&id).await.unwrap().unwrap();
		assert!(process.data.unwrap().log.is_none());
		assert!(process.set.log_objects);
	}
}

#[tokio::test]
async fn sandbox_put_preserves_existing_contents_and_processes() {
	let (_dir, index) = new_index();
	let id = tg::sandbox::Id::new();
	let mut arg = sandbox_arg(id.clone(), tg::sandbox::Status::Destroyed);
	arg.principal = tg::Principal::User(tg::user::Id::new());
	arg.processes = Some(vec![tg::process::Id::new(), tg::process::Id::new()]);
	put_sandbox(&index, arg.clone()).await.unwrap();
	put_sandbox(&index, arg.clone()).await.unwrap();

	let mut changed = arg.clone();
	changed.data.as_mut().unwrap().data.owner = Some(tg::Principal::User(tg::user::Id::new()));
	assert!(put_sandbox(&index, changed.clone()).await.is_err());
	let mut reordered = arg.clone();
	reordered.processes.as_mut().unwrap().reverse();
	assert!(put_sandbox(&index, reordered).await.is_err());
	let stored = index.try_get_sandbox(&id).await.unwrap().unwrap();
	assert!(stored.data.unwrap().data.owner.is_none());
	changed.principal = tg::Principal::Sandbox(id);
	put_sandbox(&index, changed).await.unwrap();
}

#[tokio::test]
async fn concurrent_identical_process_puts_grant_both_callers() {
	let (_dir, index) = new_index();
	let id = tg::process::Id::new();
	let alice = tg::Principal::User(tg::user::Id::new());
	let bob = tg::Principal::User(tg::user::Id::new());
	let permission = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Node,
	);
	let permission_arg = |principal: &tg::Principal| tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(principal.clone()),
		permissions: permission.into(),
		resource: id.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(i64::MAX),
		},
		subject: principal.try_to_subject().unwrap(),
		time_to_touch: None,
		version: None,
	};
	let mut first = process_arg(id.clone(), tg::process::Status::Finished);
	first.children = Some(Vec::new());
	first.permissions = vec![permission_arg(&alice)];
	first.principal = alice.clone();
	let mut second = first.clone();
	second.permissions = vec![permission_arg(&bob)];
	second.principal = bob.clone();

	let (first, second) = tokio::join!(put_process(&index, first), put_process(&index, second));
	first.unwrap();
	second.unwrap();
	let arg = tangram_index::verify::Arg {
		requested: permission.into(),
		required: permission.into(),
		resource: tg::Selector::Id(id.into()),
		storage: tg::storage::Set::Process(tg::process::storage::Set::empty()),
		subject: None,
		tokens: Vec::new(),
	};
	let mut granted = 0;
	for principal in [alice, bob] {
		let output = index
			.verify_batch(
				std::slice::from_ref(&arg),
				tangram_index::verify::Config::default(),
				&principal,
			)
			.await
			.unwrap();
		granted += usize::from(output[0].permissions.contains(permission));
	}
	assert_eq!(granted, 2);
}

#[tokio::test]
async fn identical_sandbox_put_grants_permissions_after_comparison() {
	let (_dir, index) = new_index();
	let id = tg::sandbox::Id::new();
	let mut arg = sandbox_arg(id.clone(), tg::sandbox::Status::Destroyed);
	arg.principal = tg::Principal::Root;
	arg.processes = Some(Vec::new());
	put_sandbox(&index, arg.clone()).await.unwrap();

	let principal = tg::Principal::User(tg::user::Id::new());
	let permission = tg::authorization::Permission::Sandbox(
		tg::authorization::permission::sandbox::Permission::Node,
	);
	arg.permissions = vec![tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(principal.clone()),
		permissions: permission.into(),
		resource: id.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(i64::MAX),
		},
		subject: principal.try_to_subject().unwrap(),
		time_to_touch: None,
		version: None,
	}];
	arg.principal = principal.clone();
	let verify = tangram_index::verify::Arg {
		requested: permission.into(),
		required: permission.into(),
		resource: tg::Selector::Id(id.into()),
		storage: tg::storage::Set::Object(tg::object::storage::Set::empty()),
		subject: None,
		tokens: Vec::new(),
	};
	for complete in [false, true] {
		let mut submission = arg.clone();
		if !complete {
			submission.processes = None;
		}
		put_sandbox(&index, submission).await.unwrap();
		if !complete {
			let mut conflicting = arg.clone();
			conflicting.data.as_mut().unwrap().data.hostname = Some("different".into());
			assert!(put_sandbox(&index, conflicting).await.is_err());
		}
		let output = index
			.verify_batch(
				std::slice::from_ref(&verify),
				tangram_index::verify::Config::default(),
				&principal,
			)
			.await
			.unwrap();
		assert_eq!(output[0].permissions.contains(permission), complete);
	}
}

async fn put_process(index: &Index, arg: tangram_index::process::put::Arg) -> tg::Result<()> {
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(arg)],
	};
	index.batch(arg).await??;
	Ok(())
}

async fn put_sandbox(index: &Index, arg: tangram_index::sandbox::put::Arg) -> tg::Result<()> {
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutSandbox(arg)],
	};
	index.batch(arg).await??;
	Ok(())
}

#[tokio::test]
async fn concurrent_sandbox_put_preserves_the_winner() {
	let (_dir, index) = new_index();
	let id = tg::sandbox::Id::new();
	let mut first = sandbox_arg(id.clone(), tg::sandbox::Status::Destroyed);
	first.principal = tg::Principal::User(tg::user::Id::new());
	first.processes = Some(Vec::new());
	let mut second = first.clone();
	first.data.as_mut().unwrap().data.hostname = Some("first".into());
	second.data.as_mut().unwrap().data.hostname = Some("second".into());
	let (first, second) = tokio::join!(put_sandbox(&index, first), put_sandbox(&index, second));
	assert_ne!(first.is_ok(), second.is_ok());
	let stored = index.try_get_sandbox(&id).await.unwrap().unwrap();
	assert_eq!(
		stored.data.unwrap().data.hostname.as_deref(),
		Some(if first.is_ok() { "first" } else { "second" })
	);
}

fn execute_write_requests(
	index: &Index,
	requests: Vec<crate::Request>,
	write_operation_batch_size: usize,
) -> Vec<tg::Result<crate::Response>> {
	let (sender_high, receiver_high) = crossbeam_channel::unbounded();
	let (sender_medium, receiver_medium) = crossbeam_channel::unbounded();
	let (sender_low, receiver_low) = crossbeam_channel::unbounded();
	let mut receivers = Vec::new();
	for request in requests {
		let (sender, receiver) = tokio::sync::oneshot::channel();
		sender_medium.send((request, sender)).unwrap();
		receivers.push(receiver);
	}
	drop(sender_high);
	drop(sender_medium);
	drop(sender_low);
	let arg = crate::writer::Arg {
		db: &index.db,
		env: &index.env,
		max_process_depth: None,
		receiver_high: &receiver_high,
		receiver_low: &receiver_low,
		receiver_medium: &receiver_medium,
		subspace: &index.subspace,
		usage_partition_total: index.usage_partition_total(),
		write_operation_batch_size,
	};
	Index::writer_task(arg);
	receivers
		.iter_mut()
		.map(|receiver| receiver.try_recv().unwrap())
		.collect()
}

#[tokio::test]
async fn process_rejection_commits_unrelated_requests_and_prior_items() {
	let (_dir, index) = new_index();
	let existing = tg::process::Id::new();
	put_process(
		&index,
		process_arg(existing.clone(), tg::process::Status::Finished),
	)
	.await
	.unwrap();
	let before = tg::process::Id::new();
	let after = tg::process::Id::new();
	let unrelated = tg::process::Id::new();
	let mut conflicting = process_arg(existing.clone(), tg::process::Status::Finished);
	conflicting.principal = tg::Principal::User(tg::user::Id::new());
	conflicting.data.as_mut().unwrap().host = "conflict".into();
	let batch = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutProcess(process_arg(
				before.clone(),
				tg::process::Status::Finished,
			)),
			tangram_index::batch::Item::PutProcess(conflicting),
			tangram_index::batch::Item::PutProcess(process_arg(
				after.clone(),
				tg::process::Status::Finished,
			)),
		],
	};
	let requests = vec![
		crate::Request::Batch(batch),
		crate::Request::PutProcesses(vec![process_arg(
			unrelated.clone(),
			tg::process::Status::Finished,
		)]),
	];
	let results = execute_write_requests(&index, requests, 64);
	assert!(matches!(&results[0], Ok(crate::Response::Mutation(Err(_)))));
	assert!(matches!(&results[1], Ok(crate::Response::Mutation(Ok(())))));
	assert!(index.try_get_process(&before).await.unwrap().is_some());
	assert!(index.try_get_process(&after).await.unwrap().is_none());
	assert!(index.try_get_process(&unrelated).await.unwrap().is_some());
	assert_eq!(
		index
			.try_get_process(&existing)
			.await
			.unwrap()
			.unwrap()
			.data
			.unwrap()
			.host,
		""
	);
}

#[tokio::test]
async fn process_rejection_skips_remaining_bulk_writes() {
	let (_dir, index) = new_index();
	let before = tg::process::Id::new();
	let rejected = tg::process::Id::new();
	let after = tg::process::Id::new();
	let unrelated = tg::process::Id::new();
	let mut unfinished = process_arg(rejected.clone(), tg::process::Status::Started);
	unfinished.principal = tg::Principal::User(tg::user::Id::new());
	let requests = vec![
		crate::Request::PutProcesses(vec![
			process_arg(before.clone(), tg::process::Status::Finished),
			unfinished,
			process_arg(after.clone(), tg::process::Status::Finished),
		]),
		crate::Request::PutProcesses(vec![process_arg(
			unrelated.clone(),
			tg::process::Status::Finished,
		)]),
	];
	let results = execute_write_requests(&index, requests, 1);
	assert!(matches!(&results[0], Ok(crate::Response::Mutation(Err(_)))));
	assert!(matches!(&results[1], Ok(crate::Response::Mutation(Ok(())))));
	assert!(index.try_get_process(&before).await.unwrap().is_some());
	assert!(index.try_get_process(&rejected).await.unwrap().is_none());
	assert!(index.try_get_process(&after).await.unwrap().is_none());
	assert!(index.try_get_process(&unrelated).await.unwrap().is_some());
}

#[tokio::test]
async fn unexpected_write_failure_aborts_all_coalesced_requests() {
	let (_dir, index) = new_index();
	let corrupt = tg::process::Id::new();
	let key = crate::Key::Process(crate::process::Key::Process(corrupt.clone()));
	let key = Index::pack(&index.subspace, &key);
	let mut transaction = index.env.write_txn().unwrap();
	index.db.put(&mut transaction, &key, &[255]).unwrap();
	transaction.commit().unwrap();
	let before = tg::process::Id::new();
	let after = tg::process::Id::new();
	let requests = [before.clone(), corrupt, after.clone()]
		.into_iter()
		.map(|id| {
			crate::Request::PutProcesses(vec![process_arg(id, tg::process::Status::Finished)])
		})
		.collect();
	let results = execute_write_requests(&index, requests, 64);
	assert!(results.iter().all(Result::is_err));
	assert!(index.try_get_process(&before).await.unwrap().is_none());
	assert!(index.try_get_process(&after).await.unwrap().is_none());
}

#[tokio::test]
async fn sandbox_rejection_preserves_permissions_and_unrelated_writes() {
	let (_dir, index) = new_index();
	let existing = tg::sandbox::Id::new();
	let mut original = sandbox_arg(existing.clone(), tg::sandbox::Status::Destroyed);
	original.principal = tg::Principal::Root;
	original.processes = Some(Vec::new());
	put_sandbox(&index, original.clone()).await.unwrap();
	let principal = tg::Principal::User(tg::user::Id::new());
	let permission = tg::authorization::Permission::Sandbox(
		tg::authorization::permission::sandbox::Permission::Node,
	);
	let mut conflicting = original;
	conflicting.principal = principal.clone();
	conflicting.data.as_mut().unwrap().data.hostname = Some("conflict".into());
	conflicting.permissions = vec![tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(principal.clone()),
		permissions: permission.into(),
		resource: existing.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(i64::MAX),
		},
		subject: principal.try_to_subject().unwrap(),
		time_to_touch: None,
		version: None,
	}];
	let unrelated = tg::sandbox::Id::new();
	let requests = vec![
		crate::Request::PutSandboxes(vec![conflicting]),
		crate::Request::PutSandboxes(vec![sandbox_arg(
			unrelated.clone(),
			tg::sandbox::Status::Destroyed,
		)]),
	];
	let results = execute_write_requests(&index, requests, 64);
	assert!(matches!(&results[0], Ok(crate::Response::Mutation(Err(_)))));
	assert!(matches!(&results[1], Ok(crate::Response::Mutation(Ok(())))));
	assert!(index.try_get_sandbox(&unrelated).await.unwrap().is_some());
	assert_ne!(
		index
			.try_get_sandbox(&existing)
			.await
			.unwrap()
			.unwrap()
			.data
			.unwrap()
			.data
			.hostname
			.as_deref(),
		Some("conflict")
	);
	let arg = tangram_index::verify::Arg {
		requested: permission.into(),
		required: permission.into(),
		resource: tg::Selector::Id(existing.into()),
		storage: tg::storage::Set::Object(tg::object::storage::Set::empty()),
		subject: None,
		tokens: Vec::new(),
	};
	let output = index
		.verify_batch(
			std::slice::from_ref(&arg),
			tangram_index::verify::Config::default(),
			&principal,
		)
		.await
		.unwrap();
	assert!(!output[0].permissions.contains(permission));
}
