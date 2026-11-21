use {
	super::super::{Index, Kind},
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

fn process_arg(id: &tg::process::Id, data: tg::process::Data) -> tangram_index::process::put::Arg {
	let command = tg::command::Id::new(b"command");
	tangram_index::process::put::Arg {
		account: None,
		boot: None,
		cached: false,
		captures: tangram_index::permission::capture::Captures::default(),
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
		principal: tg::Principal::Process(id.clone()),
		registration: None,
		sandbox: None,
		sandbox_boot: None,
		sequence: 0,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	}
}

fn process_data(status: tg::process::Status, host: String) -> tg::process::Data {
	let finished = status.is_finished();
	tg::process::Data {
		cacheable: false,
		checksum: tg::process::Checksum::default(),
		children: None,
		command: tg::Referent::with_node(tg::Either::Right(tg::command::Id::new(b"command"))),
		created_at: 0,
		creator: tg::Principal::Root,
		debug: None,
		error: None,
		exit: finished.then_some(0),
		finished_at: finished.then_some(0),
		host,
		log: None,
		output: None,
		owner: tg::Principal::Root,
		retry: false,
		sandbox: None,
		started_at: Some(0),
		status,
		stderr: tg::process::Stdio::default(),
		stdin: tg::process::Stdio::default(),
		stdout: tg::process::Stdio::default(),
		tty: None,
	}
}

fn count_process_data_fragments(index: &Index, id: &tg::process::Id) -> usize {
	let transaction = index.env.read_txn().unwrap();
	let id_bytes = id.to_bytes();
	let prefix = Index::pack(
		&index.subspace,
		&(Kind::ProcessData.to_i32().unwrap(), id_bytes.as_ref()),
	);
	index
		.db
		.prefix_iter(&transaction, &prefix)
		.unwrap()
		.map(Result::unwrap)
		.count()
}

fn count_principal_keys(index: &Index, kind: Kind, principal: &tg::Principal) -> usize {
	let transaction = index.env.read_txn().unwrap();
	let prefix = Index::pack(
		&index.subspace,
		&(kind.to_i32().unwrap(), principal.to_string()),
	);
	index
		.db
		.prefix_iter(&transaction, &prefix)
		.unwrap()
		.map(Result::unwrap)
		.count()
}

async fn put_process(index: &Index, arg: tangram_index::process::put::Arg) {
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutProcess(Box::new(arg))],
	};
	index.write(arg).await.unwrap().unwrap();
}

#[tokio::test]
async fn rewriting_process_data_replaces_its_fragments() {
	let (_dir, index) = super::new_index();
	let id = tg::process::Id::new();

	// Put started process data that spans many fragments.
	let data = process_data(tg::process::Status::Started, "host".repeat(100));
	put_process(&index, process_arg(&id, data)).await;
	let started = count_process_data_fragments(&index, &id);

	// Finish the process with shorter data.
	let data = process_data(tg::process::Status::Finished, String::new());
	let bytes = tangram_index::process::data::serialize(&data).unwrap();
	put_process(&index, process_arg(&id, data.clone())).await;
	let finished = count_process_data_fragments(&index, &id);

	// Verify that the stale fragments were removed and that the record and data agree.
	assert!(finished < started);
	assert_eq!(finished, bytes.len().div_ceil(16));
	let (process, stored) = index.try_get_process_with_data(&id).await.unwrap().unwrap();
	assert_eq!(process.status, Some(tg::process::Status::Finished));
	assert_eq!(
		tangram_index::process::data::serialize(&stored.unwrap()).unwrap(),
		bytes
	);
}

#[tokio::test]
async fn cleaning_a_process_deletes_its_data() {
	let (_dir, index) = super::new_index();
	let id = tg::process::Id::new();
	let data = process_data(tg::process::Status::Finished, "host".repeat(100));
	put_process(&index, process_arg(&id, data)).await;
	assert!(count_process_data_fragments(&index, &id) > 1);

	// Clean every process.
	let arg = tangram_index::clean::Arg {
		batch_size: 100,
		max_object_touched_at: i64::MAX,
		max_process_touched_at: i64::MAX,
		max_sandbox_touched_at: i64::MAX,
		now: i64::MAX,
		partition_end: 1,
		partition_start: 0,
	};
	index.clean(arg).await.unwrap();

	// Verify that the process and its data are gone.
	assert!(index.try_get_process(&id).await.unwrap().is_none());
	assert_eq!(count_process_data_fragments(&index, &id), 0);
}

#[tokio::test]
async fn putting_a_process_without_data_keeps_its_cache_entry() {
	let (_dir, index) = super::new_index();
	let id = tg::process::Id::new();
	let command: tg::object::Id = tg::command::Id::new(b"command").into();
	let mut data = process_data(tg::process::Status::Finished, String::new());
	data.cacheable = true;
	put_process(&index, process_arg(&id, data.clone())).await;
	assert_eq!(
		index
			.try_get_cached_processes(&command)
			.await
			.unwrap()
			.len(),
		1
	);

	// Put the finished process again as another principal, which preserves the stored data.
	let mut arg = process_arg(&id, data);
	arg.principal = tg::Principal::Root;
	put_process(&index, arg).await;

	// Verify that the process is still a cache candidate.
	let cached = index.try_get_cached_processes(&command).await.unwrap();
	assert_eq!(cached.len(), 1);
	assert_eq!(cached[0].0, id);
}

#[tokio::test]
async fn touching_a_process_with_data_returns_its_data() {
	let (_dir, index) = super::new_index();
	let id = tg::process::Id::new();
	let missing = tg::process::Id::new();
	let data = process_data(tg::process::Status::Finished, "host".repeat(100));
	let bytes = tangram_index::process::data::serialize(&data).unwrap();
	put_process(&index, process_arg(&id, data)).await;

	// Touch the process and a missing process with data.
	let output = index
		.touch_processes_with_data_with_account(
			&[id.clone(), missing],
			None,
			1,
			std::time::Duration::ZERO,
		)
		.await
		.unwrap();

	// Verify that the touched process is returned with its data and the missing process is absent.
	let (process, stored) = output[0].clone().unwrap();
	assert_eq!(process.touched_at, 1);
	assert_eq!(
		tangram_index::process::data::serialize(&stored.unwrap()).unwrap(),
		bytes
	);
	assert!(output[1].is_none());
}

#[test]
fn a_zero_process_data_fragment_size_is_rejected() {
	let dir = tempfile::TempDir::new().unwrap();
	let config = crate::Config {
		map_size: 1 << 30,
		max_process_depth: None,
		max_readers: 1_000,
		object_permission_time_to_touch: std::time::Duration::ZERO,
		path: dir.path().join("index"),
		posix_sem_prefix: None,
		process_data_fragment_size: 0,
		process_permission_time_to_touch: std::time::Duration::ZERO,
		read_channel_capacity: 256,
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total: 1,
		write_channel_capacity: 256,
		write_operation_batch_size: 1,
	};
	assert!(Index::new(&config).is_err());
}

fn creator_permission_arg(id: &tg::process::Id) -> tangram_index::permission::put::Arg {
	let permission = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Parent,
	);
	tangram_index::permission::put::Arg {
		created_at: 0,
		permissions: permission.into(),
		resource: id.clone().into(),
		source: tangram_index::permission::Source::Direct {
			expires_at: Some(i64::MAX),
		},
		subject: tg::authorization::Subject::Public,
		time_to_touch: None,
		version: None,
	}
}

#[tokio::test]
async fn a_queued_record_that_arrives_after_the_process_started_puts_its_permissions() {
	let (_dir, index) = super::new_index();
	let id = tg::process::Id::new();

	// Start the process.
	let data = process_data(tg::process::Status::Started, String::new());
	put_process(&index, process_arg(&id, data)).await;

	// Put a queued record that another principal rejects, and verify that it puts no permissions.
	let data = process_data(tg::process::Status::Queued, String::new());
	let mut arg = process_arg(&id, data.clone());
	arg.permissions = vec![creator_permission_arg(&id)];
	arg.principal = tg::Principal::Root;
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutProcess(Box::new(arg))],
	};
	assert!(index.write(arg).await.unwrap().is_err());
	let permission = tangram_index::permission::get::Arg {
		resource: id.clone().into(),
		subject: tg::authorization::Subject::Public,
	};
	let permissions = index
		.get_permissions(std::slice::from_ref(&permission))
		.await
		.unwrap();
	assert_eq!(permissions[0], Vec::new());

	// Put the queued record with the creator permission.
	let mut arg = process_arg(&id, data);
	arg.permissions = vec![creator_permission_arg(&id)];
	put_process(&index, arg).await;

	// Verify that the process is still started and that the creator permission was put.
	let process = index.try_get_process(&id).await.unwrap().unwrap();
	assert_eq!(process.status, Some(tg::process::Status::Started));
	let permissions = index
		.get_permissions(std::slice::from_ref(&permission))
		.await
		.unwrap();
	assert_eq!(
		permissions[0],
		vec![tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent
		)]
	);
}

fn count_delegations(index: &Index, subject: &tg::authorization::Subject) -> usize {
	let transaction = index.env.read_txn().unwrap();
	let prefix = Index::pack(
		&index.subspace,
		&(
			Kind::DelegationSubject.to_i32().unwrap(),
			subject.to_string(),
		),
	);
	index
		.db
		.prefix_iter(&transaction, &prefix)
		.unwrap()
		.map(Result::unwrap)
		.count()
}

#[tokio::test]
async fn a_finished_process_accepts_only_the_children_that_it_lists() {
	let (_dir, index) = super::new_index();
	let parent = tg::process::Id::new();
	let listed = tg::process::Id::new();
	let unlisted = tg::process::Id::new();

	// Finish the parent with a list of children that includes only the listed child.
	let mut arg = process_arg(
		&parent,
		process_data(tg::process::Status::Finished, String::new()),
	);
	arg.children = Some(vec![tg::process::data::Child {
		cached: false,
		process: tg::Referent::with_node(listed.clone()),
	}]);
	put_process(&index, arg).await;

	// Start both children with the parent, and delegate the parent's authority to each.
	for child in [&listed, &unlisted] {
		let mut arg = process_arg(
			child,
			process_data(tg::process::Status::Started, String::new()),
		);
		arg.parent = Some(parent.clone());
		let delegation = tangram_index::delegation::put::Arg {
			expires_at: i64::MAX,
			resource: tg::command::Id::new(b"command").into(),
			source: tg::authorization::Subject::Process(parent.clone()),
			subject: tg::authorization::Subject::Process(child.clone()),
			version: None,
		};
		let arg = tangram_index::write::Arg {
			items: vec![
				tangram_index::Item::PutProcess(Box::new(arg)),
				tangram_index::Item::PutDelegation(Box::new(delegation)),
			],
		};
		index.write(arg).await.unwrap().unwrap();
	}

	// Verify that the parent lists only the listed child, and that only the listed child received the parent's authority.
	let children = index
		.try_get_process_children(&parent, std::io::SeekFrom::Start(0), 10)
		.await
		.unwrap()
		.unwrap();
	let children = children
		.into_iter()
		.map(|child| child.process.node)
		.collect::<Vec<_>>();
	assert_eq!(children, vec![listed.clone()]);
	let subject = tg::authorization::Subject::Process(listed);
	assert_eq!(count_delegations(&index, &subject), 1);
	let subject = tg::authorization::Subject::Process(unlisted.clone());
	assert_eq!(count_delegations(&index, &subject), 0);
	assert!(index.try_get_process(&unlisted).await.unwrap().is_some());
}

#[tokio::test]
async fn processes_are_listed_by_creator_and_owner_and_status() {
	let (_dir, index) = super::new_index();
	let id = tg::process::Id::new();
	let creator = tg::Principal::User(tg::user::Id::new());
	let owner = tg::Principal::Organization(tg::organization::Id::new());
	let started = Some(tg::process::Status::Started);
	let finished = Some(tg::process::Status::Finished);

	// Put a started process.
	let mut data = process_data(tg::process::Status::Started, String::new());
	data.creator = creator.clone();
	data.owner = owner.clone();
	put_process(&index, process_arg(&id, data.clone())).await;

	// Verify that the process is listed as started by its creator and its owner.
	let arg = |principal: &tg::Principal, status| tangram_index::process::list::Arg {
		after: None,
		limit: 10,
		principal: principal.clone(),
		status,
	};
	let ids = |output: tangram_index::process::list::Output| {
		output
			.data
			.into_iter()
			.map(|(id, _, _)| id)
			.collect::<Vec<_>>()
	};
	let output = index
		.list_processes_for_creator(arg(&creator, started))
		.await
		.unwrap();
	assert_eq!(ids(output), vec![id.clone()]);
	let output = index
		.list_processes_for_owner(arg(&owner, started))
		.await
		.unwrap();
	assert_eq!(ids(output), vec![id.clone()]);
	let output = index
		.list_processes_for_owner(arg(&owner, finished))
		.await
		.unwrap();
	assert!(output.data.is_empty());

	// Finish the process.
	let mut data = process_data(tg::process::Status::Finished, String::new());
	data.creator = creator.clone();
	data.owner = owner.clone();
	put_process(&index, process_arg(&id, data)).await;

	// Verify that the process moved from started to finished.
	let output = index
		.list_processes_for_creator(arg(&creator, started))
		.await
		.unwrap();
	assert!(output.data.is_empty());
	let output = index
		.list_processes_for_creator(arg(&creator, finished))
		.await
		.unwrap();
	assert_eq!(ids(output), vec![id.clone()]);
	let output = index
		.list_processes_for_owner(arg(&owner, None))
		.await
		.unwrap();
	assert_eq!(ids(output), vec![id.clone()]);
	assert_eq!(
		count_principal_keys(&index, Kind::CreatorProcess, &creator),
		1
	);
	assert_eq!(count_principal_keys(&index, Kind::OwnerProcess, &owner), 1);

	// Clean every process.
	let arg = tangram_index::clean::Arg {
		batch_size: 100,
		max_object_touched_at: i64::MAX,
		max_process_touched_at: i64::MAX,
		max_sandbox_touched_at: i64::MAX,
		now: i64::MAX,
		partition_end: 1,
		partition_start: 0,
	};
	index.clean(arg).await.unwrap();

	// Verify that the cleaned process has no creator or owner keys.
	assert_eq!(
		count_principal_keys(&index, Kind::CreatorProcess, &creator),
		0
	);
	assert_eq!(count_principal_keys(&index, Kind::OwnerProcess, &owner), 0);
}

#[tokio::test]
async fn processes_are_listed_by_page() {
	let (_dir, index) = super::new_index();
	let creator = tg::Principal::User(tg::user::Id::new());

	// Put three started processes.
	let mut ids = Vec::new();
	for _ in 0..3 {
		let id = tg::process::Id::new();
		let mut data = process_data(tg::process::Status::Started, String::new());
		data.creator = creator.clone();
		put_process(&index, process_arg(&id, data)).await;
		ids.push(id);
	}

	// Verify that the first page lists the first two processes and continues after the second.
	let arg = tangram_index::process::list::Arg {
		after: None,
		limit: 2,
		principal: creator.clone(),
		status: Some(tg::process::Status::Started),
	};
	let output = index.list_processes_for_creator(arg).await.unwrap();
	let page = output
		.data
		.into_iter()
		.map(|(id, _, _)| id)
		.collect::<Vec<_>>();
	assert_eq!(page, ids[..2]);
	let next = output.next.unwrap();
	assert_eq!(next.process, ids[1]);

	// Verify that the second page lists the last process and does not continue.
	let arg = tangram_index::process::list::Arg {
		after: Some(next),
		limit: 2,
		principal: creator.clone(),
		status: Some(tg::process::Status::Started),
	};
	let output = index.list_processes_for_creator(arg).await.unwrap();
	let page = output
		.data
		.into_iter()
		.map(|(id, _, _)| id)
		.collect::<Vec<_>>();
	assert_eq!(page, ids[2..]);
	assert!(output.next.is_none());
}

#[tokio::test]
async fn a_write_cannot_change_the_creator_or_the_owner_of_a_process() {
	let (_dir, index) = super::new_index();
	let id = tg::process::Id::new();
	let user = tg::Principal::Process(tg::process::Id::new());

	// Start the process.
	let data = process_data(tg::process::Status::Started, String::new());
	put_process(&index, process_arg(&id, data.clone())).await;

	// Put the process's own data with another creator, and verify that the write is rejected.
	let mut changed = data.clone();
	changed.creator = user.clone();
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutProcess(Box::new(process_arg(
			&id, changed,
		)))],
	};
	assert!(index.write(arg).await.unwrap().is_err());

	// Put the process's own data with another owner, and verify that the write is rejected.
	let mut changed = data.clone();
	changed.owner = user;
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutProcess(Box::new(process_arg(
			&id, changed,
		)))],
	};
	assert!(index.write(arg).await.unwrap().is_err());

	// Put the process's own data with the same creator and owner, and verify that they are unchanged.
	put_process(&index, process_arg(&id, data)).await;
	let process = index.try_get_process(&id).await.unwrap().unwrap();
	assert_eq!(process.creator, tg::Principal::Root);
	assert_eq!(process.owner, tg::Principal::Root);
}

#[tokio::test]
async fn a_write_cannot_change_the_sandbox_of_a_process() {
	let (_dir, index) = super::new_index();
	let id = tg::process::Id::new();
	let sandbox = tg::sandbox::Id::new();

	// Queue the process in a sandbox.
	let mut data = process_data(tg::process::Status::Queued, String::new());
	data.sandbox = Some(sandbox.clone());
	put_process(&index, process_arg(&id, data.clone())).await;

	// Start the process in another sandbox, and verify that the write is rejected.
	let mut changed = data.clone();
	changed.sandbox = Some(tg::sandbox::Id::new());
	changed.status = tg::process::Status::Started;
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutProcess(Box::new(process_arg(
			&id, changed,
		)))],
	};
	assert!(index.write(arg).await.unwrap().is_err());

	// Start the process in its sandbox, and verify that the write is accepted.
	data.status = tg::process::Status::Started;
	put_process(&index, process_arg(&id, data)).await;
	let process = index.try_get_process(&id).await.unwrap().unwrap();
	assert_eq!(process.status, Some(tg::process::Status::Started));
	assert_eq!(process.sandbox, Some(sandbox));
}

fn sandbox_put_arg(
	id: &tg::sandbox::Id,
	boot: u64,
	status: tg::sandbox::Status,
) -> tangram_index::sandbox::put::Arg {
	let data = tg::sandbox::get::Output {
		data: tg::sandbox::Data {
			boot,
			cpu: None,
			creator: tg::Principal::Root,
			error: None,
			expires_at: None,
			hostname: None,
			id: id.clone(),
			memory: None,
			mounts: Vec::new(),
			network: None,
			owner: tg::Principal::Root,
			status,
			stopped_at: status.is_stopped().then_some(1),
			storage: None,
			time_to_live: None,
			time_to_stop: None,
			usage: None,
		},
		location: None,
		tokens: tg::authorization::Tokens::default(),
	};
	tangram_index::sandbox::put::Arg {
		account: None,
		created_at: 1,
		data: Some(data),
		id: id.clone(),
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Sandbox(id.clone()),
		process: None,
		processes: None,
		registration: None,
		runner: None,
		sequence: 0,
		stopped_boot: None,
		stopped_usage: None,
		touched_at: 1,
	}
}

/// Create the arg of a finish of a queued process in a sandbox that applies only while the sandbox is at the boot.
fn conditional_finish_arg(
	id: &tg::process::Id,
	sandbox: &tg::sandbox::Id,
	boot: u64,
) -> tangram_index::process::put::Arg {
	let mut data = process_data(tg::process::Status::Finished, String::new());
	data.exit = Some(1);
	data.sandbox = Some(sandbox.clone());
	data.started_at = None;
	let mut arg = process_arg(id, data);
	arg.sandbox_boot = Some(tangram_index::process::put::SandboxBoot {
		boot,
		sandbox: sandbox.clone(),
	});
	arg
}

/// Create the arg of a queued process in a sandbox.
fn queued_process_arg(
	id: &tg::process::Id,
	sandbox: &tg::sandbox::Id,
) -> tangram_index::process::put::Arg {
	let mut data = process_data(tg::process::Status::Queued, String::new());
	data.sandbox = Some(sandbox.clone());
	data.started_at = None;
	process_arg(id, data)
}

async fn process_status(index: &Index, id: &tg::process::Id) -> Option<tg::process::Status> {
	index.try_get_process(id).await.unwrap().unwrap().status
}

async fn write_items(index: &Index, items: Vec<tangram_index::Item>) -> tg::Result<()> {
	let arg = tangram_index::write::Arg { items };
	index.write(arg).await.unwrap()
}

#[tokio::test]
async fn a_conditional_finish_of_a_queued_process_applies_while_the_sandbox_is_at_the_boot() {
	let (_dir, index) = super::new_index();
	let index = &index;
	for status in [
		tg::sandbox::Status::Started,
		tg::sandbox::Status::Stopped,
		tg::sandbox::Status::Destroyed,
	] {
		// Queue a process in a sandbox at boot 1.
		let sandbox = tg::sandbox::Id::new();
		let id = tg::process::Id::new();
		let item = tangram_index::Item::PutSandbox(Box::new(sandbox_put_arg(&sandbox, 1, status)));
		write_items(index, vec![item]).await.unwrap();
		put_process(index, queued_process_arg(&id, &sandbox)).await;

		// Finish the process on the condition that the sandbox is at boot 1, and verify that the finish is applied.
		let item =
			tangram_index::Item::PutProcess(Box::new(conditional_finish_arg(&id, &sandbox, 1)));
		write_items(index, vec![item]).await.unwrap();
		assert_eq!(
			process_status(index, &id).await,
			Some(tg::process::Status::Finished)
		);
	}
}

#[tokio::test]
async fn a_conditional_finish_of_a_queued_process_does_not_apply_at_a_later_boot() {
	let (_dir, index) = super::new_index();
	let index = &index;
	// Queue a process in a sandbox that is starting its second boot.
	let sandbox = tg::sandbox::Id::new();
	let id = tg::process::Id::new();
	let item = tangram_index::Item::PutSandbox(Box::new(sandbox_put_arg(
		&sandbox,
		2,
		tg::sandbox::Status::Starting,
	)));
	write_items(index, vec![item]).await.unwrap();
	put_process(index, queued_process_arg(&id, &sandbox)).await;

	// Finish the process on the condition that the sandbox is at boot 1 in the same write as another process, and verify that the finish is rejected and the other process is put.
	let other = tg::process::Id::new();
	let items = vec![
		tangram_index::Item::PutProcess(Box::new(conditional_finish_arg(&id, &sandbox, 1))),
		tangram_index::Item::PutProcess(Box::new(queued_process_arg(&other, &sandbox))),
	];
	assert!(write_items(index, items).await.is_err());
	assert_eq!(
		process_status(index, &id).await,
		Some(tg::process::Status::Queued)
	);
	assert_eq!(
		process_status(index, &other).await,
		Some(tg::process::Status::Queued)
	);
}
