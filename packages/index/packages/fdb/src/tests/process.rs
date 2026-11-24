use {
	super::super::{Index, Kind},
	foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

/// Get the options for an index in the cloud databases' cluster, or `None` if the cloud databases are not available.
pub(super) fn options() -> Option<crate::Options> {
	let cluster = tangram_tests_util::database::cluster()?;
	let options = options_with_cluster(cluster);
	Some(options)
}

fn options_with_cluster(cluster: std::path::PathBuf) -> crate::Options {
	crate::Options {
		cleaning_partition_total: 1,
		cluster,
		instance: Some(format!(
			"index_process_test_{:032x}/",
			rand::random::<u128>()
		)),
		max_process_depth: None,
		max_write_operation_batch_size: 1024,
		object_permission_time_to_touch: std::time::Duration::ZERO,
		permission_update_partition_total: 1,
		process_data_fragment_size: 16,
		process_permission_time_to_touch: std::time::Duration::ZERO,
		read_channel_capacity: 256,
		read_request_batch_size: 4,
		read_task_concurrency: 4,
		read_task_count: 1,
		storage_and_metadata_update_partition_total: 1,
		storage_relation_batch_size: 8,
		usage_partition_total: 1,
		usage_update_partition_total: 1,
		verification: crate::VerificationConfig { concurrency: 1 },
		write_item_transaction_concurrency: 1,
		write_operation_batch_size: 1024,
		write_transaction_concurrency: 1,
	}
}

fn process_arg(id: &tg::process::Id, data: tg::process::Data) -> tangram_index::process::put::Arg {
	// Give unfinished data a boot, which the index requires of every unfinished process.
	let boot = (!data.status.is_finished()).then_some(0);
	let command = tg::command::Id::new(b"command");
	tangram_index::process::put::Arg {
		account: None,
		boot,
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
		sandbox: None,
		sequence: 0,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	}
}

/// The sandbox of the processes that the tests write.
fn sandbox_id() -> tg::sandbox::Id {
	"sbx_0006gwnhtrsdvah1bjsskmh8bczm".parse().unwrap()
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
		sandbox: Some(sandbox_id()),
		started_at: Some(0),
		status,
		stderr: tg::process::Stdio::default(),
		stdin: tg::process::Stdio::default(),
		stdout: tg::process::Stdio::default(),
		tty: None,
	}
}

async fn count_process_data_fragments(index: &Index, id: &tg::process::Id) -> usize {
	crate::run(&index.database, async |txn| {
		let id_bytes = id.to_bytes();
		let prefix = Index::pack(
			&index.subspace,
			&(Kind::ProcessData.to_i32().unwrap(), id_bytes.as_ref()),
		);
		let range = foundationdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix));
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		Ok(ControlFlow::Break(entries.len()))
	})
	.await
	.unwrap()
}

async fn put_process(index: &Index, arg: tangram_index::process::put::Arg) {
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutProcess(Box::new(arg))],
	};
	index.write(arg).await.unwrap().unwrap();
}

#[tokio::test]
async fn rewriting_process_data_replaces_its_fragments() {
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
		let id = tg::process::Id::new();

		// Put started process data that spans many fragments.
		let data = process_data(tg::process::Status::Started, "host".repeat(100));
		put_process(index, process_arg(&id, data)).await;
		let started = count_process_data_fragments(index, &id).await;

		// Finish the process with shorter data.
		let data = process_data(tg::process::Status::Finished, String::new());
		let bytes = tangram_index::process::data::serialize(&data).unwrap();
		put_process(index, process_arg(&id, data)).await;
		let finished = count_process_data_fragments(index, &id).await;

		// Verify that the stale fragments were removed and that the record and data agree.
		assert!(finished < started);
		assert_eq!(finished, bytes.len().div_ceil(16));
		let (process, stored) = index
			.try_get_processes_with_data(std::slice::from_ref(&id))
			.await
			.unwrap()
			.pop()
			.unwrap()
			.unwrap();
		assert_eq!(process.status, Some(tg::process::Status::Finished));
		assert_eq!(
			tangram_index::process::data::serialize(&stored.unwrap()).unwrap(),
			bytes
		);
	})
	.await;
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
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
		let id = tg::process::Id::new();

		// Start the process.
		let data = process_data(tg::process::Status::Started, String::new());
		put_process(index, process_arg(&id, data)).await;

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
		put_process(index, arg).await;

		// Verify that the process is still started and that the creator permission was put.
		let process = index
			.try_get_processes(std::slice::from_ref(&id))
			.await
			.unwrap()
			.pop()
			.unwrap()
			.unwrap();
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
	})
	.await;
}

#[tokio::test]
async fn cleaning_a_process_deletes_its_data() {
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
		let id = tg::process::Id::new();
		let data = process_data(tg::process::Status::Finished, "host".repeat(100));
		put_process(index, process_arg(&id, data)).await;
		assert!(count_process_data_fragments(index, &id).await > 1);

		// Clean until the process is deleted.
		for _ in 0..100 {
			let arg = tangram_index::clean::Arg {
				batch_size: 100,
				max_object_touched_at: i64::MAX,
				max_process_touched_at: i64::MAX,
				max_sandbox_touched_at: i64::MAX,
				now: i64::MAX,
				partition_end: 1,
				partition_start: 0,
			};
			let output = index.clean(arg).await.unwrap();
			if output.done {
				break;
			}
		}

		// Verify that the process and its data are gone.
		let processes = index
			.try_get_processes(std::slice::from_ref(&id))
			.await
			.unwrap();
		assert!(processes[0].is_none());
		assert_eq!(count_process_data_fragments(index, &id).await, 0);
	})
	.await;
}

#[tokio::test]
async fn putting_a_process_without_data_keeps_its_cache_entry() {
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
		let id = tg::process::Id::new();
		let command: tg::object::Id = tg::command::Id::new(b"command").into();
		let mut data = process_data(tg::process::Status::Finished, String::new());
		data.cacheable = true;
		put_process(index, process_arg(&id, data.clone())).await;
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
		put_process(index, arg).await;

		// Verify that the process is still a cache candidate.
		let cached = index.try_get_cached_processes(&command).await.unwrap();
		assert_eq!(cached.len(), 1);
		assert_eq!(cached[0].0, id);
	})
	.await;
}

#[tokio::test]
async fn touching_a_process_with_data_returns_its_data() {
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
		let id = tg::process::Id::new();
		let missing = tg::process::Id::new();
		let data = process_data(tg::process::Status::Finished, "host".repeat(100));
		let bytes = tangram_index::process::data::serialize(&data).unwrap();
		put_process(index, process_arg(&id, data)).await;

		// Touch the process and a missing process with data.
		let output = index
			.touch_processes_with_data_with_account(
				&[id.clone(), missing],
				None,
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
	})
	.await;
}

#[test]
fn a_zero_process_data_fragment_size_is_rejected() {
	let options = crate::Options {
		process_data_fragment_size: 0,
		..options_with_cluster("fdb.cluster".into())
	};
	assert!(Index::validate_options(&options).is_err());
}

async fn count_delegations(index: &Index, subject: &tg::authorization::Subject) -> usize {
	crate::run(&index.database, async |txn| {
		let prefix = Index::pack(
			&index.subspace,
			&(
				Kind::DelegationSubject.to_i32().unwrap(),
				subject.to_string(),
			),
		);
		let range = foundationdb::RangeOption::from(&fdbt::Subspace::from_bytes(prefix));
		let result = txn
			.get_ranges_keyvalues(range, false)
			.try_collect::<Vec<_>>()
			.await;
		let entries = crate::retry!(result);
		Ok(ControlFlow::Break(entries.len()))
	})
	.await
	.unwrap()
}

#[tokio::test]
async fn a_finished_process_accepts_only_the_children_that_it_lists() {
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
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
		put_process(index, arg).await;

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
		let subject = tg::authorization::Subject::Process(listed.clone());
		assert_eq!(count_delegations(index, &subject).await, 1);
		let subject = tg::authorization::Subject::Process(unlisted.clone());
		assert_eq!(count_delegations(index, &subject).await, 0);
		let processes = index
			.try_get_processes(std::slice::from_ref(&unlisted))
			.await
			.unwrap();
		assert!(processes[0].is_some());
	})
	.await;
}

#[tokio::test]
async fn a_write_cannot_change_the_creator_or_the_owner_of_a_process() {
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
		let id = tg::process::Id::new();
		let user = tg::Principal::Process(tg::process::Id::new());

		// Start the process.
		let data = process_data(tg::process::Status::Started, String::new());
		put_process(index, process_arg(&id, data.clone())).await;

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
		put_process(index, process_arg(&id, data)).await;
		let process = index
			.try_get_processes(std::slice::from_ref(&id))
			.await
			.unwrap()
			.pop()
			.unwrap()
			.unwrap();
		assert_eq!(process.creator, tg::Principal::Root);
		assert_eq!(process.owner, tg::Principal::Root);
	})
	.await;
}

#[tokio::test]
async fn a_write_cannot_change_the_sandbox_of_a_process() {
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
		let id = tg::process::Id::new();
		let sandbox = tg::sandbox::Id::new();

		// Queue the process in a sandbox.
		let mut data = process_data(tg::process::Status::Queued, String::new());
		data.sandbox = Some(sandbox.clone());
		put_process(index, process_arg(&id, data.clone())).await;

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

		// Start the process in its sandbox with a later number, and verify that the write is accepted.
		data.status = tg::process::Status::Started;
		let mut arg = process_arg(&id, data);
		arg.sequence = 1;
		put_process(index, arg).await;
		let process = index
			.try_get_processes(std::slice::from_ref(&id))
			.await
			.unwrap()
			.pop()
			.unwrap()
			.unwrap();
		assert_eq!(process.status, Some(tg::process::Status::Started));
		assert_eq!(process.sandbox, Some(sandbox));
	})
	.await;
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
		runner: None,
		scheduler: None,
		sequence: 0,
		stopped_boot: None,
		stopped_usage: None,
		touched_at: 1,
	}
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
	index
		.try_get_processes(std::slice::from_ref(id))
		.await
		.unwrap()
		.pop()
		.unwrap()
		.unwrap()
		.status
}

async fn write_items(index: &Index, items: Vec<tangram_index::Item>) -> tg::Result<()> {
	let arg = tangram_index::write::Arg { items };
	index.write(arg).await.unwrap()
}

#[tokio::test]
async fn a_queued_process_of_a_boot_that_its_holder_ended_stays_queued() {
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
		for boot in [0, 1] {
			// Record a sandbox whose holder ended boot 0 with the number three, and that started boot 1.
			let sandbox = tg::sandbox::Id::new();
			let id = tg::process::Id::new();
			let started = sandbox_put_arg(&sandbox, 0, tg::sandbox::Status::Started);
			let mut end = sandbox_put_arg(&sandbox, 0, tg::sandbox::Status::Stopped);
			end.sequence = 3;
			let next = sandbox_put_arg(&sandbox, 1, tg::sandbox::Status::Started);
			for arg in [started, end, next] {
				let item = tangram_index::Item::PutSandbox(Box::new(arg));
				write_items(index, vec![item]).await.unwrap();
			}

			// Put a queued process of the boot, and verify that it stays queued, because a live holder finishes its own processes.
			let mut arg = queued_process_arg(&id, &sandbox);
			arg.boot = Some(boot);
			let item = tangram_index::Item::PutProcess(Box::new(arg));
			write_items(index, vec![item]).await.unwrap();
			assert_eq!(
				process_status(index, &id).await,
				Some(tg::process::Status::Queued)
			);
		}
	})
	.await;
}

#[tokio::test]
async fn a_zero_end_fails_the_unfinished_processes_of_exactly_its_boot() {
	let Some(options) = options() else {
		return;
	};
	super::run(&options, async |index| {
		// Record a sandbox at boot 0 with an unfinished and a finished process of the boot, and an unfinished process of the next boot.
		let sandbox = tg::sandbox::Id::new();
		let started = sandbox_put_arg(&sandbox, 0, tg::sandbox::Status::Started);
		let item = tangram_index::Item::PutSandbox(Box::new(started));
		write_items(index, vec![item]).await.unwrap();
		let unfinished = tg::process::Id::new();
		let finished = tg::process::Id::new();
		let other = tg::process::Id::new();
		let mut arg = queued_process_arg(&unfinished, &sandbox);
		arg.boot = Some(0);
		let mut items = vec![tangram_index::Item::PutProcess(Box::new(arg))];
		let mut data = process_data(tg::process::Status::Finished, String::new());
		data.sandbox = Some(sandbox.clone());
		let mut arg = process_arg(&finished, data);
		arg.boot = Some(0);
		items.push(tangram_index::Item::PutProcess(Box::new(arg)));
		let mut arg = queued_process_arg(&other, &sandbox);
		arg.boot = Some(1);
		items.push(tangram_index::Item::PutProcess(Box::new(arg)));
		write_items(index, items).await.unwrap();

		// End the boot with the number zero, and verify that only the unfinished process of the boot is finished.
		let end = sandbox_put_arg(&sandbox, 0, tg::sandbox::Status::Stopped);
		let item = tangram_index::Item::PutSandbox(Box::new(end));
		write_items(index, vec![item]).await.unwrap();
		assert_eq!(
			process_status(index, &unfinished).await,
			Some(tg::process::Status::Finished)
		);
		assert_eq!(
			process_status(index, &finished).await,
			Some(tg::process::Status::Finished)
		);
		assert_eq!(
			process_status(index, &other).await,
			Some(tg::process::Status::Queued)
		);

		// Put a late queued process of the boot, and verify that it is finished, because its write is numbered at the boot's end.
		let late = tg::process::Id::new();
		let mut arg = queued_process_arg(&late, &sandbox);
		arg.boot = Some(0);
		let item = tangram_index::Item::PutProcess(Box::new(arg));
		write_items(index, vec![item]).await.unwrap();
		assert_eq!(
			process_status(index, &late).await,
			Some(tg::process::Status::Finished)
		);
	})
	.await;
}
