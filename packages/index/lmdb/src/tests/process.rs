use {
	super::super::{Index, Kind},
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_index::prelude::*,
};

fn process_arg(id: &tg::process::Id, data: tg::process::Data) -> tangram_index::process::put::Arg {
	let command = tg::command::Id::new(b"command");
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
		principal: tg::Principal::Process(id.clone()),
		sandbox: None,
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
		debug: None,
		error: None,
		exit: finished.then_some(0),
		finished_at: finished.then_some(0),
		host,
		log: None,
		output: None,
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

async fn put_process(index: &Index, arg: tangram_index::process::put::Arg) {
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(arg)],
	};
	index.batch(arg).await.unwrap().unwrap();
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
		let arg = tangram_index::batch::Arg {
			items: vec![
				tangram_index::batch::Item::PutProcess(arg),
				tangram_index::batch::Item::PutDelegation(delegation),
			],
		};
		index.batch(arg).await.unwrap().unwrap();
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
async fn stale_capture_cannot_restore_cleaned_process_records_or_permissions() {
	let (_dir, index) = super::new_index();

	// Create a process with a pending capture and a captured node permission.
	let id = tg::process::Id::new();
	let data = process_data(tg::process::Status::Finished, String::new());
	let resource: tg::Id = tg::file::Id::new(b"capture-race").into();
	let capture = tangram_index::permission::capture::Capture {
		expires_at: i64::MAX,
		principal: tg::Principal::Anonymous,
		resource: tg::Referent::with_node(resource.clone()),
		sources: std::collections::BTreeSet::new(),
		subject: id.clone().into(),
		version: None,
	};
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutPermissionCapture(
			capture.clone(),
		)],
	};
	index.batch(batch).await.unwrap().unwrap();
	let permission = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: Some(tg::Principal::Process(id.clone())),
		permissions: tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		)
		.into(),
		resource: resource.clone(),
		source: tangram_index::permission::Source::Direct { expires_at: None },
		subject: tg::authorization::Subject::Process(id.clone()),
		time_to_touch: None,
		version: None,
	};
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::CapturePermission {
			capture: capture.clone(),
			permission: permission.clone(),
		}],
	};
	index.batch(batch).await.unwrap().unwrap();

	// Capture can precede the first process indexing during spawn.
	{
		let transaction = index.env.read_txn().unwrap();
		let permissions = Index::get_resource_permission_entries_for_subject_with_transaction(
			&index.db,
			&index.subspace,
			&transaction,
			&resource,
			&permission.subject,
		)
		.unwrap();
		assert_eq!(permissions.len(), 1);
	}
	put_process(&index, process_arg(&id, data)).await;

	// Read the capture as a worker would before cleanup.
	let records = index.get_permission_captures(&resource).await.unwrap();
	assert_eq!(records.len(), 1);

	// Garbage-collect the process and its capture records.
	let arg = tangram_index::clean::Arg {
		batch_size: 100,
		max_object_touched_at: i64::MAX,
		max_process_touched_at: i64::MAX,
		max_sandbox_touched_at: i64::MAX,
		now: i64::MAX - 1,
		partition_end: 1,
		partition_start: 0,
	};
	index.clean(arg).await.unwrap();
	assert!(index.try_get_process(&id).await.unwrap().is_none());
	assert_eq!(index.get_permission_captures(&resource).await.unwrap(), []);

	// Resume the stale worker after cleanup.
	let batch = tangram_index::batch::Arg {
		items: records
			.into_iter()
			.map(
				|capture| tangram_index::batch::Item::ContinuePermissionCapture {
					resource: capture.resource.clone(),
					capture,
				},
			)
			.chain(std::iter::once(
				tangram_index::batch::Item::CapturePermission {
					capture: capture.clone(),
					permission: permission.clone(),
				},
			))
			.collect(),
	};
	index.batch(batch).await.unwrap().unwrap();
	assert!(
		index
			.get_permission_captures(&resource)
			.await
			.unwrap()
			.is_empty(),
		"an in-flight worker restored capture records for a cleaned process"
	);
	let transaction = index.env.read_txn().unwrap();
	let permissions = Index::get_resource_permission_entries_for_subject_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&resource,
		&permission.subject,
	)
	.unwrap();
	assert!(
		permissions.is_empty(),
		"an in-flight worker restored permissions for a cleaned process"
	);
}
