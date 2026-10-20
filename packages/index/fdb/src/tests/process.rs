use {
	super::super::{Index, Kind},
	foundationdb_tuple as fdbt,
	futures::TryStreamExt as _,
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

fn options() -> crate::Options {
	let cluster = std::env::var_os("FDB_CLUSTER_FILE")
		.expect("set FDB_CLUSTER_FILE")
		.into();
	options_with_cluster(cluster)
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
		write_operation_batch_size: 1024,
		write_transaction_concurrency: 1,
	}
}

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
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(arg)],
	};
	index.batch(arg).await.unwrap().unwrap();
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn rewriting_process_data_replaces_its_fragments() {
	super::run(&options(), async |index| {
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

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn cleaning_a_process_deletes_its_data() {
	super::run(&options(), async |index| {
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
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn putting_a_process_without_data_keeps_its_cache_entry() {
	super::run(&options(), async |index| {
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
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn touching_a_process_with_data_returns_its_data() {
	super::run(&options(), async |index| {
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
