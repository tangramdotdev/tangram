use {super::super::Index, tangram_client::prelude::*};

#[tokio::test]
async fn sandbox_keys_resolve_by_boot() {
	let Some(cluster) = tangram_tests_util::database::cluster() else {
		return;
	};
	let options = crate::Options {
		cleaning_partition_total: 1,
		cluster,
		instance: Some(format!(
			"index_scheduler_test_{:032x}/",
			rand::random::<u128>()
		)),
		max_process_depth: None,
		object_permission_time_to_touch: std::time::Duration::ZERO,
		max_write_operation_batch_size: 1024,
		permission_update_partition_total: 1,
		process_data_fragment_size: 16,
		process_permission_time_to_touch: std::time::Duration::ZERO,
		read_channel_capacity: 256,
		read_request_batch_size: 4,
		read_task_concurrency: 16,
		read_task_count: 4,
		storage_and_metadata_update_partition_total: 1,
		storage_relation_batch_size: 8,
		usage_partition_total: 1,
		usage_update_partition_total: 1,
		verification: crate::VerificationConfig { concurrency: 64 },
		write_item_transaction_concurrency: 1,
		write_operation_batch_size: 1024,
		write_transaction_concurrency: 1,
	};
	super::run(&options, async |index| {
		let first = tg::scheduler::Id::new();
		let second = tg::scheduler::Id::new();
		let sandbox = tg::sandbox::Id::new();

		// A put records the request in the scheduler's queue and the scheduler as the sandbox's owner.
		write(index, put(&sandbox, 0, &first)).await;
		assert_eq!(queue(index, &first).await, vec![sandbox.clone()]);
		assert_eq!(owner(index, &sandbox).await, Some(first.clone()));

		// A put of a later boot moves the sandbox to the second scheduler, and an older put after it does not apply.
		write(index, put(&sandbox, 1, &second)).await;
		write(index, put(&sandbox, 0, &first)).await;
		assert_eq!(queue(index, &first).await, Vec::<tg::sandbox::Id>::new());
		assert_eq!(queue(index, &second).await, vec![sandbox.clone()]);

		// An older delete does not apply, and a delete of the boot removes both keys.
		write(index, delete(&sandbox, 0)).await;
		assert_eq!(owner(index, &sandbox).await, Some(second.clone()));
		write(index, delete(&sandbox, 1)).await;
		assert_eq!(queue(index, &second).await, Vec::<tg::sandbox::Id>::new());
		assert_eq!(owner(index, &sandbox).await, None);

		// A put of the boot after its delete does not apply.
		write(index, put(&sandbox, 1, &second)).await;
		assert_eq!(queue(index, &second).await, Vec::<tg::sandbox::Id>::new());

		// A delete that applies before its put wins.
		let other = tg::sandbox::Id::new();
		write(index, delete(&other, 0)).await;
		write(index, put(&other, 0, &first)).await;
		assert_eq!(owner(index, &other).await, None);
	})
	.await;
}

fn put(sandbox: &tg::sandbox::Id, boot: u64, scheduler: &tg::scheduler::Id) -> tangram_index::Item {
	let arg = tangram_index::scheduler::sandbox::put::Arg {
		boot,
		request: b"request".to_vec(),
		sandbox: sandbox.clone(),
		scheduler: scheduler.clone(),
	};
	tangram_index::Item::PutSchedulerSandbox(Box::new(arg))
}

fn delete(sandbox: &tg::sandbox::Id, boot: u64) -> tangram_index::Item {
	let arg = tangram_index::scheduler::sandbox::delete::Arg {
		boot,
		sandbox: sandbox.clone(),
	};
	tangram_index::Item::DeleteSchedulerSandbox(Box::new(arg))
}

async fn write(index: &Index, item: tangram_index::Item) {
	let arg = tangram_index::write::Arg { items: vec![item] };
	index.write(arg).await.unwrap().unwrap();
}

async fn queue(index: &Index, scheduler: &tg::scheduler::Id) -> Vec<tg::sandbox::Id> {
	let sandboxes = index.get_scheduler_sandboxes(scheduler).await.unwrap();
	sandboxes.into_iter().map(|sandbox| sandbox.id).collect()
}

async fn owner(index: &Index, sandbox: &tg::sandbox::Id) -> Option<tg::scheduler::Id> {
	index.try_get_sandbox_scheduler(sandbox).await.unwrap()
}
