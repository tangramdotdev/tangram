use {super::new_index, tangram_client::prelude::*};

#[tokio::test]
async fn registration_attempts_fence_delayed_updates_and_cleanup() {
	for same_scheduler in [false, true] {
		let (_directory, index) = new_index();
		let first = tangram_index::runner::Registration {
			attempt: 1,
			runner: tg::runner::Id::new(),
			scheduler: tg::scheduler::Id::new(),
		};
		let group = tg::group::Id::new();
		let put = tangram_index::batch::Arg {
			items: vec![
				tangram_index::batch::Item::PutRunner(first.clone()),
				tangram_index::batch::Item::PutGroup(tangram_index::group::put::Arg {
					id: group.clone(),
					parent: None,
					specifier: "test".parse().unwrap(),
				}),
			],
		};
		index.batch(put).await.unwrap().unwrap();
		// Exercise both migration to a different scheduler and reconnection to the same one.
		let scheduler = if same_scheduler {
			first.scheduler.clone()
		} else {
			tg::scheduler::Id::new()
		};
		let current = tangram_index::runner::Registration {
			attempt: 2,
			scheduler,
			..first.clone()
		};
		let put = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutRunner(current.clone())],
		};
		index.batch(put).await.unwrap().unwrap();
		let conflicting = tangram_index::runner::Registration {
			scheduler: tg::scheduler::Id::new(),
			..current.clone()
		};
		let put = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutRunner(conflicting)],
		};
		assert!(index.batch(put).await.unwrap().is_err());
		let stale = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutRunner(first.clone())],
		};
		assert!(index.batch(stale).await.unwrap().is_err());
		let stale = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::DeleteGroup(group.clone())],
		}
		.with_runner(first.clone());
		assert!(index.batch(stale).await.unwrap().is_err());
		let groups = index
			.try_get_groups(std::slice::from_ref(&group))
			.await
			.unwrap();
		assert!(groups[0].is_some());
		let check = tangram_index::batch::Arg::default().with_runner(current);
		index.batch(check).await.unwrap().unwrap();
	}
}

#[tokio::test]
async fn attempt_allocations_are_unique_and_survive_reopen() {
	use super::{Config, Index};
	let dir = tempfile::tempdir().unwrap();
	let config = Config {
		map_size: 1 << 30,
		max_process_depth: None,
		max_readers: 1_000,
		object_permission_time_to_touch: std::time::Duration::ZERO,
		path: dir.path().join("index"),
		posix_sem_prefix: None,
		process_data_fragment_size: 16,
		process_permission_time_to_touch: std::time::Duration::ZERO,
		read_channel_capacity: 256,
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total: 1,
		write_channel_capacity: 256,
		write_operation_batch_size: 1,
	};
	let index = Index::new(&config).unwrap();

	let runner = tg::runner::Id::new();
	let allocations =
		futures::future::try_join_all((0..32).map(|_| index.next_runner_attempt(&runner)))
			.await
			.unwrap();
	let mut allocations = allocations;
	allocations.sort_unstable();
	assert_eq!(allocations, (1..=32).collect::<Vec<_>>());

	drop(index);
	let index = Index::new(&config).unwrap();

	assert_eq!(index.next_runner_attempt(&runner).await.unwrap(), 33);
	assert_eq!(
		index
			.next_runner_attempt(&tg::runner::Id::new())
			.await
			.unwrap(),
		1
	);
	let registration = tangram_index::runner::Registration {
		attempt: 100,
		runner: runner.clone(),
		scheduler: tg::scheduler::Id::new(),
	};
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutRunner(registration.clone())],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert_eq!(index.next_runner_attempt(&runner).await.unwrap(), 101);
	// Allocation must leave the accepted registration unchanged.
	index
		.batch(tangram_index::batch::Arg::default().with_runner(registration.clone()))
		.await
		.unwrap()
		.unwrap();
	let registration = tangram_index::runner::Registration {
		attempt: u64::MAX,
		..registration
	};
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutRunner(registration)],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert!(index.next_runner_attempt(&runner).await.is_err());
}
