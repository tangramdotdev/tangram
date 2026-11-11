use {super::new_index, tangram_client::prelude::*};

#[tokio::test]
async fn registration_indices_fence_delayed_updates_and_cleanup() {
	let (_directory, index) = new_index();

	for same_scheduler in [false, true] {
		let first = tangram_index::runner::put::Arg {
			id: tg::runner::Id::new(),
			registration: 1,
			scheduler: tg::scheduler::Id::new(),
		};
		let current = tangram_index::runner::put::Arg {
			registration: 2,
			scheduler: if same_scheduler {
				first.scheduler.clone()
			} else {
				tg::scheduler::Id::new()
			},
			..first.clone()
		};
		for runner_arg in [&first, &current, &current] {
			let arg = tangram_index::write::Arg {
				items: vec![tangram_index::Item::PutRunner(Box::new(runner_arg.clone()))],
			};
			index.write(arg).await.unwrap().unwrap();
		}
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutRunner(Box::new(first.clone()))],
		};
		assert!(index.write(arg).await.unwrap().is_err());
		assert_eq!(
			index
				.next_runner_registration_index(&current.id)
				.await
				.unwrap(),
			3
		);
		let conflicting = tangram_index::runner::put::Arg {
			scheduler: tg::scheduler::Id::new(),
			..current.clone()
		};
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutRunner(Box::new(conflicting))],
		};
		assert!(index.write(arg).await.unwrap().is_err());
		let process = tg::process::Id::new();
		let sandbox = tg::sandbox::Id::new();
		for arg in [&first, &current] {
			let registration = tangram_index::runner::Registration {
				index: arg.registration,
				runner: arg.id.clone(),
				scheduler: arg.scheduler.clone(),
			};
			let process_arg = tangram_index::process::put::Arg {
				account: None,
				cached: false,
				captures: tangram_index::permission::capture::Captures::default(),
				children: None,
				command: None,
				command_id: tg::command::Id::new(b"command").into(),
				data: None,
				error: None,
				id: process.clone(),
				location: None,
				log: None,
				metadata: tg::process::Metadata::default(),
				options: tg::referent::Options::default(),
				output: None,
				parent: None,
				permissions: vec![],
				principal: tg::Principal::Root,
				registration: Some(registration.clone()),
				sandbox: None,
				storage: tg::process::storage::Set::NODE,
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 0,
			};
			let sandbox_arg = tangram_index::sandbox::put::Arg {
				account: None,
				created_at: 0,
				data: None,
				id: sandbox.clone(),
				location: None,
				permissions: vec![],
				principal: tg::Principal::Root,
				process: None,
				processes: None,
				registration: Some(registration.clone()),
				runner: None,
				stopped_boot: None,
				stopped_usage: None,
				touched_at: 0,
			};
			for item in [
				tangram_index::Item::PutProcess(Box::new(process_arg)),
				tangram_index::Item::PutSandbox(Box::new(sandbox_arg)),
			] {
				let arg = tangram_index::write::Arg { items: vec![item] };
				let accepted = index.write(arg).await.unwrap().is_ok();
				assert_eq!(accepted, registration.index == current.registration);
			}
			assert_eq!(
				index
					.try_get_processes(std::slice::from_ref(&process))
					.await
					.unwrap()[0]
					.is_some(),
				registration.index == current.registration
			);
			assert_eq!(
				index
					.try_get_sandboxes(std::slice::from_ref(&sandbox))
					.await
					.unwrap()[0]
					.is_some(),
				registration.index == current.registration
			);
		}
	}
}

#[tokio::test]
async fn registration_index_allocations_are_unique_and_survive_reopen() {
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
	let allocations = futures::future::try_join_all(
		(0..32).map(|_| index.next_runner_registration_index(&runner)),
	)
	.await
	.unwrap();
	let mut allocations = allocations;
	allocations.sort_unstable();
	assert_eq!(allocations, (1..=32).collect::<Vec<_>>());

	drop(index);
	let index = Index::new(&config).unwrap();

	assert_eq!(
		index.next_runner_registration_index(&runner).await.unwrap(),
		33
	);
	assert_eq!(
		index
			.next_runner_registration_index(&tg::runner::Id::new())
			.await
			.unwrap(),
		1
	);
	let runner_arg = tangram_index::runner::put::Arg {
		id: runner.clone(),
		registration: 1,
		scheduler: tg::scheduler::Id::new(),
	};
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutRunner(Box::new(runner_arg))],
	};
	index.write(arg).await.unwrap().unwrap();
	assert_eq!(
		index.next_runner_registration_index(&runner).await.unwrap(),
		34
	);
	let runner_arg = tangram_index::runner::put::Arg {
		id: runner.clone(),
		registration: 100,
		scheduler: tg::scheduler::Id::new(),
	};
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutRunner(Box::new(runner_arg.clone()))],
	};
	index.write(arg).await.unwrap().unwrap();
	assert_eq!(
		index.next_runner_registration_index(&runner).await.unwrap(),
		101
	);
	// Allocation must leave the accepted registration unchanged.
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutRunner(Box::new(runner_arg.clone()))],
	};
	index.write(arg).await.unwrap().unwrap();
	let runner_arg = tangram_index::runner::put::Arg {
		registration: u64::MAX,
		..runner_arg
	};
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutRunner(Box::new(runner_arg))],
	};
	index.write(arg).await.unwrap().unwrap();
	assert!(index.next_runner_registration_index(&runner).await.is_err());
}

#[tokio::test]
async fn a_scheduler_lists_its_runners() {
	let (_directory, index) = new_index();
	let first = tg::scheduler::Id::new();
	let second = tg::scheduler::Id::new();
	let runner = tg::runner::Id::new();
	for (registration, scheduler) in [(1, &first), (2, &second)] {
		let arg = tangram_index::runner::put::Arg {
			id: runner.clone(),
			registration,
			scheduler: scheduler.clone(),
		};
		let arg = tangram_index::write::Arg {
			items: vec![tangram_index::Item::PutRunner(Box::new(arg))],
		};
		index.write(arg).await.unwrap().unwrap();
	}
	let runners = index.get_scheduler_runners(&first).await.unwrap();
	assert_eq!(runners.len(), 0);
	assert_eq!(
		index.get_scheduler_runners(&second).await.unwrap(),
		vec![runner]
	);
}
