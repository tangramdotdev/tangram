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
			let arg = tangram_index::batch::Arg {
				items: vec![tangram_index::batch::Item::PutRunner(runner_arg.clone())],
			};
			index.batch(arg).await.unwrap().unwrap();
		}
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutRunner(first.clone())],
		};
		assert!(index.batch(arg).await.unwrap().is_err());
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
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutRunner(conflicting)],
		};
		assert!(index.batch(arg).await.unwrap().is_err());
		let process = tg::process::Id::new();
		let sandbox = tg::sandbox::Id::new();
		for arg in [&first, &current] {
			let registration = tangram_index::runner::Registration {
				index: arg.registration,
				runner: arg.id.clone(),
				scheduler: arg.scheduler.clone(),
			};
			let process_arg = tangram_index::process::put::Arg {
				cached: false,
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
				processes: None,
				registration: Some(registration.clone()),
				runner: None,
				scheduled: None,
				stopped_usage: None,
				touched_at: 0,
			};
			for item in [
				tangram_index::batch::Item::PutProcess(process_arg),
				tangram_index::batch::Item::PutSandbox(sandbox_arg),
			] {
				let arg = tangram_index::batch::Arg { items: vec![item] };
				let accepted = index.batch(arg).await.unwrap().is_ok();
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
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutRunner(runner_arg)],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert_eq!(
		index.next_runner_registration_index(&runner).await.unwrap(),
		34
	);
	let runner_arg = tangram_index::runner::put::Arg {
		id: runner.clone(),
		registration: 100,
		scheduler: tg::scheduler::Id::new(),
	};
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutRunner(runner_arg.clone())],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert_eq!(
		index.next_runner_registration_index(&runner).await.unwrap(),
		101
	);
	// Allocation must leave the accepted registration unchanged.
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutRunner(runner_arg.clone())],
	};
	index.batch(arg).await.unwrap().unwrap();
	let runner_arg = tangram_index::runner::put::Arg {
		registration: u64::MAX,
		..runner_arg
	};
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutRunner(runner_arg)],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert!(index.next_runner_registration_index(&runner).await.is_err());
}

#[tokio::test]
async fn a_scheduler_owns_a_sandbox_until_its_runner_connects() {
	let (_directory, index) = new_index();
	let scheduler = tg::scheduler::Id::new();
	let scheduled = tangram_index::sandbox::put::Scheduled {
		request: b"request".to_vec(),
		scheduler: scheduler.clone(),
	};

	// The accepted request is recorded with the starting sandbox.
	let id = tg::sandbox::Id::new();
	let starting = sandbox_arg(&id, tg::sandbox::Status::Starting, Some(scheduled.clone()));
	put_sandbox(&index, starting.clone()).await;
	let sandboxes = index.get_scheduler_sandboxes(&scheduler).await.unwrap();
	assert_eq!(sandboxes.len(), 1);
	assert_eq!(sandboxes[0].id, id);
	assert_eq!(sandboxes[0].request, b"request");

	// A destroy before the runner connects keeps the request until the sandbox is gone.
	let destroying = sandbox_arg(&id, tg::sandbox::Status::Destroying, None);
	put_sandbox(&index, destroying).await;
	let sandboxes = index.get_scheduler_sandboxes(&scheduler).await.unwrap();
	assert_eq!(sandboxes.len(), 1);
	let destroyed = sandbox_arg(&id, tg::sandbox::Status::Destroyed, None);
	put_sandbox(&index, destroyed).await;
	let sandboxes = index.get_scheduler_sandboxes(&scheduler).await.unwrap();
	assert!(sandboxes.is_empty());

	// The request is removed in the transaction that records the runner's connection.
	let id = tg::sandbox::Id::new();
	let starting = sandbox_arg(&id, tg::sandbox::Status::Starting, Some(scheduled.clone()));
	put_sandbox(&index, starting.clone()).await;
	let started = sandbox_arg(&id, tg::sandbox::Status::Started, None);
	put_sandbox(&index, started).await;
	let sandboxes = index.get_scheduler_sandboxes(&scheduler).await.unwrap();
	assert!(sandboxes.is_empty());

	// A late starting write that loses to the connection records no request.
	put_sandbox(&index, starting).await;
	let sandboxes = index.get_scheduler_sandboxes(&scheduler).await.unwrap();
	assert!(sandboxes.is_empty());

	// A starting write that arrives after the connection records no request.
	let id = tg::sandbox::Id::new();
	let started = sandbox_arg(&id, tg::sandbox::Status::Started, None);
	put_sandbox(&index, started).await;
	let starting = sandbox_arg(&id, tg::sandbox::Status::Starting, Some(scheduled));
	put_sandbox(&index, starting).await;
	let sandboxes = index.get_scheduler_sandboxes(&scheduler).await.unwrap();
	assert!(sandboxes.is_empty());
}

#[tokio::test]
async fn a_second_scheduler_takes_over_the_sandbox_key() {
	let (_directory, index) = new_index();
	let first = tg::scheduler::Id::new();
	let second = tg::scheduler::Id::new();
	let id = tg::sandbox::Id::new();
	let scheduled = |scheduler: &tg::scheduler::Id| tangram_index::sandbox::put::Scheduled {
		request: b"request".to_vec(),
		scheduler: scheduler.clone(),
	};

	// A second scheduler accepts the same boot, so that it alone holds the key.
	let arg = sandbox_arg(&id, tg::sandbox::Status::Starting, Some(scheduled(&first)));
	put_sandbox(&index, arg).await;
	let arg = sandbox_arg(&id, tg::sandbox::Status::Starting, Some(scheduled(&second)));
	put_sandbox(&index, arg).await;
	assert!(
		index
			.get_scheduler_sandboxes(&first)
			.await
			.unwrap()
			.is_empty()
	);
	assert_eq!(
		index.get_scheduler_sandboxes(&second).await.unwrap().len(),
		1
	);

	// The runner's connection removes the key.
	let arg = sandbox_arg(&id, tg::sandbox::Status::Started, None);
	put_sandbox(&index, arg).await;
	assert!(
		index
			.get_scheduler_sandboxes(&second)
			.await
			.unwrap()
			.is_empty()
	);
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
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutRunner(arg)],
		};
		index.batch(arg).await.unwrap().unwrap();
	}
	let runners = index.get_scheduler_runners(&first).await.unwrap();
	assert_eq!(runners.len(), 0);
	assert_eq!(
		index.get_scheduler_runners(&second).await.unwrap(),
		vec![runner]
	);
}

async fn put_sandbox(index: &crate::Index, arg: tangram_index::sandbox::put::Arg) {
	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutSandbox(arg)],
	};
	index.batch(arg).await.unwrap().unwrap();
}

fn sandbox_arg(
	id: &tg::sandbox::Id,
	status: tg::sandbox::Status,
	scheduled: Option<tangram_index::sandbox::put::Scheduled>,
) -> tangram_index::sandbox::put::Arg {
	let data = tg::sandbox::get::Output {
		data: tg::sandbox::Data {
			boot: 0,
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
			stopped_at: None,
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
		created_at: 0,
		data: Some(data),
		id: id.clone(),
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Sandbox(id.clone()),
		processes: None,
		registration: None,
		runner: None,
		scheduled,
		stopped_usage: None,
		touched_at: 0,
	}
}
