use {super::super::Index, tangram_client::prelude::*, tangram_index::prelude::*};

fn put_arg(
	id: &tg::sandbox::Id,
	boot: u64,
	status: tg::sandbox::Status,
	stopped_boot: Option<u64>,
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
		stopped_boot,
		stopped_usage: None,
		touched_at: 1,
	}
}

async fn put(index: &Index, arg: tangram_index::sandbox::put::Arg) -> tg::Result<()> {
	let arg = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutSandbox(Box::new(arg))],
	};
	index.write(arg).await.unwrap()
}

async fn status(index: &Index, id: &tg::sandbox::Id) -> (u64, tg::sandbox::Status) {
	let data = index
		.try_get_sandbox(id)
		.await
		.unwrap()
		.unwrap()
		.data
		.unwrap()
		.data;
	(data.boot, data.status)
}

/// Check that of a start and a destroy of a sandbox that is stopped at its first boot, both conditioned on that boot, only the first is applied.
async fn check_one_change_of_a_stopped_sandbox_wins(index: &Index, destroy_first: bool) {
	let id = tg::sandbox::Id::new();
	put(index, put_arg(&id, 0, tg::sandbox::Status::Stopped, None))
		.await
		.unwrap();
	let start = put_arg(&id, 1, tg::sandbox::Status::Starting, Some(0));
	let destroy = put_arg(&id, 0, tg::sandbox::Status::Destroying, Some(0));
	let (first, second, expected) = if destroy_first {
		(destroy, start, (0, tg::sandbox::Status::Destroying))
	} else {
		(start, destroy, (1, tg::sandbox::Status::Starting))
	};
	put(index, first).await.unwrap();
	assert!(put(index, second).await.is_err());
	assert_eq!(status(index, &id).await, expected);
}

/// Check that a reservation of the next boot of a stopped sandbox writes the boot and the scheduler's request in one transaction, or writes nothing if the sandbox is no longer stopped at the boot.
async fn check_a_reservation_writes_all_or_nothing(index: &Index) {
	let id = tg::sandbox::Id::new();
	let scheduler = tg::scheduler::Id::new();
	put(index, put_arg(&id, 0, tg::sandbox::Status::Stopped, None))
		.await
		.unwrap();
	let reservation = |boot| {
		let sandbox = put_arg(&id, boot + 1, tg::sandbox::Status::Starting, Some(boot));
		let request = tangram_index::scheduler::sandbox::put::Arg {
			boot: boot + 1,
			request: b"request".to_vec(),
			sandbox: id.clone(),
			scheduler: scheduler.clone(),
		};
		tangram_index::write::Arg {
			items: vec![
				tangram_index::Item::PutSandbox(Box::new(sandbox)),
				tangram_index::Item::PutSchedulerSandbox(Box::new(request)),
			],
		}
	};

	// A reservation of a boot that the sandbox is not stopped at writes nothing.
	assert!(index.write(reservation(1)).await.unwrap().is_err());
	assert_eq!(status(index, &id).await, (0, tg::sandbox::Status::Stopped));
	assert_eq!(index.try_get_sandbox_scheduler(&id).await.unwrap(), None);
	assert!(
		index
			.get_scheduler_sandboxes(&scheduler)
			.await
			.unwrap()
			.is_empty()
	);

	// A reservation of the boot that the sandbox is stopped at writes the next boot and the scheduler's request.
	index.write(reservation(0)).await.unwrap().unwrap();
	assert_eq!(status(index, &id).await, (1, tg::sandbox::Status::Starting));
	assert_eq!(
		index.try_get_sandbox_scheduler(&id).await.unwrap(),
		Some(scheduler.clone())
	);
	let sandboxes = index.get_scheduler_sandboxes(&scheduler).await.unwrap();
	assert_eq!(sandboxes.len(), 1);
	assert_eq!(sandboxes[0].id, id);
	assert_eq!(sandboxes[0].request, b"request".to_vec());
}

#[tokio::test]
async fn a_destroy_of_a_stopped_sandbox_rejects_a_later_start() {
	let Some(options) = super::process::options() else {
		return;
	};
	super::run(&options, async |index| {
		check_one_change_of_a_stopped_sandbox_wins(index, true).await;
	})
	.await;
}

#[tokio::test]
async fn a_start_of_a_stopped_sandbox_rejects_a_later_destroy() {
	let Some(options) = super::process::options() else {
		return;
	};
	super::run(&options, async |index| {
		check_one_change_of_a_stopped_sandbox_wins(index, false).await;
	})
	.await;
}

#[tokio::test]
async fn a_reservation_of_the_next_boot_writes_all_or_nothing() {
	let Some(options) = super::process::options() else {
		return;
	};
	super::run(&options, async |index| {
		check_a_reservation_writes_all_or_nothing(index).await;
	})
	.await;
}
