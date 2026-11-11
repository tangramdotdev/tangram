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

/// Run a write in one transaction as the retry of an attempt whose commit may or may not have succeeded.
async fn write_again(
	index: &Index,
	arg: &tangram_index::write::Arg,
	maybe_committed: bool,
	process_data_fragment_size: usize,
) -> tg::Result<()> {
	crate::run(&index.database, async |txn| {
		Index::write_with_transaction(
			txn,
			&index.subspace,
			&arg.items,
			index.partition_totals,
			process_data_fragment_size,
			maybe_committed,
		)
		.await
	})
	.await
	.unwrap()
}

/// Check that a retry of a write of a stopped sandbox, whose earlier attempt may have committed, is accepted as its own write if the index has what it produces, and is rejected otherwise.
async fn check_a_maybe_committed_write_of_a_stopped_sandbox_is_its_own(
	index: &Index,
	process_data_fragment_size: usize,
) {
	let id = tg::sandbox::Id::new();
	let scheduler = tg::scheduler::Id::new();
	put(index, put_arg(&id, 0, tg::sandbox::Status::Stopped, None))
		.await
		.unwrap();
	let reservation = |request: &[u8]| {
		let sandbox = put_arg(&id, 1, tg::sandbox::Status::Starting, Some(0));
		let request = tangram_index::scheduler::sandbox::put::Arg {
			boot: 1,
			request: request.to_vec(),
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

	// Commit the reservation, as the first attempt whose commit reported an unknown result.
	let arg = reservation(b"request");
	index.write(arg.clone()).await.unwrap().unwrap();

	// A retry that knows the first attempt may have committed recognizes its own reservation.
	write_again(index, &arg, true, process_data_fragment_size)
		.await
		.unwrap();

	// A retry that does not know it, or a reservation by a different start, is rejected.
	assert!(
		write_again(index, &arg, false, process_data_fragment_size)
			.await
			.is_err()
	);
	let other = reservation(b"other");
	assert!(
		write_again(index, &other, true, process_data_fragment_size)
			.await
			.is_err()
	);

	// A destroy conditioned on the stopped boot is rejected although its attempt may have committed, because the index has the reservation and not the destruction.
	let destroy = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutSandbox(Box::new(put_arg(
			&id,
			0,
			tg::sandbox::Status::Destroying,
			Some(0),
		)))],
	};
	assert!(
		write_again(index, &destroy, true, process_data_fragment_size)
			.await
			.is_err()
	);
	assert_eq!(status(index, &id).await, (1, tg::sandbox::Status::Starting));

	// A retry of a destroy of a stopped sandbox whose attempt may have committed recognizes its own destruction.
	let id = tg::sandbox::Id::new();
	put(index, put_arg(&id, 0, tg::sandbox::Status::Stopped, None))
		.await
		.unwrap();
	let destroy = tangram_index::write::Arg {
		items: vec![tangram_index::Item::PutSandbox(Box::new(put_arg(
			&id,
			0,
			tg::sandbox::Status::Destroying,
			Some(0),
		)))],
	};
	index.write(destroy.clone()).await.unwrap().unwrap();
	write_again(index, &destroy, true, process_data_fragment_size)
		.await
		.unwrap();
	assert!(
		write_again(index, &destroy, false, process_data_fragment_size)
			.await
			.is_err()
	);
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

#[tokio::test]
async fn a_maybe_committed_write_of_a_stopped_sandbox_is_its_own() {
	let Some(options) = super::process::options() else {
		return;
	};
	let process_data_fragment_size = options.process_data_fragment_size;
	super::run(&options, async |index| {
		check_a_maybe_committed_write_of_a_stopped_sandbox_is_its_own(
			index,
			process_data_fragment_size,
		)
		.await;
	})
	.await;
}

#[tokio::test]
async fn concurrent_writes_of_one_sandbox_and_a_write_across_sandboxes_are_applied() {
	let Some(options) = super::process::options() else {
		return;
	};
	super::run(&options, async |index| {
		// Write boots of one sandbox concurrently with writes of other sandboxes, so that the writes share lanes and transactions.
		let id = tg::sandbox::Id::new();
		let others = (0..8).map(|_| tg::sandbox::Id::new()).collect::<Vec<_>>();
		let boots = (0..16).map(|boot| {
			put(
				index,
				put_arg(&id, boot, tg::sandbox::Status::Started, None),
			)
		});
		let others_puts = others
			.iter()
			.map(|other| put(index, put_arg(other, 0, tg::sandbox::Status::Started, None)));
		let (boots, others_puts) = futures::future::join(
			futures::future::join_all(boots),
			futures::future::join_all(others_puts),
		)
		.await;
		assert!(boots.iter().chain(&others_puts).all(Result::is_ok));
		assert_eq!(status(index, &id).await, (15, tg::sandbox::Status::Started));

		// A write of two sandboxes, which the writer may split across lanes, answers once and applies both.
		let a = tg::sandbox::Id::new();
		let b = tg::sandbox::Id::new();
		let arg = tangram_index::write::Arg {
			items: vec![
				tangram_index::Item::PutSandbox(Box::new(put_arg(
					&a,
					0,
					tg::sandbox::Status::Stopped,
					None,
				))),
				tangram_index::Item::PutSandbox(Box::new(put_arg(
					&b,
					0,
					tg::sandbox::Status::Stopped,
					None,
				))),
			],
		};
		index.write(arg).await.unwrap().unwrap();
		assert_eq!(status(index, &a).await, (0, tg::sandbox::Status::Stopped));
		assert_eq!(status(index, &b).await, (0, tg::sandbox::Status::Stopped));
	})
	.await;
}
