use {
	super::{super::Index, new_index, new_index_with_usage_partition_total},
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

fn add_delta(
	index: &Index,
	account: &tangram_index::usage::Account,
	at: i64,
	kind: tangram_index::usage::DeltaKind,
	delta: i64,
) {
	add_delta_to_partition(index, account, at, kind, delta, 0);
}

fn add_delta_to_partition(
	index: &Index,
	account: &tangram_index::usage::Account,
	at: i64,
	kind: tangram_index::usage::DeltaKind,
	delta: i64,
	partition: u64,
) {
	let mut transaction = index.env.write_txn().unwrap();
	let entry = tangram_index::usage::DeltaArg {
		account,
		at,
		delta,
		kind,
		partition,
	};
	Index::add_usage_delta(&index.db, &index.subspace, &mut transaction, entry).unwrap();
	transaction.commit().unwrap();
}

async fn aggregate(index: &Index, now: jiff::Timestamp) {
	loop {
		let output = index
			.aggregate_usage(tangram_index::usage::aggregate::Arg {
				batch_size: 1_000,
				delta_time_to_live: crate::tests::USAGE_DELTA_TIME_TO_LIVE,
				now,
				partition_end: 1,
				partition_start: 0,
			})
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
}

fn hour(start: i64) -> tangram_index::usage::Period {
	tangram_index::usage::Period::from_kind_and_start(tangram_index::usage::PeriodKind::Hour, start)
		.unwrap()
}

#[test]
fn usage_keys_round_trip() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let mut keys = vec![
		crate::usage::Key::Aggregation {
			account: account.clone(),
			hour: 60 * 60,
			partition: 4,
		},
		crate::usage::Key::Started,
	];
	for (kind, start, value) in [
		(tangram_index::usage::PeriodKind::Hour, 0, 0),
		(tangram_index::usage::PeriodKind::Day, 0, 1),
		(tangram_index::usage::PeriodKind::Week, -3 * 24 * 60 * 60, 2),
		(tangram_index::usage::PeriodKind::Month, 0, 3),
	] {
		assert_eq!(kind as u8, value);
		let period = tangram_index::usage::Period::from_kind_and_start(kind, start).unwrap();
		keys.push(crate::usage::Key::Aggregate {
			account: account.clone(),
			partition: 3,
			period,
		});
		keys.push(crate::usage::Key::Unavailable {
			account: account.clone(),
			kind,
			partition: 5,
		});
	}
	for (kind, value) in [
		(tangram_index::usage::DeltaKind::ObjectCount, 0),
		(tangram_index::usage::DeltaKind::ObjectSize, 1),
		(tangram_index::usage::DeltaKind::ProcessCount, 2),
		(tangram_index::usage::DeltaKind::SandboxCount, 3),
		(tangram_index::usage::DeltaKind::SandboxCpuConsumption, 4),
		(tangram_index::usage::DeltaKind::SandboxMemoryConsumption, 5),
	] {
		assert_eq!(kind.to_i32(), Some(value));
		keys.push(crate::usage::Key::Delta {
			account: account.clone(),
			hour: 2 * 60 * 60,
			kind,
			partition: 5,
		});
	}
	for expected in keys {
		let key = crate::Key::Usage(expected.clone());
		let bytes = Index::pack(&index.subspace, &key);
		let crate::Key::Usage(actual) = Index::unpack(&index.subspace, &bytes).unwrap() else {
			panic!("unexpected key type");
		};
		assert_eq!(actual, expected);
	}
}

#[tokio::test]
async fn usage_start_time_is_initialized_once() {
	let (_dir, index) = new_index();
	let key = Index::pack(
		&index.subspace,
		&crate::Key::Usage(crate::usage::Key::Started),
	);
	let mut transaction = index.env.write_txn().unwrap();
	index.db.delete(&mut transaction, &key).unwrap();
	transaction.commit().unwrap();

	let first = jiff::Timestamp::new(60 * 60, 0).unwrap();
	let second = jiff::Timestamp::new(2 * 60 * 60, 0).unwrap();
	index.start_usage(first).await.unwrap();
	index.start_usage(second).await.unwrap();

	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let error = index
		.get_usage(
			&account,
			hour(0),
			second,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap_err();
	assert!(error.to_string().contains("usage is unavailable"));
	let usage = index
		.get_usage(
			&account,
			hour(60 * 60),
			second,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(usage, tangram_index::usage::Aggregate::default());
}

#[tokio::test]
async fn usage_unavailability_is_scoped_by_account_and_partition() {
	let (_dir, index) = new_index_with_usage_partition_total(2);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let other = tangram_index::usage::Account::User(tg::user::Id::new());
	let period = hour(0);
	let mut transaction = index.env.write_txn().unwrap();
	let entry = tangram_index::usage::PartitionAggregate {
		object_count: 3,
		..Default::default()
	};
	Index::put_usage_aggregate_with_transaction(
		&index.db,
		&index.subspace,
		&mut transaction,
		&account,
		0,
		period,
		entry,
	)
	.unwrap();
	let entry = tangram_index::usage::PartitionAggregate {
		object_count: 7,
		..Default::default()
	};
	Index::put_usage_aggregate_with_transaction(
		&index.db,
		&index.subspace,
		&mut transaction,
		&account,
		1,
		period,
		entry,
	)
	.unwrap();
	transaction.commit().unwrap();

	let retained = std::time::Duration::from_hours(365 * 24);
	let now = jiff::Timestamp::new(2 * 60 * 60, 0).unwrap();
	index
		.expire_usage(tangram_index::usage::expire::Arg {
			batch_size: 1_000,
			day_time_to_live: retained,
			delta_time_to_live: retained,
			hour_time_to_live: std::time::Duration::ZERO,
			month_time_to_live: retained,
			now,
			partition_end: 1,
			partition_start: 0,
			week_time_to_live: retained,
		})
		.await
		.unwrap();

	let error = index
		.get_usage(
			&account,
			period,
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap_err();
	assert!(error.to_string().contains("usage is unavailable"));
	let usage = index
		.get_usage(&other, period, now, crate::tests::USAGE_DELTA_TIME_TO_LIVE)
		.await
		.unwrap();
	assert_eq!(usage, tangram_index::usage::Aggregate::default());
	let mut transaction = index.env.write_txn().unwrap();
	let cutoff = Index::try_get_usage_unavailable_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&account,
		tangram_index::usage::PeriodKind::Hour,
		0,
	)
	.unwrap();
	assert_eq!(cutoff, Some(60 * 60));
	let cutoff = Index::try_get_usage_unavailable_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&account,
		tangram_index::usage::PeriodKind::Hour,
		1,
	)
	.unwrap();
	assert_eq!(cutoff, None);
	Index::mark_usage_unavailable_with_transaction(
		&index.db,
		&index.subspace,
		&mut transaction,
		&account,
		tangram_index::usage::PeriodKind::Hour,
		0,
		0,
	)
	.unwrap();
	let cutoff = Index::try_get_usage_unavailable_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&account,
		tangram_index::usage::PeriodKind::Hour,
		0,
	)
	.unwrap();
	assert_eq!(cutoff, Some(60 * 60));
	let aggregate = Index::try_get_usage_aggregate_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&account,
		1,
		period,
	)
	.unwrap()
	.unwrap();
	assert_eq!(aggregate.object_count, 7);
}

fn sandbox_data(
	id: tg::sandbox::Id,
	owner: tg::Principal,
	status: tg::sandbox::Status,
	usage: Option<tg::sandbox::Usage>,
) -> tg::sandbox::get::Output {
	tg::sandbox::get::Output {
		data: tg::sandbox::Data {
			boot: 0,
			cpu: Some(2.into()),
			creator: owner.clone(),
			error: None,
			hostname: None,
			id,
			memory: Some((1 << 30).into()),
			mounts: Vec::new(),
			network: None,
			owner,
			status,
			storage: None,
			ttl: None,
			usage,
		},
		location: None,
		tokens: tg::authorization::Tokens::default(),
	}
}

#[tokio::test]
async fn aggregates_compute_and_carries_storage_across_empty_hours() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::SandboxCpuConsumption,
		3,
	);
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::SandboxMemoryConsumption,
		5,
	);
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectSize,
		10,
	);
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::SandboxCount,
		1,
	);

	let now = jiff::Timestamp::new(3 * 60 * 60, 0).unwrap();
	let first = index
		.get_usage(
			&account,
			hour(0),
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(first.sandbox_cpu_consumption, 3);
	assert_eq!(first.sandbox_memory_consumption, 5);
	assert_eq!(first.object_count, 1);
	assert_eq!(first.object_size, 10);
	assert_eq!(first.sandbox_count, 1);

	let period = tangram_index::usage::Period::day("1970-01-01").unwrap();
	let during_third = jiff::Timestamp::new(2 * 60 * 60 + 1, 0).unwrap();
	let partial = index
		.get_usage(
			&account,
			period,
			during_third,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(partial.sandbox_cpu_consumption, 3);
	assert_eq!(partial.sandbox_memory_consumption, 5);
	assert_eq!(partial.object_count, 3);
	assert_eq!(partial.object_size, 30);
	assert_eq!(partial.sandbox_count, 1);

	let third = index
		.get_usage(
			&account,
			hour(2 * 60 * 60),
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(third.sandbox_cpu_consumption, 0);
	assert_eq!(third.sandbox_memory_consumption, 0);
	assert_eq!(third.object_count, 1);
	assert_eq!(third.object_size, 10);
	assert_eq!(third.sandbox_count, 0);
}

#[tokio::test]
async fn aggregates_late_storage_deltas_into_future_hours() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	add_delta(
		&index,
		&account,
		2 * 60 * 60 + 1,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	let now = jiff::Timestamp::new(3 * 60 * 60 + 30 * 60, 0).unwrap();
	aggregate(&index, now).await;
	let period = hour(3 * 60 * 60);
	let usage = index
		.get_usage(
			&account,
			period,
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(usage.object_count, 1);

	add_delta(
		&index,
		&account,
		2 * 60 * 60 + 2,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	aggregate(&index, now).await;
	let usage = index
		.get_usage(
			&account,
			period,
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(usage.object_count, 2);
}

#[tokio::test]
async fn aggregates_hourly_storage_gauges_into_a_day() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::SandboxCpuConsumption,
		3,
	);
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectSize,
		10,
	);
	add_delta(
		&index,
		&account,
		2 * 60 * 60 + 1,
		tangram_index::usage::DeltaKind::ObjectCount,
		-1,
	);
	add_delta(
		&index,
		&account,
		2 * 60 * 60 + 1,
		tangram_index::usage::DeltaKind::ObjectSize,
		-10,
	);

	let period = tangram_index::usage::Period::day("1970-01-01").unwrap();
	let now = jiff::Timestamp::new(24 * 60 * 60, 0).unwrap();
	let usage = index
		.get_usage(
			&account,
			period,
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(usage.sandbox_cpu_consumption, 3);
	assert_eq!(usage.sandbox_memory_consumption, 0);
	assert_eq!(usage.object_count, 2);
	assert_eq!(usage.object_size, 20);
	assert_eq!(usage.process_count, 0);
	assert_eq!(usage.sandbox_count, 0);
}

#[tokio::test]
async fn aggregates_signed_storage_across_partitions() {
	let (_dir, index) = new_index_with_usage_partition_total(2);
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	add_delta_to_partition(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
		0,
	);
	add_delta_to_partition(
		&index,
		&account,
		2 * 60 * 60 + 1,
		tangram_index::usage::DeltaKind::ObjectCount,
		-1,
		1,
	);

	let period = tangram_index::usage::Period::day("1970-01-01").unwrap();
	let now = jiff::Timestamp::new(4 * 60 * 60, 0).unwrap();
	let usage = index
		.get_usage(
			&account,
			period,
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(usage.object_count, 2);
}

#[tokio::test]
async fn aggregates_parent_periods_without_boundary_usage() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::SandboxCpuConsumption,
		3,
	);
	let now = jiff::Timestamp::new(32 * 24 * 60 * 60, 0).unwrap();
	aggregate(&index, now).await;

	let transaction = index.env.write_txn().unwrap();

	for period in [
		tangram_index::usage::Period::day("1970-01-01").unwrap(),
		tangram_index::usage::Period::month("1970-01").unwrap(),
		tangram_index::usage::Period::week("1970-W01").unwrap(),
	] {
		let aggregate = Index::try_get_usage_aggregate_with_transaction(
			&index.db,
			&index.subspace,
			&transaction,
			&account,
			0,
			period,
		)
		.unwrap()
		.unwrap();
		assert_eq!(aggregate.sandbox_cpu_consumption, 3);
	}
}

#[tokio::test]
async fn expire_usage_preserves_aggregates_before_deleting_deltas() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	let now = jiff::Timestamp::new(2 * 60 * 60, 0).unwrap();
	let period = hour(0);
	let expected = index
		.get_usage(
			&account,
			period,
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	aggregate(&index, now).await;
	let retained = std::time::Duration::new(365 * 86_400, 0);
	let arg = tangram_index::usage::expire::Arg {
		batch_size: 1_000,
		day_time_to_live: retained,
		delta_time_to_live: std::time::Duration::ZERO,
		hour_time_to_live: retained,
		month_time_to_live: retained,
		now,
		partition_end: 1,
		partition_start: 0,
		week_time_to_live: retained,
	};
	index.expire_usage(arg.clone()).await.unwrap();
	let actual = index
		.get_usage(
			&account,
			period,
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(actual, expected);

	let arg = tangram_index::usage::expire::Arg {
		hour_time_to_live: std::time::Duration::ZERO,
		..arg
	};
	index.expire_usage(arg.clone()).await.unwrap();
	let actual = index
		.get_usage(
			&account,
			period,
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(actual, expected);

	let now = jiff::Timestamp::new(24 * 60 * 60, 0).unwrap();
	aggregate(&index, now).await;
	let arg = tangram_index::usage::expire::Arg { now, ..arg };
	index.expire_usage(arg).await.unwrap();
	let error = index
		.get_usage(
			&account,
			period,
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap_err();
	assert!(error.to_string().contains("usage is unavailable"));
	let day = tangram_index::usage::Period::day("1970-01-01").unwrap();
	let actual = index
		.get_usage(&account, day, now, crate::tests::USAGE_DELTA_TIME_TO_LIVE)
		.await
		.unwrap();
	assert_eq!(actual.object_count, 24);
}

#[tokio::test]
async fn expire_usage_preserves_a_zero_storage_checkpoint() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	add_delta(
		&index,
		&account,
		2 * 60 * 60 + 1,
		tangram_index::usage::DeltaKind::ObjectCount,
		-1,
	);
	let now = jiff::Timestamp::new(4 * 60 * 60, 0).unwrap();
	let retained = std::time::Duration::new(365 * 86_400, 0);
	let arg = tangram_index::usage::expire::Arg {
		batch_size: 1_000,
		day_time_to_live: retained,
		delta_time_to_live: std::time::Duration::ZERO,
		hour_time_to_live: retained,
		month_time_to_live: retained,
		now,
		partition_end: 1,
		partition_start: 0,
		week_time_to_live: retained,
	};
	index.expire_usage(arg).await.unwrap();

	let usage = index
		.get_usage(
			&account,
			hour(3 * 60 * 60),
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(usage, tangram_index::usage::Aggregate::default());
}

#[tokio::test]
async fn get_current_usage_does_not_queue_future_aggregation() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	let now = jiff::Timestamp::new(1, 0).unwrap();
	let usage = index
		.get_usage(
			&account,
			hour(0),
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(usage.object_count, 1);

	let transaction = index.env.write_txn().unwrap();
	let next = Index::contains_usage_aggregation_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&account,
		60 * 60,
		0,
	)
	.unwrap();
	let day = tangram_index::usage::Period::day("1970-01-01").unwrap();
	let closing_hour = tangram_index::usage::closing_hour(day).unwrap();
	let closing = Index::contains_usage_aggregation_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&account,
		closing_hour,
		0,
	)
	.unwrap();
	assert!(!next);
	assert!(!closing);
}

#[tokio::test]
async fn records_compute_once_when_a_sandbox_is_destroyed() {
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let account = tangram_index::usage::Account::User(user.clone());
	let owner = tg::Principal::User(user);
	let sandbox = tg::sandbox::Id::new();
	let started = tangram_index::sandbox::put::Arg {
		account: Some(account.clone()),
		created_at: 1,
		data: Some(sandbox_data(
			sandbox.clone(),
			owner.clone(),
			tg::sandbox::Status::Started,
			None,
		)),
		id: sandbox.clone(),
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Sandbox(sandbox.clone()),
		processes: None,
		runner: None,
		touched_at: 1,
	};
	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(started)],
		})
		.await
		.unwrap()
		.unwrap();
	let destroyed = tangram_index::sandbox::put::Arg {
		account: Some(account.clone()),
		created_at: 1,
		data: Some(sandbox_data(
			sandbox.clone(),
			owner,
			tg::sandbox::Status::Destroyed,
			Some(tg::sandbox::Usage {
				cpu: tg::sandbox::ResourceUsage {
					consumption: 123,
					reservation: 789,
				},
				memory: tg::sandbox::ResourceUsage {
					consumption: 456,
					reservation: 0,
				},
				storage: tg::sandbox::ResourceUsage::default(),
			}),
		)),
		id: sandbox.clone(),
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Sandbox(sandbox.clone()),
		processes: None,
		runner: None,
		touched_at: 2,
	};
	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(destroyed.clone())],
		})
		.await
		.unwrap()
		.unwrap();
	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(destroyed)],
		})
		.await
		.unwrap()
		.unwrap();

	let now = jiff::Timestamp::new(60 * 60, 0).unwrap();
	let usage = index
		.get_usage(
			&account,
			hour(0),
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(usage.sandbox_cpu_consumption, 123);
	assert_eq!(usage.sandbox_cpu_reservation, 789);
	assert_eq!(usage.sandbox_memory_consumption, 456);
	assert_eq!(usage.sandbox_count, 1);
}

#[tokio::test]
async fn records_compute_once_when_destroy_precedes_start() {
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let account = tangram_index::usage::Account::User(user.clone());
	let owner = tg::Principal::User(user);
	let sandbox = tg::sandbox::Id::new();
	for status in [
		tg::sandbox::Status::Destroyed,
		tg::sandbox::Status::Started,
		tg::sandbox::Status::Destroyed,
	] {
		let usage = status.is_destroyed().then_some(tg::sandbox::Usage {
			cpu: tg::sandbox::ResourceUsage {
				consumption: 123,
				reservation: 789,
			},
			memory: tg::sandbox::ResourceUsage {
				consumption: 456,
				reservation: 0,
			},
			storage: tg::sandbox::ResourceUsage::default(),
		});
		let arg = tangram_index::sandbox::put::Arg {
			account: Some(account.clone()),
			created_at: 1,
			data: Some(sandbox_data(sandbox.clone(), owner.clone(), status, usage)),
			id: sandbox.clone(),
			location: None,
			permissions: Vec::new(),
			principal: tg::Principal::Sandbox(sandbox.clone()),
			processes: None,
			runner: None,
			touched_at: 2,
		};
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(arg)],
		};
		index.batch(arg).await.unwrap().unwrap();
	}
	let now = jiff::Timestamp::new(60 * 60, 0).unwrap();
	let usage = index
		.get_usage(
			&account,
			hour(0),
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(usage.sandbox_cpu_consumption, 123);
	assert_eq!(usage.sandbox_cpu_reservation, 789);
	assert_eq!(usage.sandbox_memory_consumption, 456);
	assert_eq!(usage.sandbox_count, 1);
}

#[tokio::test]
async fn does_not_record_compute_without_a_destroyed_sandbox_account() {
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let account = tangram_index::usage::Account::User(user.clone());
	let owner = tg::Principal::User(user);
	let sandbox = tg::sandbox::Id::new();
	let started = tangram_index::sandbox::put::Arg {
		account: Some(account.clone()),
		created_at: 1,
		data: Some(sandbox_data(
			sandbox.clone(),
			owner.clone(),
			tg::sandbox::Status::Started,
			None,
		)),
		id: sandbox.clone(),
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Sandbox(sandbox.clone()),
		processes: None,
		runner: None,
		touched_at: 1,
	};
	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(started)],
		})
		.await
		.unwrap()
		.unwrap();
	let destroyed = tangram_index::sandbox::put::Arg {
		account: None,
		created_at: 1,
		data: Some(sandbox_data(
			sandbox.clone(),
			owner,
			tg::sandbox::Status::Destroyed,
			Some(tg::sandbox::Usage {
				cpu: tg::sandbox::ResourceUsage {
					consumption: 123,
					reservation: 789,
				},
				memory: tg::sandbox::ResourceUsage {
					consumption: 456,
					reservation: 0,
				},
				storage: tg::sandbox::ResourceUsage::default(),
			}),
		)),
		id: sandbox.clone(),
		location: None,
		permissions: Vec::new(),
		principal: tg::Principal::Sandbox(sandbox.clone()),
		processes: None,
		runner: None,
		touched_at: 2,
	};
	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutSandbox(destroyed)],
		})
		.await
		.unwrap()
		.unwrap();

	let now = jiff::Timestamp::new(60 * 60, 0).unwrap();
	let usage = index
		.get_usage(
			&account,
			hour(0),
			now,
			crate::tests::USAGE_DELTA_TIME_TO_LIVE,
		)
		.await
		.unwrap();
	assert_eq!(usage, tangram_index::usage::Aggregate::default());
}

const HOUR: i64 = 60 * 60;

/// The time to live configuration used by the usage expiration CLI test.
fn expire_arg(now: jiff::Timestamp) -> tangram_index::usage::expire::Arg {
	tangram_index::usage::expire::Arg {
		batch_size: 1_000,
		day_time_to_live: std::time::Duration::from_hours(744),
		delta_time_to_live: std::time::Duration::from_hours(1),
		hour_time_to_live: std::time::Duration::from_hours(24),
		month_time_to_live: std::time::Duration::from_hours(744),
		now,
		partition_end: 1,
		partition_start: 0,
		week_time_to_live: std::time::Duration::from_hours(168),
	}
}

async fn expire(index: &Index, now: jiff::Timestamp) {
	for _ in 0..16 {
		let output = index.expire_usage(expire_arg(now)).await.unwrap();
		if output.done {
			break;
		}
	}
}

fn at(seconds: i64) -> jiff::Timestamp {
	jiff::Timestamp::new(seconds, 0).unwrap()
}

/// Read a period's usage, or `None` when it is unavailable.
async fn usage(
	index: &Index,
	account: &tangram_index::usage::Account,
	period: tangram_index::usage::Period,
	now: jiff::Timestamp,
) -> Option<tangram_index::usage::Aggregate> {
	match index
		.get_usage(account, period, now, crate::tests::USAGE_DELTA_TIME_TO_LIVE)
		.await
	{
		Ok(usage) => Some(usage),
		Err(error) if error.to_string().contains("usage is unavailable") => None,
		Err(error) => panic!("failed to get the usage: {error}"),
	}
}

/// Aggregate and expire once per hour from `start` through `end`, as a server polling in real time would.
async fn step_hourly(index: &Index, start: i64, end: i64) {
	for hour in start..=end {
		let now = at(hour * HOUR);
		aggregate(index, now).await;
		expire(index, now).await;
	}
}

fn day() -> tangram_index::usage::Period {
	tangram_index::usage::Period::day("1970-01-01").unwrap()
}

#[tokio::test]
async fn expiration_before_aggregation_after_a_jump_matches_incremental_aggregation() {
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let now = at(25 * HOUR);

	// Aggregate and expire hourly, as the reference.
	let (_reference_dir, reference) = new_index();
	add_delta(
		&reference,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectCount,
		2,
	);
	step_hourly(&reference, 1, 25).await;
	let expected = usage(&reference, &account, day(), now).await.unwrap();
	assert_eq!(expected.object_count, 48);

	// Jump past the delta and hour time to live, and expire before aggregating.
	let (_dir, index) = new_index();
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectCount,
		2,
	);
	expire(&index, now).await;
	aggregate(&index, now).await;
	expire(&index, now).await;
	let actual = usage(&index, &account, day(), now).await.unwrap();
	assert_eq!(actual, expected);
}

#[tokio::test]
async fn aggregation_before_expiration_after_a_jump_matches_incremental_aggregation() {
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let now = at(25 * HOUR);

	// Aggregate and expire hourly, as the reference.
	let (_reference_dir, reference) = new_index();
	add_delta(
		&reference,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectCount,
		2,
	);
	step_hourly(&reference, 1, 25).await;
	let expected = usage(&reference, &account, day(), now).await.unwrap();

	// Jump past the delta and hour time to live, and aggregate before expiring.
	let (_dir, index) = new_index();
	add_delta(
		&index,
		&account,
		1,
		tangram_index::usage::DeltaKind::ObjectCount,
		2,
	);
	aggregate(&index, now).await;
	expire(&index, now).await;
	let actual = usage(&index, &account, day(), now).await.unwrap();
	assert_eq!(actual, expected);
}

#[tokio::test]
async fn a_late_delta_within_the_delta_time_to_live_is_counted() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let now = at(3 * HOUR + HOUR / 2);

	// Aggregate and expire an hour, then write a late delta for it before its deltas may expire.
	add_delta(
		&index,
		&account,
		2 * HOUR + 1,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	aggregate(&index, now).await;
	expire(&index, now).await;
	add_delta(
		&index,
		&account,
		2 * HOUR + 2,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	aggregate(&index, now).await;
	let usage = usage(&index, &account, hour(2 * HOUR), now).await.unwrap();
	assert_eq!(usage.object_count, 2);
}

#[tokio::test]
async fn a_late_delta_after_its_hour_s_deltas_may_have_expired_is_dropped() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let now = at(4 * HOUR);

	// Aggregate and expire an hour.
	for second in [1, 2] {
		add_delta(
			&index,
			&account,
			2 * HOUR + second,
			tangram_index::usage::DeltaKind::ObjectCount,
			1,
		);
	}
	aggregate(&index, now).await;
	expire(&index, now).await;
	let expected = usage(&index, &account, hour(2 * HOUR), now).await.unwrap();
	assert_eq!(expected.object_count, 2);

	// Write a late delta for the hour, which must not replace its total with one that lacks the expired deltas.
	add_delta(
		&index,
		&account,
		2 * HOUR + 3,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	aggregate(&index, now).await;
	let actual = usage(&index, &account, hour(2 * HOUR), now).await.unwrap();
	assert_eq!(actual, expected);
}

#[tokio::test]
async fn a_late_delta_after_its_previous_hour_total_expired_is_dropped() {
	let (_dir, index) = new_index();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let now = at(27 * HOUR);

	// Aggregate and expire hourly past the hour time to live.
	for time in [1, 2 * HOUR + 1] {
		add_delta(
			&index,
			&account,
			time,
			tangram_index::usage::DeltaKind::ObjectCount,
			1,
		);
	}
	step_hourly(&index, 1, 27).await;
	let expected = usage(&index, &account, day(), now).await.unwrap();

	// Write a late delta for an hour whose previous hour total has expired, which must not change the day's total.
	add_delta(
		&index,
		&account,
		2 * HOUR + 1,
		tangram_index::usage::DeltaKind::ObjectCount,
		1,
	);
	aggregate(&index, now).await;
	let actual = usage(&index, &account, day(), now).await.unwrap();
	assert_eq!(actual, expected);
}
