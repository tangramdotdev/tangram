use {
	super::{super::Index, new_index, new_index_with_usage_partition_total},
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

fn add_delta(
	index: &Index,
	account: &crate::usage::Account,
	at: i64,
	kind: crate::usage::DeltaKind,
	delta: i64,
) {
	add_delta_to_partition(index, account, at, kind, delta, 0);
}

fn add_delta_to_partition(
	index: &Index,
	account: &crate::usage::Account,
	at: i64,
	kind: crate::usage::DeltaKind,
	delta: i64,
	partition: u64,
) {
	let mut transaction = index.env.write_txn().unwrap();
	let entry = crate::usage::DeltaArg {
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
			.aggregate_usage(crate::usage::aggregate::Arg {
				batch_size: 1_000,
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

fn hour(start: i64) -> crate::usage::Period {
	crate::usage::Period::from_kind_and_start(crate::usage::PeriodKind::Hour, start).unwrap()
}

#[test]
fn usage_keys_round_trip() {
	let (_dir, index) = new_index();
	let account = crate::usage::Account::User(tg::user::Id::new());
	let mut keys = vec![
		crate::lmdb::usage::Key::Aggregation {
			account: account.clone(),
			hour: 60 * 60,
			partition: 4,
		},
		crate::lmdb::usage::Key::Started,
	];
	for (kind, start, value) in [
		(crate::usage::PeriodKind::Hour, 0, 0),
		(crate::usage::PeriodKind::Day, 0, 1),
		(crate::usage::PeriodKind::Week, -3 * 24 * 60 * 60, 2),
		(crate::usage::PeriodKind::Month, 0, 3),
	] {
		assert_eq!(kind as u8, value);
		let period = crate::usage::Period::from_kind_and_start(kind, start).unwrap();
		keys.push(crate::lmdb::usage::Key::Aggregate {
			account: account.clone(),
			partition: 3,
			period,
		});
		keys.push(crate::lmdb::usage::Key::Unavailable {
			account: account.clone(),
			kind,
			partition: 5,
		});
	}
	for (kind, value) in [
		(crate::usage::DeltaKind::ObjectCount, 0),
		(crate::usage::DeltaKind::ObjectSize, 1),
		(crate::usage::DeltaKind::ProcessCount, 2),
		(crate::usage::DeltaKind::SandboxCount, 3),
		(crate::usage::DeltaKind::SandboxCpu, 4),
		(crate::usage::DeltaKind::SandboxMemory, 5),
	] {
		assert_eq!(kind.to_i32(), Some(value));
		keys.push(crate::lmdb::usage::Key::Delta {
			account: account.clone(),
			hour: 2 * 60 * 60,
			kind,
			partition: 5,
		});
	}
	for expected in keys {
		let key = crate::lmdb::Key::Usage(expected.clone());
		let bytes = Index::pack(&index.subspace, &key);
		let crate::lmdb::Key::Usage(actual) = Index::unpack(&index.subspace, &bytes).unwrap()
		else {
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
		&crate::lmdb::Key::Usage(crate::lmdb::usage::Key::Started),
	);
	let mut transaction = index.env.write_txn().unwrap();
	index.db.delete(&mut transaction, &key).unwrap();
	transaction.commit().unwrap();

	let first = jiff::Timestamp::new(60 * 60, 0).unwrap();
	let second = jiff::Timestamp::new(2 * 60 * 60, 0).unwrap();
	index.start_usage(first).await.unwrap();
	index.start_usage(second).await.unwrap();

	let account = crate::usage::Account::User(tg::user::Id::new());
	let error = index
		.get_usage(&account, hour(0), second)
		.await
		.unwrap_err();
	assert!(error.to_string().contains("usage is unavailable"));
	let usage = index
		.get_usage(&account, hour(60 * 60), second)
		.await
		.unwrap();
	assert_eq!(usage, crate::usage::Aggregate::default());
}

#[tokio::test]
async fn usage_unavailability_is_scoped_by_account_and_partition() {
	let (_dir, index) = new_index_with_usage_partition_total(2);
	let account = crate::usage::Account::User(tg::user::Id::new());
	let other = crate::usage::Account::User(tg::user::Id::new());
	let period = hour(0);
	let mut transaction = index.env.write_txn().unwrap();
	let entry = crate::usage::PartitionAggregate {
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
	let entry = crate::usage::PartitionAggregate {
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
		.expire_usage(crate::usage::expire::Arg {
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

	let error = index.get_usage(&account, period, now).await.unwrap_err();
	assert!(error.to_string().contains("usage is unavailable"));
	let usage = index.get_usage(&other, period, now).await.unwrap();
	assert_eq!(usage, crate::usage::Aggregate::default());
	let mut transaction = index.env.write_txn().unwrap();
	let cutoff = Index::try_get_usage_unavailable_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&account,
		crate::usage::PeriodKind::Hour,
		0,
	)
	.unwrap();
	assert_eq!(cutoff, Some(60 * 60));
	let cutoff = Index::try_get_usage_unavailable_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&account,
		crate::usage::PeriodKind::Hour,
		1,
	)
	.unwrap();
	assert_eq!(cutoff, None);
	Index::mark_usage_unavailable_with_transaction(
		&index.db,
		&index.subspace,
		&mut transaction,
		&account,
		crate::usage::PeriodKind::Hour,
		0,
		0,
	)
	.unwrap();
	let cutoff = Index::try_get_usage_unavailable_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&account,
		crate::usage::PeriodKind::Hour,
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
	usage: Option<tg::sandbox::get::Usage>,
) -> tg::sandbox::get::Output {
	tg::sandbox::get::Output {
		cpu: Some(2),
		creator: Some(owner.clone()),
		hostname: None,
		id,
		isolation: None,
		location: None,
		memory: Some(1 << 30),
		mounts: Vec::new(),
		network: None,
		owner: Some(owner),
		status,
		tokens: tg::authorization::Tokens::default(),
		ttl: None,
		usage,
	}
}

#[tokio::test]
async fn aggregates_compute_and_carries_storage_across_empty_hours() {
	let (_dir, index) = new_index();
	let account = crate::usage::Account::User(tg::user::Id::new());
	add_delta(&index, &account, 1, crate::usage::DeltaKind::SandboxCpu, 3);
	add_delta(
		&index,
		&account,
		1,
		crate::usage::DeltaKind::SandboxMemory,
		5,
	);
	add_delta(&index, &account, 1, crate::usage::DeltaKind::ObjectCount, 1);
	add_delta(&index, &account, 1, crate::usage::DeltaKind::ObjectSize, 10);
	add_delta(
		&index,
		&account,
		1,
		crate::usage::DeltaKind::SandboxCount,
		1,
	);

	let now = jiff::Timestamp::new(3 * 60 * 60, 0).unwrap();
	let first = index.get_usage(&account, hour(0), now).await.unwrap();
	assert_eq!(first.sandbox_cpu, 3);
	assert_eq!(first.sandbox_memory, 5);
	assert_eq!(first.object_count, 1);
	assert_eq!(first.object_size, 10);
	assert_eq!(first.sandbox_count, 1);

	let period = crate::usage::Period::day("1970-01-01").unwrap();
	let during_third = jiff::Timestamp::new(2 * 60 * 60 + 1, 0).unwrap();
	let partial = index
		.get_usage(&account, period, during_third)
		.await
		.unwrap();
	assert_eq!(partial.sandbox_cpu, 3);
	assert_eq!(partial.sandbox_memory, 5);
	assert_eq!(partial.object_count, 3);
	assert_eq!(partial.object_size, 30);
	assert_eq!(partial.sandbox_count, 1);

	let third = index
		.get_usage(&account, hour(2 * 60 * 60), now)
		.await
		.unwrap();
	assert_eq!(third.sandbox_cpu, 0);
	assert_eq!(third.sandbox_memory, 0);
	assert_eq!(third.object_count, 1);
	assert_eq!(third.object_size, 10);
	assert_eq!(third.sandbox_count, 0);
}

#[tokio::test]
async fn aggregates_late_storage_deltas_into_future_hours() {
	let (_dir, index) = new_index();
	let account = crate::usage::Account::User(tg::user::Id::new());
	add_delta(&index, &account, 1, crate::usage::DeltaKind::ObjectCount, 1);
	let now = jiff::Timestamp::new(3 * 60 * 60, 0).unwrap();
	aggregate(&index, now).await;
	let period = hour(60 * 60);
	let usage = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(usage.object_count, 1);

	add_delta(&index, &account, 1, crate::usage::DeltaKind::ObjectCount, 1);
	aggregate(&index, now).await;
	let usage = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(usage.object_count, 2);
}

#[tokio::test]
async fn aggregates_hourly_storage_gauges_into_a_day() {
	let (_dir, index) = new_index();
	let account = crate::usage::Account::User(tg::user::Id::new());
	add_delta(&index, &account, 1, crate::usage::DeltaKind::SandboxCpu, 3);
	add_delta(&index, &account, 1, crate::usage::DeltaKind::ObjectCount, 1);
	add_delta(&index, &account, 1, crate::usage::DeltaKind::ObjectSize, 10);
	add_delta(
		&index,
		&account,
		2 * 60 * 60 + 1,
		crate::usage::DeltaKind::ObjectCount,
		-1,
	);
	add_delta(
		&index,
		&account,
		2 * 60 * 60 + 1,
		crate::usage::DeltaKind::ObjectSize,
		-10,
	);

	let period = crate::usage::Period::day("1970-01-01").unwrap();
	let now = jiff::Timestamp::new(24 * 60 * 60, 0).unwrap();
	let usage = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(usage.sandbox_cpu, 3);
	assert_eq!(usage.sandbox_memory, 0);
	assert_eq!(usage.object_count, 2);
	assert_eq!(usage.object_size, 20);
	assert_eq!(usage.process_count, 0);
	assert_eq!(usage.sandbox_count, 0);
}

#[tokio::test]
async fn aggregates_signed_storage_across_partitions() {
	let (_dir, index) = new_index_with_usage_partition_total(2);
	let account = crate::usage::Account::User(tg::user::Id::new());
	add_delta_to_partition(
		&index,
		&account,
		1,
		crate::usage::DeltaKind::ObjectCount,
		1,
		0,
	);
	add_delta_to_partition(
		&index,
		&account,
		2 * 60 * 60 + 1,
		crate::usage::DeltaKind::ObjectCount,
		-1,
		1,
	);

	let period = crate::usage::Period::day("1970-01-01").unwrap();
	let now = jiff::Timestamp::new(4 * 60 * 60, 0).unwrap();
	let usage = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(usage.object_count, 2);
}

#[tokio::test]
async fn aggregates_parent_periods_without_boundary_usage() {
	let (_dir, index) = new_index();
	let account = crate::usage::Account::User(tg::user::Id::new());
	add_delta(&index, &account, 1, crate::usage::DeltaKind::SandboxCpu, 3);
	let now = jiff::Timestamp::new(32 * 24 * 60 * 60, 0).unwrap();
	aggregate(&index, now).await;

	let transaction = index.env.write_txn().unwrap();
	for period in [
		crate::usage::Period::day("1970-01-01").unwrap(),
		crate::usage::Period::month("1970-01").unwrap(),
		crate::usage::Period::week("1970-W01").unwrap(),
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
		assert_eq!(aggregate.sandbox_cpu, 3);
	}
}

#[tokio::test]
async fn expire_usage_preserves_aggregates_before_deleting_deltas() {
	let (_dir, index) = new_index();
	let account = crate::usage::Account::User(tg::user::Id::new());
	add_delta(&index, &account, 1, crate::usage::DeltaKind::ObjectCount, 1);
	let now = jiff::Timestamp::new(2 * 60 * 60, 0).unwrap();
	let period = hour(0);
	let expected = index.get_usage(&account, period, now).await.unwrap();
	aggregate(&index, now).await;
	let retained = std::time::Duration::new(365 * 86_400, 0);
	let arg = crate::usage::expire::Arg {
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
	let actual = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(actual, expected);

	let arg = crate::usage::expire::Arg {
		hour_time_to_live: std::time::Duration::ZERO,
		..arg
	};
	index.expire_usage(arg.clone()).await.unwrap();
	let actual = index.get_usage(&account, period, now).await.unwrap();
	assert_eq!(actual, expected);

	let now = jiff::Timestamp::new(24 * 60 * 60, 0).unwrap();
	aggregate(&index, now).await;
	let arg = crate::usage::expire::Arg { now, ..arg };
	index.expire_usage(arg).await.unwrap();
	let error = index.get_usage(&account, period, now).await.unwrap_err();
	assert!(error.to_string().contains("usage is unavailable"));
	let day = crate::usage::Period::day("1970-01-01").unwrap();
	let actual = index.get_usage(&account, day, now).await.unwrap();
	assert_eq!(actual.object_count, 24);
}

#[tokio::test]
async fn expire_usage_preserves_a_zero_storage_checkpoint() {
	let (_dir, index) = new_index();
	let account = crate::usage::Account::User(tg::user::Id::new());
	add_delta(&index, &account, 1, crate::usage::DeltaKind::ObjectCount, 1);
	add_delta(
		&index,
		&account,
		2 * 60 * 60 + 1,
		crate::usage::DeltaKind::ObjectCount,
		-1,
	);
	let now = jiff::Timestamp::new(4 * 60 * 60, 0).unwrap();
	let retained = std::time::Duration::new(365 * 86_400, 0);
	let arg = crate::usage::expire::Arg {
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
		.get_usage(&account, hour(3 * 60 * 60), now)
		.await
		.unwrap();
	assert_eq!(usage, crate::usage::Aggregate::default());
}

#[tokio::test]
async fn get_current_usage_does_not_queue_future_aggregation() {
	let (_dir, index) = new_index();
	let account = crate::usage::Account::User(tg::user::Id::new());
	add_delta(&index, &account, 1, crate::usage::DeltaKind::ObjectCount, 1);
	let now = jiff::Timestamp::new(1, 0).unwrap();
	let usage = index.get_usage(&account, hour(0), now).await.unwrap();
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
	let day = crate::usage::Period::day("1970-01-01").unwrap();
	let closing_hour = crate::usage::closing_hour(day).unwrap();
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
	let account = crate::usage::Account::User(user.clone());
	let owner = tg::Principal::User(user);
	let sandbox = tg::sandbox::Id::new();
	let started = crate::sandbox::put::Arg {
		account: Some(account.clone()),
		created_at: 1,
		data: Some(sandbox_data(
			sandbox.clone(),
			owner.clone(),
			tg::sandbox::Status::Started,
			None,
		)),
		id: sandbox.clone(),
		runner: None,
		touched_at: 1,
	};
	index
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::PutSandbox(started)],
		})
		.await
		.unwrap();
	let destroyed = crate::sandbox::put::Arg {
		account: Some(account.clone()),
		created_at: 1,
		data: Some(sandbox_data(
			sandbox.clone(),
			owner,
			tg::sandbox::Status::Destroyed,
			Some(tg::sandbox::get::Usage {
				cpu: 123,
				memory: 456,
			}),
		)),
		id: sandbox,
		runner: None,
		touched_at: 2,
	};
	index
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::PutSandbox(destroyed.clone())],
		})
		.await
		.unwrap();
	index
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::PutSandbox(destroyed)],
		})
		.await
		.unwrap();

	let now = jiff::Timestamp::new(60 * 60, 0).unwrap();
	let usage = index.get_usage(&account, hour(0), now).await.unwrap();
	assert_eq!(usage.sandbox_cpu, 123);
	assert_eq!(usage.sandbox_memory, 456);
	assert_eq!(usage.sandbox_count, 1);
}

#[tokio::test]
async fn does_not_record_compute_without_a_destroyed_sandbox_account() {
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let account = crate::usage::Account::User(user.clone());
	let owner = tg::Principal::User(user);
	let sandbox = tg::sandbox::Id::new();
	let started = crate::sandbox::put::Arg {
		account: Some(account.clone()),
		created_at: 1,
		data: Some(sandbox_data(
			sandbox.clone(),
			owner.clone(),
			tg::sandbox::Status::Started,
			None,
		)),
		id: sandbox.clone(),
		runner: None,
		touched_at: 1,
	};
	index
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::PutSandbox(started)],
		})
		.await
		.unwrap();
	let destroyed = crate::sandbox::put::Arg {
		account: None,
		created_at: 1,
		data: Some(sandbox_data(
			sandbox.clone(),
			owner,
			tg::sandbox::Status::Destroyed,
			Some(tg::sandbox::get::Usage {
				cpu: 123,
				memory: 456,
			}),
		)),
		id: sandbox,
		runner: None,
		touched_at: 2,
	};
	index
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::PutSandbox(destroyed)],
		})
		.await
		.unwrap();

	let now = jiff::Timestamp::new(60 * 60, 0).unwrap();
	let usage = index.get_usage(&account, hour(0), now).await.unwrap();
	assert_eq!(usage, crate::usage::Aggregate::default());
}
