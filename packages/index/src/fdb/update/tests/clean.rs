use {
	super::{Index, Key, Kind, directory, put, run},
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn cleans_versions_after_collecting_their_objects_and_processes() {
	run(async |index| {
		let object = directory(&[]);
		let object_id = object.id.clone();
		let process = crate::process::put::Arg {
			cached: false,
			children: None,
			command: object_id.clone(),
			data: None,
			error: None,
			id: tg::process::Id::new(),
			location: None,
			log: None,
			metadata: tg::process::Metadata::default(),
			options: tg::referent::Options::default(),
			output: None,
			parent: None,
			sandbox: None,
			storage: crate::process::Storage::default(),
			time_to_touch: std::time::Duration::ZERO,
			touched_at: 0,
		};
		let process_id = process.id.clone();
		let ids = [
			tg::Either::Left(object_id.clone()),
			tg::Either::Right(process_id.clone()),
		];
		let subject = tg::authorization::Subject::User(tg::user::Id::new());
		let kinds = [Kind::Grant(subject), Kind::Node];
		let arg = crate::batch::Arg {
			items: vec![
				crate::batch::Item::PutObject(object),
				crate::batch::Item::PutProcess(process),
			],
		};
		index.batch(arg).await.unwrap();
		for id in &ids {
			for kind in &kinds {
				enqueue(index, id, kind).await;
			}
		}
		drain(index).await;
		for id in &ids {
			for kind in &kinds {
				assert!(version(index, id, kind).await.is_some());
			}
		}
		for _ in 0..100 {
			let before = count_clean_entries(index).await;
			let output = index.clean(clean_arg(1, 0, 2)).await.unwrap();
			let deleted = output.objects.len() + output.processes.len();
			assert!(before - count_clean_entries(index).await + deleted <= 1);
			if output.done {
				break;
			}
		}
		assert!(index.try_get_objects(&[object_id]).await.unwrap()[0].is_none());
		assert!(index.try_get_processes(&[process_id]).await.unwrap()[0].is_none());
		assert_eq!(count_versions(index).await, 0);

		// Stale updates must not leave orphaned versions after their objects and processes are gone.
		for id in &ids {
			for kind in &kinds {
				enqueue(index, id, kind).await;
			}
		}
		drain(index).await;
		for _ in 0..100 {
			if index.clean(clean_arg(1, 0, 2)).await.unwrap().done {
				break;
			}
		}
		assert_eq!(count_versions(index).await, 0);
		assert_eq!(count_clean_entries(index).await, 0);
	})
	.await;
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn cleans_versions_without_collecting_the_objects() {
	run(async |index| {
		let mut object = directory(&[]);
		object.touched_at = 100;
		let id = tg::Either::Left(object.id.clone());
		put(index, vec![object]).await;
		let subjects = [
			tg::authorization::Subject::Process(tg::process::Id::new()),
			tg::authorization::Subject::Process(tg::process::Id::new()),
		];
		for subject in subjects {
			enqueue(index, &id, &Kind::Grant(subject)).await;
		}
		drain(index).await;
		assert_eq!(count_versions(index).await, 3);
		let mut done = false;
		for _ in 0..20 {
			let before = count_versions(index).await;
			let clean_before = count_clean_entries(index).await;
			let output = index.clean(clean_arg(1, 0, 2)).await.unwrap();
			let after = count_versions(index).await;
			assert!(before - after <= 1);
			assert!(clean_before - count_clean_entries(index).await <= 1);
			if output.done {
				done = true;
				break;
			}
		}
		assert!(done);
		assert_eq!(count_versions(index).await, 0);
		assert_eq!(count_clean_entries(index).await, 0);
		let object = id.left().unwrap();
		assert!(index.try_get_objects(&[object]).await.unwrap()[0].is_some());
	})
	.await;
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn cleaning_respects_other_partitions_and_newer_versions() {
	run(async |index| {
		let mut object = directory(&[]);
		object.touched_at = 100;
		let missing = tg::object::Id::new(tg::object::Kind::Directory, &vec![0].into());
		let mut blocker = directory(&[("missing", missing)]);
		blocker.touched_at = 100;
		let partition = Index::partition_for_id(object.id.to_bytes().as_ref(), 2);
		let other_partition = 1 - partition;
		let id = tg::Either::Left(object.id.clone());
		let blocker_id = tg::Either::Left(blocker.id.clone());
		put(index, vec![object, blocker]).await;
		drain(index).await;
		index.clean(clean_arg(100, 0, 2)).await.unwrap();
		let subject = tg::authorization::Subject::User(tg::user::Id::new());
		for (kind, queue) in [
			(Kind::Grant(subject), crate::update::Kind::Grant),
			(Kind::Node, crate::update::Kind::Node),
		] {
			enqueue(index, &id, &kind).await;
			drain(index).await;
			let previous = version(index, &id, &kind).await.unwrap();
			enqueue(index, &blocker_id, &kind).await;
			assign(index, &blocker_id, &kind, queue, other_partition).await;
			enqueue(index, &id, &kind).await;
			assign(index, &id, &kind, queue, partition).await;
			index
				.update_batch(queue, 100, partition, partition + 1)
				.await
				.unwrap();
			let expected = version(index, &id, &kind).await.unwrap();

			// Recreate a stale cleanup entry to verify that it cannot delete a newer version.
			crate::fdb::run(&index.database, |txn| {
				let id = &id;
				let kind = &kind;
				let previous = &previous;
				async move {
					let key = crate::fdb::Key::Update(Key::Clean {
						id: id.clone(),
						kind: kind.clone(),
						partition,
						version: previous.clone(),
					});
					txn.set(&Index::pack(&index.subspace, &key), &[]);
					Ok(ControlFlow::Break(()))
				}
			})
			.await
			.unwrap();

			// The old cleanup entry is eligible, but the current version is still needed.
			let output = index
				.clean(clean_arg(100, partition, partition + 1))
				.await
				.unwrap();
			assert!(!output.done);
			assert_eq!(version(index, &id, &kind).await, Some(expected.clone()));
			let output = index
				.clean(clean_arg(100, partition, partition + 1))
				.await
				.unwrap();
			assert!(
				output.done,
				"blocked versions must not keep the cleaner busy"
			);

			// An update at exactly the recorded version also prevents its collection.
			drain(index).await;
			crate::fdb::run(&index.database, |txn| {
				let id = &blocker_id;
				let kind = &kind;
				let expected = &expected;
				async move {
					Index::enqueue_update_propagate(&txn, &index.subspace, id, kind, expected, 2)
						.await
				}
			})
			.await
			.unwrap();
			index.clean(clean_arg(100, 0, 2)).await.unwrap();
			assert_eq!(version(index, &id, &kind).await, Some(expected));

			// Collection is restricted to the cleaner's assigned partitions.
			drain(index).await;
			index
				.clean(clean_arg(100, other_partition, other_partition + 1))
				.await
				.unwrap();
			assert!(version(index, &id, &kind).await.is_some());
			index
				.clean(clean_arg(100, partition, partition + 1))
				.await
				.unwrap();
			assert!(version(index, &id, &kind).await.is_none());
		}
	})
	.await;
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn concurrent_propagation_conflicts_with_cleaning() {
	run(async |index| {
		let mut object = directory(&[]);
		object.touched_at = 100;
		let id = tg::Either::Left(object.id.clone());
		put(index, vec![object]).await;
		drain(index).await;
		let transaction = index.database.create_trx().unwrap();
		let transaction = crate::fdb::Transaction::new(transaction);
		let arg = crate::fdb::clean::TransactionArg {
			batch_size: 100,
			max_object_touched_at: 0,
			max_process_touched_at: 0,
			max_sandbox_touched_at: 0,
			now: 0,
			partition_end: 2,
			partition_start: 0,
			partition_total: 2,
			subspace: &index.subspace,
			txn: &transaction,
			usage_partition_total: 1,
		};
		let ControlFlow::Break(output) = Index::clean_with_transaction(arg).await.unwrap() else {
			panic!("the cleaning transaction failed");
		};
		assert!(!output.done);
		enqueue(index, &id, &Kind::Node).await;
		drain(index).await;
		let expected = version(index, &id, &Kind::Node).await.unwrap();
		assert!(transaction.take().unwrap().commit().await.is_err());
		assert_eq!(version(index, &id, &Kind::Node).await, Some(expected));
		index.clean(clean_arg(100, 0, 2)).await.unwrap();
		assert!(version(index, &id, &Kind::Node).await.is_none());
	})
	.await;
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn unrelated_queue_progress_does_not_conflict_with_cleaning() {
	run(async |index| {
		let mut object = directory(&[]);
		object.touched_at = 100;
		let id = tg::Either::Left(object.id.clone());
		let missing = tg::object::Id::new(tg::object::Kind::Directory, &vec![0].into());
		let mut unrelated = directory(&[("missing", missing)]);
		unrelated.touched_at = 100;
		let unrelated_id = tg::Either::Left(unrelated.id.clone());
		put(index, vec![object, unrelated]).await;
		drain(index).await;
		let subject = tg::authorization::Subject::User(tg::user::Id::new());
		for (kind, queue) in [
			(Kind::Grant(subject), crate::update::Kind::Grant),
			(Kind::Node, crate::update::Kind::Node),
		] {
			index.clean(clean_arg(100, 0, 2)).await.unwrap();
			enqueue(index, &id, &kind).await;
			drain(index).await;
			enqueue(index, &unrelated_id, &kind).await;

			// Pause cleanup after it has read the queue heads and selected this item's version.
			let transaction = index.database.create_trx().unwrap();
			let transaction = crate::fdb::Transaction::new(transaction);
			let arg = crate::fdb::clean::TransactionArg {
				batch_size: 100,
				max_object_touched_at: 0,
				max_process_touched_at: 0,
				max_sandbox_touched_at: 0,
				now: 0,
				partition_end: 2,
				partition_start: 0,
				partition_total: 2,
				subspace: &index.subspace,
				txn: &transaction,
				usage_partition_total: 1,
			};
			let ControlFlow::Break(output) = Index::clean_with_transaction(arg).await.unwrap()
			else {
				panic!("the cleaning transaction failed");
			};
			assert!(!output.done);

			// Consuming another item's queue entry must not invalidate the cleanup transaction.
			assert_eq!(index.update_batch(queue, 100, 0, 2).await.unwrap().count, 1);
			transaction.take().unwrap().commit().await.unwrap();
			assert!(version(index, &id, &kind).await.is_none());
		}
	})
	.await;
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn replacing_propagated_versions_replaces_cleanup_entries() {
	run(async |index| {
		let mut object = directory(&[]);
		object.touched_at = 100;
		let id = tg::Either::Left(object.id.clone());
		put(index, vec![object]).await;
		drain(index).await;
		let subject = tg::authorization::Subject::User(tg::user::Id::new());
		for kind in [Kind::Grant(subject), Kind::Node] {
			enqueue(index, &id, &kind).await;
			drain(index).await;
			let previous = version(index, &id, &kind).await.unwrap();
			for _ in 0..4 {
				enqueue(index, &id, &kind).await;
				drain(index).await;
				assert_eq!(
					count_clean_entries(index).await,
					count_versions(index).await
				);
			}

			// Lowering the propagated version must replace the cleanup entry as well.
			crate::fdb::run(&index.database, |txn| {
				let id = &id;
				let kind = &kind;
				let previous = &previous;
				async move {
					Index::enqueue_update_propagate(&txn, &index.subspace, id, kind, previous, 2)
						.await
				}
			})
			.await
			.unwrap();
			drain(index).await;
			assert_eq!(version(index, &id, &kind).await, Some(previous));
			assert_eq!(
				count_clean_entries(index).await,
				count_versions(index).await
			);
		}
		index.clean(clean_arg(100, 0, 2)).await.unwrap();
		assert_eq!(count_clean_entries(index).await, 0);
		assert_eq!(count_versions(index).await, 0);
	})
	.await;
}

async fn assign(
	index: &Index,
	id: &tg::Either<tg::object::Id, tg::process::Id>,
	kind: &Kind,
	queue: crate::update::Kind,
	partition: u64,
) {
	crate::fdb::run(&index.database, |txn| async move {
		let key_kind = super::super::update_version_key_kind(queue)
			.to_i32()
			.unwrap();
		let prefix = Index::pack(&index.subspace, &(key_kind,));
		let subspace = foundationdb_tuple::Subspace::from_bytes(prefix);
		let range = foundationdb::RangeOption::from(&subspace);
		let result = txn.get_range(&range, 1, false).await;
		for entry in crate::fdb::retry!(result) {
			let crate::fdb::Key::Update(Key::UpdateVersion {
				id: entry_id,
				version,
				..
			}) = Index::unpack(&index.subspace, entry.key())?
			else {
				unreachable!();
			};
			if entry_id != *id {
				continue;
			}
			txn.clear(entry.key());
			let key = crate::fdb::Key::Update(Key::UpdateVersion {
				id: id.clone(),
				kind: kind.clone(),
				partition,
				version,
			});
			txn.set(&Index::pack(&index.subspace, &key), &[]);
		}
		Ok(ControlFlow::Break(()))
	})
	.await
	.unwrap();
}

async fn version(
	index: &Index,
	id: &tg::Either<tg::object::Id, tg::process::Id>,
	kind: &Kind,
) -> Option<foundationdb_tuple::Versionstamp> {
	crate::fdb::run(&index.database, |txn| async move {
		Index::try_get_update_propagated_version(&txn, &index.subspace, id, kind).await
	})
	.await
	.unwrap()
}

async fn enqueue(index: &Index, id: &tg::Either<tg::object::Id, tg::process::Id>, kind: &Kind) {
	crate::fdb::run(&index.database, |txn| async move {
		Index::enqueue_update_with_kind(
			&txn,
			&index.subspace,
			id,
			kind,
			super::super::Source::Put,
			2,
		);
		Ok(ControlFlow::Break(()))
	})
	.await
	.unwrap();
}

async fn drain(index: &Index) {
	for _ in 0..100 {
		let mut count = 0;
		for kind in [crate::update::Kind::Grant, crate::update::Kind::Node] {
			count += index.update_batch(kind, 100, 0, 2).await.unwrap().count;
		}
		if count == 0 {
			return;
		}
	}
	panic!("the updates did not drain");
}

fn clean_arg(batch_size: usize, partition_start: u64, partition_end: u64) -> crate::clean::Arg {
	crate::clean::Arg {
		batch_size,
		max_object_touched_at: 0,
		max_process_touched_at: 0,
		max_sandbox_touched_at: 0,
		now: 0,
		partition_end,
		partition_start,
	}
}

async fn count_versions(index: &Index) -> usize {
	let kinds = [
		crate::fdb::Kind::GrantUpdatePropagatedVersion,
		crate::fdb::Kind::NodeUpdatePropagatedVersion,
	];
	count_keys(index, &kinds).await
}

async fn count_clean_entries(index: &Index) -> usize {
	let kinds = [
		crate::fdb::Kind::GrantUpdateClean,
		crate::fdb::Kind::NodeUpdateClean,
	];
	count_keys(index, &kinds).await
}

async fn count_keys(index: &Index, kinds: &[crate::fdb::Kind]) -> usize {
	crate::fdb::run(&index.database, |txn| async move {
		let mut count = 0;
		for kind in kinds {
			let prefix = Index::pack(&index.subspace, &(kind.to_i32().unwrap(),));
			let subspace = foundationdb_tuple::Subspace::from_bytes(prefix);
			let range = foundationdb::RangeOption::from(&subspace);
			let result = txn.get_range(&range, 1, false).await;
			count += crate::fdb::retry!(result).len();
		}
		Ok(ControlFlow::Break(count))
	})
	.await
	.unwrap()
}
