use {
	super::{Index, Key, Kind},
	crate::tests::{directory, put},
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

mod clean;
mod storage;

#[tokio::test]
async fn propagation_preserves_the_wait_version() {
	run(async |index| {
		// Establish a directory chain whose metadata is incomplete until the leaf arrives.
		let mut leaf = directory(&[]);
		let middle = directory(&[("leaf", leaf.id.clone())]);
		let top = directory(&[("middle", middle.id.clone())]);
		put(index, vec![top.clone(), middle]).await;
		drain(index).await;
		assert_eq!(count(index, &top.id).await, None);

		// Put the complete leaf.
		leaf.metadata.subtree = tg::object::metadata::Subtree {
			count: Some(1),
			depth: Some(1),
			size: Some(leaf.metadata.node.size),
			solvable: Some(false),
			solved: Some(true),
		};
		leaf.storage.insert(tg::object::storage::Set::SUBTREE);
		put(index, vec![leaf]).await;
		let cutoff = index.get_transaction_id().await.unwrap();

		// Waiting must include every ancestor until the top directory is complete.
		for _ in 0..100 {
			if count(index, &top.id).await.is_some() {
				break;
			}
			let oldest = index
				.try_get_oldest_update_transaction_id(
					tangram_index::update::Kind::StorageAndMetadata,
				)
				.await
				.unwrap();
			assert!(
				oldest.is_some_and(|version| version <= cutoff),
				"the wait cutoff lost its pending propagation: {oldest:?} > {cutoff}"
			);
			index
				.update_batch(tangram_index::update::Kind::StorageAndMetadata, 1, 0, 2)
				.await
				.unwrap();
		}
		assert_eq!(count(index, &top.id).await, Some(3));
		drain(index).await;
		let oldest = index
			.try_get_oldest_update_transaction_id(tangram_index::update::Kind::StorageAndMetadata)
			.await
			.unwrap();
		assert!(oldest.is_none());
	})
	.await;
}

#[tokio::test]
async fn older_propagation_through_a_completed_item_preserves_the_wait_version() {
	run(async |index| {
		// Establish a directory chain whose metadata is incomplete until the leaf arrives.
		let mut leaf = directory(&[]);
		let middle = directory(&[("leaf", leaf.id.clone())]);
		let top = directory(&[("middle", middle.id.clone())]);
		put(index, vec![top.clone(), middle.clone()]).await;
		drain(index).await;
		assert_eq!(count(index, &top.id).await, None);

		// Put the complete leaf.
		leaf.metadata.subtree = tg::object::metadata::Subtree {
			count: Some(1),
			depth: Some(1),
			size: Some(leaf.metadata.node.size),
			solvable: Some(false),
			solved: Some(true),
		};
		leaf.storage.insert(tg::object::storage::Set::SUBTREE);
		put(index, vec![leaf]).await;
		let cutoff = index.get_transaction_id().await.unwrap();

		// Enqueue a newer put of the middle directory in the partition without the leaf's update.
		let entries = queue(index).await;
		let [(leaf_partition, _)] = entries.as_slice() else {
			panic!("expected one queued update");
		};
		let partition = 1 - leaf_partition;
		crate::run(&index.database, async |txn| {
			let key = crate::Key::Update(Key::StorageAndMetadataUpdate {
				id: tg::Either::Left(middle.id.clone()),
				partition,
				source: super::StorageAndMetadataSource::Put,
				version: foundationdb_tuple::Versionstamp::incomplete(0),
			});
			let key = Index::pack_with_versionstamp(&index.subspace, &key);
			txn.atomic_op(
				&key,
				&[],
				foundationdb::options::MutationType::SetVersionstampedKey,
			);
			Ok(ControlFlow::Break(()))
		})
		.await
		.unwrap();

		// Update the middle directory before the older leaf update, which completes it with the newer version.
		index
			.update_batch(
				tangram_index::update::Kind::StorageAndMetadata,
				1,
				partition,
				partition + 1,
			)
			.await
			.unwrap();

		// Waiting must include every ancestor until the top directory is complete.
		for _ in 0..100 {
			if count(index, &top.id).await.is_some() {
				break;
			}
			let entries = queue(index).await;
			let Some((partition, version)) = entries
				.into_iter()
				.min_by(|(_, left), (_, right)| left.cmp(right))
			else {
				panic!("the top directory is incomplete without a pending update");
			};
			let transaction_id = u64::from_be_bytes(version.as_bytes()[..8].try_into().unwrap());
			assert!(
				transaction_id <= cutoff,
				"the wait cutoff lost its pending propagation: {transaction_id} > {cutoff}"
			);
			index
				.update_batch(
					tangram_index::update::Kind::StorageAndMetadata,
					1,
					partition,
					partition + 1,
				)
				.await
				.unwrap();
		}
		assert_eq!(count(index, &top.id).await, Some(3));
	})
	.await;
}

#[tokio::test]
async fn duplicate_updates_are_idempotent() {
	run(async |index| {
		// Put a directory whose child is complete.
		let mut leaf = directory(&[]);
		leaf.metadata.subtree = tg::object::metadata::Subtree {
			count: Some(1),
			depth: Some(1),
			size: Some(leaf.metadata.node.size),
			solvable: Some(false),
			solved: Some(true),
		};
		leaf.storage.insert(tg::object::storage::Set::SUBTREE);
		let parent = directory(&[("leaf", leaf.id.clone())]);
		put(index, vec![leaf, parent.clone()]).await;

		// Enqueue duplicate updates for the directory.
		let id = tg::Either::Left(parent.id.clone());
		for _ in 0..4 {
			crate::run(&index.database, async |txn| {
				Index::enqueue_update(
					txn,
					&index.subspace,
					&id,
					super::StorageAndMetadataSource::Put,
					2,
				);
				Ok(ControlFlow::Break(()))
			})
			.await
			.unwrap();
		}
		drain(index).await;
		assert_eq!(count(index, &parent.id).await, Some(2));
	})
	.await;
}

async fn run(test: impl AsyncFnOnce(&Index)) {
	let partition_totals = crate::PartitionTotals {
		cleaning: 2,
		permission_update: 2,
		storage_and_metadata_update: 2,
		usage_update: 2,
		usage: 1,
	};
	run_with_partition_totals(partition_totals, test).await;
}

async fn run_with_partition_totals(
	partition_totals: crate::PartitionTotals,
	test: impl AsyncFnOnce(&Index),
) {
	let Some(cluster) = tangram_tests_util::database::cluster() else {
		return;
	};
	let options = crate::Options {
		verification: crate::VerificationConfig { concurrency: 1 },
		cleaning_partition_total: partition_totals.cleaning,
		cluster,
		permission_update_partition_total: partition_totals.permission_update,
		process_data_fragment_size: 16,
		process_permission_time_to_touch: std::time::Duration::ZERO,
		instance: Some(format!(
			"index_update_test_{:032x}/",
			rand::random::<u128>()
		)),
		max_process_depth: None,
		object_permission_time_to_touch: std::time::Duration::ZERO,
		storage_and_metadata_update_partition_total: partition_totals.storage_and_metadata_update,
		read_channel_capacity: 256,
		read_request_batch_size: 1,
		read_task_concurrency: 1,
		read_task_count: 4,
		storage_relation_batch_size: 8,
		usage_update_partition_total: partition_totals.usage_update,
		usage_partition_total: partition_totals.usage,
		max_write_operation_batch_size: 1024,
		write_item_transaction_concurrency: 1,
		write_operation_batch_size: 1024,
		write_transaction_concurrency: 1,
	};
	crate::tests::run(&options, test).await;
}

async fn count(index: &Index, id: &tg::object::Id) -> Option<u64> {
	index
		.try_get_objects(std::slice::from_ref(id))
		.await
		.unwrap()[0]
		.as_ref()
		.unwrap()
		.metadata
		.subtree
		.count
}

async fn drain(index: &Index) {
	for _ in 0..100 {
		if index
			.update_batch(tangram_index::update::Kind::StorageAndMetadata, 1024, 0, 2)
			.await
			.unwrap()
			.count == 0
		{
			return;
		}
	}
	panic!("the storage and metadata updates did not drain");
}

async fn queue(index: &Index) -> Vec<(u64, foundationdb_tuple::Versionstamp)> {
	crate::run(&index.database, async |txn| {
		let key_kind = crate::Kind::StorageAndMetadataUpdateVersion
			.to_i32()
			.unwrap();
		let prefix = Index::pack(&index.subspace, &(key_kind,));
		let range =
			foundationdb::RangeOption::from(&foundationdb_tuple::Subspace::from_bytes(prefix));
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::retry!(result);
		let mut output = Vec::new();
		for entry in entries {
			let crate::Key::Update(Key::StorageAndMetadataUpdate {
				partition, version, ..
			}) = Index::unpack(&index.subspace, entry.key())?
			else {
				return Err(tg::error!("unexpected key type"));
			};
			output.push((partition, version));
		}
		Ok(ControlFlow::Break(output))
	})
	.await
	.unwrap()
}
