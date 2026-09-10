use {
	super::{Index, Key, Kind},
	foundationdb as fdb,
	futures::FutureExt as _,
	std::{collections::BTreeMap, ops::ControlFlow, panic::AssertUnwindSafe, sync::OnceLock},
	tangram_client::prelude::*,
};

mod clean;
mod storage;

static NETWORK: OnceLock<fdb::api::NetworkAutoStop> = OnceLock::new();

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn coalesced_updates_preserve_the_oldest_version() {
	run(async |index| reproduce(index, false).await).await;
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn late_updates_preserve_the_oldest_version() {
	run(async |index| reproduce(index, true).await).await;
}

async fn run(test: impl AsyncFnOnce(&Index)) {
	let partition_totals = crate::fdb::PartitionTotals {
		cleaning: 2,
		grant_update: 2,
		log_compaction: 2,
		node_update: 2,
		storage_update: 2,
		usage: 1,
	};
	run_with_partition_totals(partition_totals, test).await;
}

async fn run_with_partition_totals(
	partition_totals: crate::fdb::PartitionTotals,
	test: impl AsyncFnOnce(&Index),
) {
	NETWORK.get_or_init(|| {
		// SAFETY: The network is initialized once and outlives every test database and runtime.
		unsafe { fdb::boot() }
	});
	let options = crate::fdb::Options {
		authorize: crate::fdb::AuthorizeConfig { concurrency: 1 },
		cleaning_partition_total: partition_totals.cleaning,
		cluster: std::env::var_os("FDB_CLUSTER_FILE")
			.expect("set FDB_CLUSTER_FILE")
			.into(),
		grant_update_partition_total: partition_totals.grant_update,
		instance: Some(format!(
			"index_update_test_{:032x}/",
			rand::random::<u128>()
		)),
		log_compaction_partition_total: partition_totals.log_compaction,
		max_process_depth: None,
		node_update_partition_total: partition_totals.node_update,
		read_request_batch_size: 1,
		read_transaction_concurrency: 1,
		storage_update_partition_total: partition_totals.storage_update,
		usage_partition_total: partition_totals.usage,
		write_operation_batch_size: 1024,
		write_transaction_concurrency: 1,
	};
	let index = Index::new(&options).unwrap();
	let result = AssertUnwindSafe(test(&index)).catch_unwind().await;
	crate::fdb::run(&index.database, |txn| {
		let (begin, end) = index.subspace.range();
		async move {
			txn.clear_range(&begin, &end);
			Ok(ControlFlow::Break(()))
		}
	})
	.await
	.unwrap();
	if let Err(error) = result {
		std::panic::resume_unwind(error);
	}
}

async fn reproduce(index: &Index, late: bool) {
	// Establish a directory chain whose metadata is incomplete until the leaf arrives.
	let mut leaf = directory(&[]);
	let mut middle = directory(&[("leaf", leaf.id.clone())]);
	let top = directory(&[("middle", middle.id.clone())]);
	put(index, vec![top.clone(), middle.clone()]).await;
	drain(index).await;
	assert_eq!(count(index, &top.id).await, None);
	leaf.metadata.subtree = tg::object::metadata::Subtree {
		count: Some(1),
		depth: Some(1),
		size: Some(leaf.metadata.node.size),
		solvable: Some(false),
		solved: Some(true),
	};
	leaf.storage.subtree = true;
	put(index, vec![leaf.clone()]).await;
	if !late {
		step(index).await;
	}
	let cutoff = index.get_transaction_id().await.unwrap();
	let entries = queue(index).await;
	let Key::UpdateVersion { version, .. } = &entries[0] else {
		unreachable!()
	};
	let version = version.clone();

	// Place the newer middle update before the older work in the partition scan order.
	let old = if late { &leaf.id } else { &middle.id };
	partition(index, old, 1).await;
	middle.metadata.subtree = tg::object::metadata::Subtree {
		count: Some(2),
		depth: Some(2),
		size: Some(middle.metadata.node.size + leaf.metadata.node.size),
		solvable: Some(false),
		solved: Some(true),
	};
	put(index, vec![middle.clone()]).await;
	partition_newest(index, &middle.id, 0).await;
	step(index).await;
	assert_eq!(count(index, &middle.id).await, Some(2));
	partition(index, &top.id, 1).await;
	if late {
		step(index).await;
	}
	step(index).await;

	// Waiting must still include the top directory even though its metadata is unchanged locally.
	let oldest = index
		.try_get_oldest_update_transaction_id(crate::update::Kind::Node)
		.await
		.unwrap();
	assert!(
		oldest.is_some_and(|version| version <= cutoff),
		"the wait cutoff lost its pending propagation: {oldest:?} > {cutoff}"
	);
	assert_eq!(count(index, &top.id).await, None);
	drain(index).await;
	assert_eq!(count(index, &top.id).await, Some(3));

	// Repeating an already covered propagation must not walk the ancestors again.
	crate::fdb::run(&index.database, |txn| {
		let id = tg::Either::Left(middle.id.clone());
		let version = &version;
		async move {
			Index::enqueue_update_propagate(&txn, &index.subspace, &id, &Kind::Node, version, 2)
				.await
		}
	})
	.await
	.unwrap();
	step(index).await;
	let entries = queue(index).await;
	assert!(entries.is_empty());

	// Collection must remove the retained versions along with their objects.
	let arg = crate::clean::Arg {
		batch_size: 100,
		max_object_touched_at: 0,
		max_process_touched_at: 0,
		max_sandbox_touched_at: 0,
		now: 0,
		partition_end: 2,
		partition_start: 0,
	};
	let mut done = false;
	for _ in 0..10 {
		if index.clean(arg.clone()).await.unwrap().done {
			done = true;
			break;
		}
	}
	assert!(done);
	let ids = [leaf.id, middle.id, top.id];
	assert!(
		index
			.try_get_objects(&ids)
			.await
			.unwrap()
			.iter()
			.all(Option::is_none)
	);
	crate::fdb::run(&index.database, |txn| {
		let ids = &ids;
		async move {
			for id in ids {
				let id = tg::Either::Left(id.clone());
				let version = crate::fdb::propagate!(
					Index::try_get_update_propagated_version(
						&txn,
						&index.subspace,
						&id,
						&Kind::Node
					)
					.await
				);
				assert!(version.is_none());
			}
			Ok(ControlFlow::Break(()))
		}
	})
	.await
	.unwrap();
}

fn directory(children: &[(&str, tg::object::Id)]) -> crate::object::put::Arg {
	let entries = children
		.iter()
		.map(|(name, id)| ((*name).to_owned(), id.to_string()))
		.collect::<BTreeMap<_, _>>();
	let data: tg::directory::Data =
		serde_json::from_value(serde_json::json!({ "entries": entries })).unwrap();
	let bytes = data.serialize().unwrap();
	let id = tg::object::Id::new(tg::object::Kind::Directory, &bytes);
	let metadata = tg::object::Metadata {
		node: tg::object::metadata::Node {
			size: bytes.len() as u64,
			solvable: false,
			solved: true,
		},
		subtree: tg::object::metadata::Subtree::default(),
	};
	crate::object::put::Arg {
		checkout: None,
		children: children.iter().map(|(_, id)| id.clone()).collect(),
		id,
		metadata,
		put: [1; 16],
		storage: crate::object::Storage::default(),
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	}
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

async fn put(index: &Index, objects: Vec<crate::object::put::Arg>) {
	let items = objects
		.into_iter()
		.map(crate::batch::Item::PutObject)
		.collect();
	let arg = crate::batch::Arg { items };
	index.batch(arg).await.unwrap();
}

async fn drain(index: &Index) {
	for _ in 0..100 {
		if index
			.update_batch(crate::update::Kind::Node, 1024, 0, 2)
			.await
			.unwrap()
			.count == 0
		{
			return;
		}
	}
	panic!("the node updates did not drain");
}

async fn step(index: &Index) {
	assert_eq!(
		index
			.update_batch(crate::update::Kind::Node, 1, 0, 2)
			.await
			.unwrap()
			.count,
		1
	);
}

async fn queue(index: &Index) -> Vec<Key> {
	crate::fdb::run(&index.database, |txn| async move {
		let begin = Index::pack(&index.subspace, &(61i32,));
		let end = Index::pack(&index.subspace, &(62i32,));
		let range = fdb::RangeOption::from((begin.as_slice(), end.as_slice()));
		let result = txn.get_range(&range, 1, false).await;
		let entries = crate::fdb::retry!(result);
		let entries = entries
			.iter()
			.map(|entry| {
				let crate::fdb::Key::Update(key) = Index::unpack(&index.subspace, entry.key())?
				else {
					unreachable!()
				};
				Ok(key)
			})
			.collect::<tg::Result<Vec<_>>>()?;
		Ok(ControlFlow::Break(entries))
	})
	.await
	.unwrap()
}

async fn partition(index: &Index, id: &tg::object::Id, partition: u64) {
	move_entries(index, id, partition, false).await;
}

async fn partition_newest(index: &Index, id: &tg::object::Id, partition: u64) {
	move_entries(index, id, partition, true).await;
}

async fn move_entries(index: &Index, id: &tg::object::Id, partition: u64, newest: bool) {
	let id = tg::Either::Left(id.clone());
	let mut entries = queue(index)
		.await
		.into_iter()
		.filter(|key| matches!(key, Key::UpdateVersion { id: entry, .. } if *entry == id))
		.collect::<Vec<_>>();
	entries.sort_by_key(|entry| match entry {
		Key::UpdateVersion { version, .. } => version.clone(),
		_ => unreachable!(),
	});
	if newest {
		entries = vec![entries.pop().unwrap()];
	}
	crate::fdb::run(&index.database, |txn| {
		let entries = &entries;
		let id = &id;
		async move {
			for entry in entries {
				let key = Index::pack(&index.subspace, &crate::fdb::Key::Update(entry.clone()));
				txn.clear(&key);
				let Key::UpdateVersion { version, .. } = entry else {
					unreachable!()
				};
				let key = crate::fdb::Key::Update(Key::UpdateVersion {
					id: id.clone(),
					kind: Kind::Node,
					partition,
					version: version.clone(),
				});
				txn.set(&Index::pack(&index.subspace, &key), &[]);
			}
			Ok(ControlFlow::Break(()))
		}
	})
	.await
	.unwrap();
}
