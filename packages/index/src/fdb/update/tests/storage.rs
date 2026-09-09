use {
	super::{Index, Key, Kind},
	crate::fdb::update::StorageKind,
	foundationdb as fdb, foundationdb_tuple as fdbt,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn coalesced_storage_additions_preserve_the_oldest_version() {
	super::run(async |index| additions(index, false).await).await;
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn late_storage_additions_preserve_the_oldest_version() {
	super::run(async |index| additions(index, true).await).await;
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn paginated_storage_updates_preserve_the_oldest_version() {
	super::run(pages).await;
}

#[tokio::test]
#[ignore = "requires FoundationDB and FDB_CLUSTER_FILE"]
async fn completed_storage_updates_preserve_the_oldest_version() {
	super::run(async |index| {
		let leaf = super::directory(&[]);
		let middle = super::directory(&[("leaf", leaf.id.clone())]);
		super::put(index, vec![leaf.clone(), middle.clone()]).await;
		super::drain(index).await;
		let account = crate::usage::Account::User(tg::user::Id::new());
		associate(index, &account, &middle.id, 0).await;
		let old = queue(index).await.pop().unwrap();
		let kind = Kind::Storage(StorageKind::Propagate {
			account: account.clone(),
			touched_at: 0,
		});
		enqueue(index, &middle.id, &kind, None).await;
		let entries = queue(index).await;
		let newer = entries.iter().max_by_key(|key| version(key)).unwrap();
		step(index, newer).await;
		step(index, &old).await;
		let entries = queue(index).await;
		assert!(
			entries
				.iter()
				.any(|key| item(key) == &leaf.id && version(key) == version(&old))
		);
		assert!(!associated(index, &account, &leaf.id).await);
		drain(index).await;
		assert!(associated(index, &account, &leaf.id).await);
	})
	.await;
}

async fn additions(index: &Index, late: bool) {
	// Two directory roots reach the same account through a shared middle directory.
	let leaf = super::directory(&[]);
	let middle = super::directory(&[("leaf", leaf.id.clone())]);
	let first = super::directory(&[("first", middle.id.clone())]);
	let second = super::directory(&[("second", middle.id.clone())]);
	super::put(
		index,
		vec![leaf.clone(), middle.clone(), first.clone(), second.clone()],
	)
	.await;
	super::drain(index).await;
	let account = crate::usage::Account::User(tg::user::Id::new());
	associate(index, &account, &first.id, 0).await;
	let old = queue(index).await.pop().unwrap();
	let old_version = version(&old).clone();
	if !late {
		step(index, &old).await;
	}
	let cutoff = index.get_transaction_id().await.unwrap();
	associate(index, &account, &second.id, i64::from(late)).await;
	let entries = queue(index).await;
	let newer = entries.iter().find(|key| item(key) == &second.id).unwrap();
	assert!(u64::from_be_bytes(version(newer).as_bytes()[..8].try_into().unwrap()) > cutoff);
	step(index, newer).await;
	let entries = queue(index).await;
	let newer = entries
		.iter()
		.filter(|key| item(key) == &middle.id)
		.max_by_key(|key| version(key))
		.unwrap();
	step(index, newer).await;
	if late {
		step(index, &old).await;
	}
	let entries = queue(index).await;
	let old = entries.iter().find(|key| matches!(key, Key::UpdateVersion { kind: Kind::Storage(StorageKind::Add { .. }), version: value, .. } if *value == old_version)).unwrap();
	step(index, old).await;
	assert!(!associated(index, &account, &leaf.id).await);
	let oldest = index
		.try_get_oldest_update_transaction_id(crate::update::Kind::Storage)
		.await
		.unwrap();
	assert!(
		oldest.is_some_and(|version| version <= cutoff),
		"the storage wait lost its pending descendants: {oldest:?} > {cutoff}"
	);
	drain(index).await;
	assert!(associated(index, &account, &leaf.id).await);

	// A repeated old addition must stop without walking the descendants again.
	enqueue(
		index,
		&middle.id,
		&Kind::Storage(StorageKind::Add {
			account: account.clone(),
			touched_at: 0,
		}),
		Some(&old_version),
	)
	.await;
	let entry = queue(index).await.pop().unwrap();
	step(index, &entry).await;
	assert!(queue(index).await.is_empty());

	// Collect the account associations while retaining the cached objects.
	let ids = [leaf.id.clone(), middle.id, first.id.clone(), second.id];
	index
		.touch_objects_with_account(&ids, None, 100, std::time::Duration::ZERO)
		.await
		.unwrap();
	let arg = crate::clean::Arg {
		batch_size: 100,
		max_object_touched_at: 1,
		max_process_touched_at: 1,
		max_sandbox_touched_at: 1,
		now: 1,
		partition_end: 2,
		partition_start: 0,
	};
	for _ in 0..20 {
		let done = index.clean(arg.clone()).await.unwrap().done;
		drain(index).await;
		if done && !associated(index, &account, &leaf.id).await {
			break;
		}
	}
	assert!(
		index
			.try_get_objects(&ids)
			.await
			.unwrap()
			.iter()
			.all(Option::is_some)
	);
	for id in &ids {
		assert!(!associated(index, &account, id).await);
		let account = &account;
		crate::fdb::run(&index.database, |txn| async move {
			for key in [
				Key::StorageAddition {
					account: account.clone(),
					id: tg::Either::Left(id.clone()),
				},
				Key::StoragePropagation {
					account: account.clone(),
					id: tg::Either::Left(id.clone()),
				},
			] {
				let value = crate::fdb::propagate!(
					Index::try_get_propagation_version(&txn, &index.subspace, &key).await
				);
				assert!(value.is_none());
			}
			Ok(ControlFlow::Break(()))
		})
		.await
		.unwrap();
	}
	associate(index, &account, &first.id, 2).await;
	drain(index).await;
	assert!(associated(index, &account, &leaf.id).await);
}

async fn pages(index: &Index) {
	let leaf = super::directory(&[]);
	let children = (0..12)
		.map(|i| super::directory(&[(&format!("leaf_{i}"), leaf.id.clone())]))
		.collect::<Vec<_>>();
	let entries = children
		.iter()
		.enumerate()
		.map(|(i, child)| (format!("child_{i}"), child.id.clone()))
		.collect::<Vec<_>>();
	let entries = entries
		.iter()
		.map(|(name, id)| (name.as_str(), id.clone()))
		.collect::<Vec<_>>();
	let middle = super::directory(&entries);
	let objects = children
		.iter()
		.cloned()
		.chain([leaf, middle.clone()])
		.collect();
	super::put(index, objects).await;
	super::drain(index).await;
	let account = crate::usage::Account::User(tg::user::Id::new());
	associate(index, &account, &middle.id, 0).await;
	let old = queue(index).await.pop().unwrap();
	let cutoff = index.get_transaction_id().await.unwrap();
	let kind = Kind::Storage(StorageKind::Propagate {
		account: account.clone(),
		touched_at: 0,
	});
	enqueue(index, &middle.id, &kind, None).await;
	let entries = queue(index).await;
	let newer = entries.iter().max_by_key(|key| version(key)).unwrap();
	assert!(version(newer) > version(&old));
	step(index, newer).await;
	let entries = queue(index).await;
	let first_child = entries.iter().find(|key| item(key) != &middle.id).unwrap();
	let first_child = item(first_child).clone();
	step(index, &old).await;
	let entries = queue(index).await;
	assert!(
		entries
			.iter()
			.any(|key| item(key) == &first_child && version(key) == version(&old)),
		"the older traversal skipped the page already visited by the newer traversal"
	);
	let oldest = index
		.try_get_oldest_update_transaction_id(crate::update::Kind::Storage)
		.await
		.unwrap();
	assert!(oldest.is_some_and(|version| version <= cutoff));
	// A newer queue marker must finish the remaining page at the older cursor version.
	step(index, newer).await;
	let entries = queue(index).await;
	for child in &children {
		assert!(
			entries
				.iter()
				.any(|key| item(key) == &child.id && version(key) == version(&old))
		);
	}
	drain(index).await;
	for child in children {
		assert!(associated(index, &account, &child.id).await);
	}
}

async fn associate(
	index: &Index,
	account: &crate::usage::Account,
	object: &tg::object::Id,
	touched_at: i64,
) {
	let arg = crate::usage::storage::put::ObjectArg {
		account: account.clone(),
		object: object.clone(),
		touched_at,
	};
	let arg = crate::batch::Arg {
		items: vec![crate::batch::Item::PutAccountObject(arg)],
	};
	index.batch(arg).await.unwrap();
}

async fn associated(
	index: &Index,
	account: &crate::usage::Account,
	object: &tg::object::Id,
) -> bool {
	crate::fdb::run(&index.database, |txn| async move {
		let key = crate::fdb::Key::Usage(crate::fdb::usage::Key::AccountObject {
			account: account.clone(),
			object: object.clone(),
		});
		let result = txn.get(&Index::pack(&index.subspace, &key), false).await;
		let value = crate::fdb::retry!(result);
		Ok(ControlFlow::Break(value.is_some()))
	})
	.await
	.unwrap()
}

async fn enqueue(
	index: &Index,
	object: &tg::object::Id,
	kind: &Kind,
	version: Option<&fdbt::Versionstamp>,
) {
	crate::fdb::run(&index.database, |txn| async move {
		Index::enqueue_update_with_kind_at_version(
			&txn,
			&index.subspace,
			&tg::Either::Left(object.clone()),
			kind,
			crate::fdb::update::Source::Put,
			2,
			version,
		);
		Ok(ControlFlow::Break(()))
	})
	.await
	.unwrap();
}

async fn step(index: &Index, selected: &Key) {
	// Fix the partition assignment while keeping every real commit versionstamp intact.
	let entries = queue(index).await;
	crate::fdb::run(&index.database, |txn| {
		let entries = &entries;
		async move {
			for entry in entries {
				let Key::UpdateVersion {
					id, kind, version, ..
				} = entry
				else {
					unreachable!()
				};
				let selected = item(entry) == item(selected)
					&& version == self::version(selected)
					&& matches!(selected, Key::UpdateVersion { kind: selected_kind, .. } if kind == selected_kind);
				txn.clear(&Index::pack(
					&index.subspace,
					&crate::fdb::Key::Update(entry.clone()),
				));
				let key = Key::UpdateVersion {
					id: id.clone(),
					kind: kind.clone(),
					partition: u64::from(!selected),
					version: version.clone(),
				};
				txn.set(
					&Index::pack(&index.subspace, &crate::fdb::Key::Update(key)),
					&[],
				);
			}
			Ok(ControlFlow::Break(()))
		}
	})
	.await
	.unwrap();
	assert_eq!(
		index
			.update_batch(crate::update::Kind::Storage, 1, 0, 1)
			.await
			.unwrap()
			.count,
		1
	);
}

async fn drain(index: &Index) {
	for _ in 0..100 {
		if index
			.update_batch(crate::update::Kind::Storage, 100, 0, 2)
			.await
			.unwrap()
			.count == 0
		{
			return;
		}
	}
	panic!("the storage updates did not drain");
}

async fn queue(index: &Index) -> Vec<Key> {
	crate::fdb::run(&index.database, |txn| async move {
		let begin = Index::pack(&index.subspace, &(63i32,));
		let end = Index::pack(&index.subspace, &(64i32,));
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

fn item(key: &Key) -> &tg::object::Id {
	let Key::UpdateVersion {
		id: tg::Either::Left(id),
		..
	} = key
	else {
		unreachable!()
	};
	id
}

fn version(key: &Key) -> &fdbt::Versionstamp {
	let Key::UpdateVersion { version, .. } = key else {
		unreachable!()
	};
	version
}
