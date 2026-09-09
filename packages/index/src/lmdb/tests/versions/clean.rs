use {
	super::{Index, Kind, Source, drain, object, put},
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

#[tokio::test]
async fn cleans_versions_after_collecting_their_objects_and_processes() {
	let (_dir, index) = super::super::new_index();
	let object = object(0, []);
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
			enqueue(&index, id, kind, None);
		}
	}
	for queue in [crate::update::Kind::Grant, crate::update::Kind::Node] {
		drain(&index, queue).await;
	}
	for id in &ids {
		for kind in &kinds {
			assert!(version(&index, id, kind).is_some());
		}
	}
	for _ in 0..100 {
		let before = count_clean_entries(&index);
		let output = index.clean(clean_arg(1)).await.unwrap();
		let deleted = output.objects.len() + output.processes.len();
		assert!(before - count_clean_entries(&index) + deleted <= 1);
		if output.done {
			break;
		}
	}
	assert!(index.try_get_objects(&[object_id]).await.unwrap()[0].is_none());
	assert!(index.try_get_processes(&[process_id]).await.unwrap()[0].is_none());
	assert_eq!(count_versions(&index), 0);

	// Stale updates must not leave orphaned versions after their objects and processes are gone.
	for id in &ids {
		for kind in &kinds {
			enqueue(&index, id, kind, None);
		}
	}
	for queue in [crate::update::Kind::Grant, crate::update::Kind::Node] {
		drain(&index, queue).await;
	}
	for _ in 0..100 {
		if index.clean(clean_arg(1)).await.unwrap().done {
			break;
		}
	}
	assert_eq!(count_versions(&index), 0);
	assert_eq!(count_clean_entries(&index), 0);
}

#[tokio::test]
async fn cleans_versions_without_collecting_the_objects() {
	let (_dir, index) = super::super::new_index();
	let mut object = object(0, []);
	object.touched_at = 100;
	let id = tg::Either::Left(object.id.clone());
	put(&index, vec![object]).await;
	let subjects = [
		tg::authorization::Subject::Process(tg::process::Id::new()),
		tg::authorization::Subject::Process(tg::process::Id::new()),
	];
	for subject in subjects {
		enqueue(&index, &id, &Kind::Grant(subject), None);
	}
	for kind in [crate::update::Kind::Grant, crate::update::Kind::Node] {
		drain(&index, kind).await;
	}
	assert_eq!(count_versions(&index), 3);
	let mut done = false;
	for _ in 0..20 {
		let before = count_versions(&index);
		let clean_before = count_clean_entries(&index);
		let output = index.clean(clean_arg(1)).await.unwrap();
		let after = count_versions(&index);
		assert!(before - after <= 1);
		assert!(clean_before - count_clean_entries(&index) <= 1);
		if output.done {
			done = true;
			break;
		}
	}
	assert!(done);
	assert_eq!(count_versions(&index), 0);
	assert_eq!(count_clean_entries(&index), 0);
	let object = id.left().unwrap();
	assert!(index.try_get_objects(&[object]).await.unwrap()[0].is_some());
}

#[tokio::test]
async fn cleaning_retains_pending_versions_and_ignores_stale_entries() {
	let (_dir, index) = super::super::new_index();
	let mut object = object(0, []);
	object.touched_at = 100;
	let id = tg::Either::Left(object.id.clone());
	put(&index, vec![object]).await;
	drain(&index, crate::update::Kind::Node).await;
	index.clean(clean_arg(100)).await.unwrap();
	let blocker = tg::Either::Right(tg::process::Id::new());
	let subject = tg::authorization::Subject::User(tg::user::Id::new());
	for (kind, queue) in [
		(Kind::Grant(subject), crate::update::Kind::Grant),
		(Kind::Node, crate::update::Kind::Node),
	] {
		enqueue(&index, &id, &kind, None);
		drain(&index, queue).await;
		let previous = version(&index, &id, &kind).unwrap();
		let oldest = index.get_transaction_id().await.unwrap();
		assert!(oldest > previous, "oldest {oldest}, previous {previous}");
		enqueue(&index, &id, &kind, None);
		drain(&index, queue).await;
		let expected = version(&index, &id, &kind).unwrap();
		assert!(expected > oldest);
		enqueue(&index, &blocker, &kind, Some(oldest));

		// The old cleanup entry is eligible, but the current version is still needed.
		assert!(!index.clean(clean_arg(1)).await.unwrap().done);
		assert_eq!(version(&index, &id, &kind), Some(expected));
		assert!(index.clean(clean_arg(100)).await.unwrap().done);

		// An update at exactly the recorded version also prevents its collection.
		drain(&index, queue).await;
		enqueue(&index, &blocker, &kind, Some(expected));
		index.clean(clean_arg(100)).await.unwrap();
		assert_eq!(version(&index, &id, &kind), Some(expected));
		drain(&index, queue).await;
		index.clean(clean_arg(100)).await.unwrap();
		assert!(version(&index, &id, &kind).is_none());
	}
}

fn version(
	index: &Index,
	id: &tg::Either<tg::object::Id, tg::process::Id>,
	kind: &Kind,
) -> Option<u64> {
	let transaction = index.env.read_txn().unwrap();
	let key = crate::lmdb::Key::Update(crate::lmdb::update::Key::PropagatedVersion {
		id: id.clone(),
		kind: kind.clone(),
	});
	index
		.db
		.get(&transaction, &Index::pack(&index.subspace, &key))
		.unwrap()
		.map(|bytes| u64::from_be_bytes(bytes.try_into().unwrap()))
}

fn enqueue(
	index: &Index,
	id: &tg::Either<tg::object::Id, tg::process::Id>,
	kind: &Kind,
	version: Option<u64>,
) {
	let mut transaction = index.env.write_txn().unwrap();
	Index::enqueue_update_with_kind(
		&index.db,
		&index.subspace,
		&mut transaction,
		id.clone(),
		kind.clone(),
		Source::Put,
		version,
	)
	.unwrap();
	transaction.commit().unwrap();
}

fn clean_arg(batch_size: usize) -> crate::clean::Arg {
	crate::clean::Arg {
		batch_size,
		max_object_touched_at: 0,
		max_process_touched_at: 0,
		max_sandbox_touched_at: 0,
		now: 0,
		partition_end: 1,
		partition_start: 0,
	}
}

fn count_versions(index: &Index) -> usize {
	let kinds = [
		crate::lmdb::Kind::GrantUpdatePropagatedVersion,
		crate::lmdb::Kind::NodeUpdatePropagatedVersion,
	];
	count_keys(index, &kinds)
}

fn count_clean_entries(index: &Index) -> usize {
	let kinds = [
		crate::lmdb::Kind::GrantUpdateClean,
		crate::lmdb::Kind::NodeUpdateClean,
	];
	count_keys(index, &kinds)
}

fn count_keys(index: &Index, kinds: &[crate::lmdb::Kind]) -> usize {
	let transaction = index.env.read_txn().unwrap();
	let mut count = 0;
	for kind in kinds {
		let prefix = Index::pack(&index.subspace, &(kind.to_i32().unwrap(),));
		count += index
			.db
			.prefix_iter(&transaction, &prefix)
			.unwrap()
			.map(Result::unwrap)
			.count();
	}
	count
}
