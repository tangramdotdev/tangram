use {
	super::super::{
		Index, Key,
		update::{Kind, Source},
	},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

mod clean;

#[tokio::test]
async fn a_batch_preserves_an_older_propagation_when_combining_updates() {
	let (_dir, index) = super::new_index();
	let leaf = object(0, []);
	let middle = object(1, [leaf.id.clone()]);
	let top = object(2, [middle.id.clone()]);
	put(&index, vec![middle.clone(), top.clone()]).await;
	drain(&index, crate::update::Kind::Node).await;
	put(&index, vec![leaf.clone()]).await;
	let cutoff = index.get_transaction_id().await.unwrap();
	let mut middle = middle;
	middle.metadata.subtree = tg::object::metadata::Subtree {
		count: Some(2),
		depth: Some(2),
		size: Some(0),
		solvable: Some(false),
		solved: Some(true),
	};
	put(&index, vec![middle]).await;

	// Select both entries before the leaf lowers the middle's pending version.
	{
		let mut transaction = index.env.write_txn().unwrap();
		let output = Index::update_batch_with_transaction(
			&index.db,
			&index.subspace,
			&mut transaction,
			2,
			crate::update::Kind::Node,
			None,
			1,
		)
		.unwrap();
		assert_eq!(output.count, 2);
		transaction.commit().unwrap();
	}
	let oldest = index
		.try_get_oldest_update_transaction_id(crate::update::Kind::Node)
		.await
		.unwrap();
	assert!(oldest.is_some_and(|version| version <= cutoff));
	let objects = index
		.try_get_objects(std::slice::from_ref(&top.id))
		.await
		.unwrap();
	assert_eq!(objects[0].as_ref().unwrap().metadata.subtree.count, None);
	drain(&index, crate::update::Kind::Node).await;
	let objects = index.try_get_objects(&[top.id]).await.unwrap();
	assert_eq!(objects[0].as_ref().unwrap().metadata.subtree.count, Some(3));
}

#[tokio::test]
async fn propagation_versions_reset_and_repeated_updates_stop() {
	let (_dir, index) = super::new_index();
	let child = object(0, []);
	let parent = object(1, [child.id.clone()]);
	put(&index, vec![child.clone(), parent.clone()]).await;
	drain(&index, crate::update::Kind::Node).await;
	let subject = tg::authorization::Subject::User(tg::user::Id::new());
	for (kind, queue) in [
		(Kind::Node, crate::update::Kind::Node),
		(Kind::Grant(subject.clone()), crate::update::Kind::Grant),
	] {
		for (source, version, propagated) in [
			(Source::Put, 100, true),
			(Source::Propagate, 90, true),
			(Source::Propagate, 90, false),
			(Source::Propagate, 110, false),
			(Source::Put, 200, true),
			(Source::Propagate, 150, true),
		] {
			{
				let mut transaction = index.env.write_txn().unwrap();
				Index::enqueue_update_with_kind(
					&index.db,
					&index.subspace,
					&mut transaction,
					tg::Either::Left(child.id.clone()),
					kind.clone(),
					source,
					Some(version),
				)
				.unwrap();
				transaction.commit().unwrap();
			}
			assert_eq!(index.update_batch(queue, 1).await.unwrap().count, 1);
			let oldest = index
				.try_get_oldest_update_transaction_id(queue)
				.await
				.unwrap();
			assert_eq!(
				oldest,
				propagated.then_some(version),
				"{kind:?}, {source:?}, {version}"
			);
			drain(&index, queue).await;
		}
	}

	// Retained propagation versions must neither prevent collection nor survive the objects.
	let arg = crate::clean::Arg {
		batch_size: 100,
		max_object_touched_at: 0,
		max_process_touched_at: 0,
		max_sandbox_touched_at: 0,
		now: 0,
		partition_end: 1,
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
	let ids = [child.id, parent.id];
	assert!(
		index
			.try_get_objects(&ids)
			.await
			.unwrap()
			.iter()
			.all(Option::is_none)
	);
	let transaction = index.env.read_txn().unwrap();
	for id in ids {
		for kind in [Kind::Node, Kind::Grant(subject.clone())] {
			let key = Key::Update(super::super::update::Key::PropagatedVersion {
				id: tg::Either::Left(id.clone()),
				kind,
			});
			assert!(
				index
					.db
					.get(&transaction, &Index::pack(&index.subspace, &key))
					.unwrap()
					.is_none()
			);
		}
	}
}

fn object(id: u8, children: impl IntoIterator<Item = tg::object::Id>) -> crate::object::put::Arg {
	let id = tg::object::Id::new(tg::object::Kind::Directory, &vec![id].into());
	let children = children.into_iter().collect::<BTreeSet<_>>();
	crate::object::put::Arg {
		checkout: None,
		children,
		id,
		metadata: tg::object::Metadata::default(),
		put: [1; 16],
		storage: crate::object::Storage::default(),
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	}
}

async fn put(index: &Index, objects: Vec<crate::object::put::Arg>) {
	let items = objects
		.into_iter()
		.map(crate::batch::Item::PutObject)
		.collect();
	let arg = crate::batch::Arg { items };
	index.batch(arg).await.unwrap();
}

async fn drain(index: &Index, kind: crate::update::Kind) {
	for _ in 0..100 {
		if index.update_batch(kind, 100).await.unwrap().count == 0 {
			return;
		}
	}
	panic!("the updates did not drain");
}
