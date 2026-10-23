use {
	super::super::{Index, Key},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

fn complete_metadata() -> tg::object::Metadata {
	tg::object::Metadata {
		subtree: tg::object::metadata::Subtree {
			count: Some(1),
			depth: Some(1),
			size: Some(1),
			solvable: Some(false),
			solved: Some(true),
		},
		..Default::default()
	}
}

fn object_arg(
	id: tg::object::Id,
	checkout: Option<tg::artifact::Id>,
) -> tangram_index::object::put::Arg {
	tangram_index::object::put::Arg {
		checkout,
		children: BTreeSet::new(),
		id,
		metadata: complete_metadata(),
		put: [1; 16],
		storage: tg::object::storage::Set::NODE | tg::object::storage::Set::SUBTREE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 10,
	}
}

fn put_checkout_arg(id: tg::artifact::Id) -> tangram_index::checkout::put::Arg {
	tangram_index::checkout::put::Arg {
		dependencies: Vec::new(),
		id: id.into(),
		touched_at: 0,
	}
}

fn relationship_exists(
	index: &Index,
	object: &tg::object::Id,
	checkout: &tg::artifact::Id,
) -> bool {
	let transaction = index.env.read_txn().unwrap();
	let key = Key::Object(crate::object::Key::ObjectCheckout {
		checkout: checkout.clone(),
		object: object.clone(),
	});
	let key = Index::pack(&index.subspace, &key);

	index.db.get(&transaction, &key).unwrap().is_some()
}

#[tokio::test]
async fn put_updates_put_while_honoring_time_to_touch() {
	let (_directory, index) = super::new_index();
	let id = tg::object::Id::new(
		tg::object::Kind::Blob,
		&b"object".as_slice().to_vec().into(),
	);
	let mut arg = object_arg(id.clone(), None);
	arg.time_to_touch = std::time::Duration::from_secs(100);
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutObject(arg.clone())],
	};
	index.batch(batch).await.unwrap().unwrap();
	arg.put = [2; 16];
	arg.touched_at = 20;
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutObject(arg)],
	};
	index.batch(batch).await.unwrap().unwrap();

	let object = index
		.try_get_objects(&[id])
		.await
		.unwrap()
		.pop()
		.unwrap()
		.unwrap();
	assert_eq!(object.put, [2; 16]);
	assert_eq!(object.touched_at, 10);
}

#[tokio::test]
async fn replacing_or_removing_an_object_checkout_removes_the_previous_relationship() {
	let (_directory, index) = super::new_index();
	let object = tg::object::Id::new(
		tg::object::Kind::Blob,
		&b"object".as_slice().to_vec().into(),
	);
	let checkout_a = tg::artifact::Id::from(tg::file::Id::new(b"checkout_a"));
	let checkout_b = tg::artifact::Id::from(tg::file::Id::new(b"checkout_b"));
	let tag = tg::tag::Id::new();
	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutCheckout(put_checkout_arg(checkout_a.clone())),
			tangram_index::batch::Item::PutObject(object_arg(
				object.clone(),
				Some(checkout_a.clone()),
			)),
			tangram_index::batch::Item::PutTag(tangram_index::tag::put::Arg {
				touched_at: 0,
				version: "initial".into(),
				account: None,
				id: tag,
				name: "tag".to_owned(),
				parent: None,
				specifier: "tag".parse().unwrap(),
				target: tg::Either::Left(object.clone()),
			}),
		],
	};
	index.batch(arg).await.unwrap().unwrap();
	assert!(relationship_exists(&index, &object, &checkout_a));
	let output = index
		.clean(tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: 0,
			max_process_touched_at: 0,
			max_sandbox_touched_at: 0,
			now: 0,
			partition_end: 1,
			partition_start: 0,
		})
		.await
		.unwrap();
	assert_eq!(output.checkouts, Vec::new());

	let arg = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutCheckout(put_checkout_arg(checkout_b.clone())),
			tangram_index::batch::Item::PutObject(object_arg(
				object.clone(),
				Some(checkout_b.clone()),
			)),
		],
	};
	index.batch(arg).await.unwrap().unwrap();

	let indexed = index
		.try_get_objects(std::slice::from_ref(&object))
		.await
		.unwrap()
		.pop()
		.unwrap()
		.unwrap();
	assert_eq!(indexed.checkout, Some(checkout_b.clone()));
	assert!(!relationship_exists(&index, &object, &checkout_a));
	assert!(relationship_exists(&index, &object, &checkout_b));

	let output = index
		.clean(tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: 0,
			max_process_touched_at: 0,
			max_sandbox_touched_at: 0,
			now: 0,
			partition_end: 1,
			partition_start: 0,
		})
		.await
		.unwrap();
	assert_eq!(output.checkouts, vec![checkout_a.into()]);

	let arg = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutObject(object_arg(
			object.clone(),
			None,
		))],
	};
	index.batch(arg).await.unwrap().unwrap();

	let indexed = index
		.try_get_objects(std::slice::from_ref(&object))
		.await
		.unwrap()
		.pop()
		.unwrap()
		.unwrap();
	assert_eq!(indexed.checkout, None);
	assert!(!relationship_exists(&index, &object, &checkout_b));

	let output = index
		.clean(tangram_index::clean::Arg {
			batch_size: 100,
			max_object_touched_at: 0,
			max_process_touched_at: 0,
			max_sandbox_touched_at: 0,
			now: 0,
			partition_end: 1,
			partition_start: 0,
		})
		.await
		.unwrap();
	assert_eq!(output.checkouts, vec![checkout_b.into()]);
}
