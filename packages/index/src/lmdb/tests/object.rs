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

fn object_arg(id: tg::object::Id, checkout: Option<tg::artifact::Id>) -> crate::object::put::Arg {
	crate::object::put::Arg {
		checkout,
		children: BTreeSet::new(),
		id,
		metadata: complete_metadata(),
		storage: crate::object::Storage { subtree: true },
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 10,
	}
}

fn put_checkout_arg(id: tg::artifact::Id) -> crate::checkout::put::Arg {
	crate::checkout::put::Arg {
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
	let key = Key::Object(crate::lmdb::object::Key::ObjectCheckout {
		checkout: checkout.clone(),
		object: object.clone(),
	});
	let key = Index::pack(&index.subspace, &key);

	index.db.get(&transaction, &key).unwrap().is_some()
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
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutCheckout(put_checkout_arg(checkout_a.clone())),
			crate::batch::Item::PutCheckout(put_checkout_arg(checkout_b.clone())),
			crate::batch::Item::PutObject(object_arg(object.clone(), Some(checkout_a.clone()))),
		],
	};
	index.batch(arg).await.unwrap();
	assert!(relationship_exists(&index, &object, &checkout_a));

	let arg = crate::batch::Arg {
		items: vec![crate::batch::Item::PutObject(object_arg(
			object.clone(),
			Some(checkout_b.clone()),
		))],
	};
	index.batch(arg).await.unwrap();

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
		.clean(crate::clean::Arg {
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

	let arg = crate::batch::Arg {
		items: vec![crate::batch::Item::PutObject(object_arg(
			object.clone(),
			None,
		))],
	};
	index.batch(arg).await.unwrap();

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
		.clean(crate::clean::Arg {
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
