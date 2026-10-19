use {super::super::Index, std::collections::BTreeSet, tangram_client::prelude::*};

#[tokio::test]
async fn object_children_are_returned_in_pages() {
	let (_dir, index) = super::new_index();

	// Put an object with five children.
	let children = (1..=5).map(object_id).collect::<BTreeSet<_>>();
	let parent = object_id(0);
	let object = tangram_index::object::put::Arg {
		checkout: None,
		children: children.clone(),
		id: parent.clone(),
		metadata: tg::object::Metadata::default(),
		put: [1; 16],
		storage: tg::object::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	};
	index
		.batch(tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::PutObject(object)],
		})
		.await
		.unwrap()
		.unwrap();

	// Read the children in pages of two.
	let transaction = index.env.read_txn().unwrap();
	let mut pages = Vec::new();
	let mut after = None;
	loop {
		let (page, cursor) = Index::get_object_children_page_with_transaction(
			&index.db,
			&index.subspace,
			&transaction,
			&parent,
			after.as_deref(),
			2,
		)
		.unwrap();
		pages.push(page);
		let Some(cursor) = cursor else {
			break;
		};
		after = Some(cursor);
	}

	// Verify that the pages contain every child exactly once, and that collecting the pages returns every child.
	assert_eq!(
		pages.iter().map(Vec::len).collect::<Vec<_>>(),
		vec![2, 2, 1]
	);
	let output = pages.into_iter().flatten().collect::<BTreeSet<_>>();
	assert_eq!(output, children);
	let output = crate::page::collect(|after| {
		Index::get_object_children_page_with_transaction(
			&index.db,
			&index.subspace,
			&transaction,
			&parent,
			after,
			2,
		)
	})
	.unwrap()
	.into_iter()
	.collect::<BTreeSet<_>>();
	assert_eq!(output, children);
}

fn object_id(n: usize) -> tg::object::Id {
	tg::object::Id::new(tg::object::Kind::Blob, &n.to_le_bytes().to_vec().into())
}
