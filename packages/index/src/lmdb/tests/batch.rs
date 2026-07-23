use {
	super::super::{Config, Index},
	std::str::FromStr as _,
	tangram_client::prelude::*,
};

fn new_index() -> (tempfile::TempDir, Index) {
	let dir = tempfile::TempDir::new().unwrap();
	let index = Index::new(&Config {
		authorize: super::super::AuthorizeConfig {
			object_subtree: crate::authorize::ObjectSubtreeConfig::default(),
		},
		map_size: 1 << 30,
		max_items_per_transaction: 1,
		max_process_depth: None,
		path: dir.path().join("index"),
	})
	.unwrap();
	(dir, index)
}

fn try_get_group(index: &Index, id: &tg::group::Id) -> Option<crate::group::Group> {
	let transaction = index.env.read_txn().unwrap();
	Index::try_get_group_with_transaction(&index.db, &index.subspace, &transaction, id).unwrap()
}

#[tokio::test]
async fn preserves_order_and_transaction_boundary() {
	let (_dir, index) = new_index();
	let id = tg::group::Id::new();
	let put_arg = crate::group::put::Arg {
		id: id.clone(),
		parent: None,
		specifier: tg::Specifier::from_str("test").unwrap(),
	};

	let before = index.get_transaction_id().await.unwrap();
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutGroup(put_arg.clone()),
			crate::batch::Item::DeleteGroup(id.clone()),
		],
	};
	index.batch(arg).await.unwrap();
	let after = index.get_transaction_id().await.unwrap();
	assert_eq!(after, before + 1);
	assert!(try_get_group(&index, &id).is_none());

	let before = index.get_transaction_id().await.unwrap();
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::DeleteGroup(id.clone()),
			crate::batch::Item::PutGroup(put_arg),
		],
	};
	index.batch(arg).await.unwrap();
	let after = index.get_transaction_id().await.unwrap();
	assert_eq!(after, before + 1);
	assert!(try_get_group(&index, &id).is_some());
}
