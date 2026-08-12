use {
	super::super::{Index, Kind},
	num_traits::ToPrimitive as _,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

fn count_clean_keys(index: &Index) -> usize {
	let transaction = index.env.read_txn().unwrap();
	let prefix = Index::pack(&index.subspace, &(Kind::Clean.to_i32().unwrap(),));
	index
		.db
		.prefix_iter(&transaction, &prefix)
		.unwrap()
		.map(Result::unwrap)
		.count()
}

#[tokio::test]
async fn account_and_entity_candidates_share_the_clean_batch() {
	let (_dir, index) = super::new_index();
	let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![0].into());
	let account = crate::usage::Account::User(tg::user::Id::new());
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutObject(crate::object::put::Arg {
				cache_entry: None,
				children: BTreeSet::new(),
				id: object.clone(),
				metadata: tg::object::Metadata::default(),
				stored: crate::object::Stored::default(),
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 1,
			}),
			crate::batch::Item::PutAccountObject(crate::usage::storage::put::ObjectArg {
				account,
				object,
				touched_at: 1,
			}),
		],
	};
	index.batch(arg).await.unwrap();
	assert_eq!(count_clean_keys(&index), 2);

	let output = index
		.clean(crate::clean::Arg {
			batch_size: 1,
			max_object_touched_at: i64::MAX,
			max_process_touched_at: i64::MAX,
			max_sandbox_touched_at: i64::MAX,
			now: i64::MAX,
			partition_end: 1,
			partition_start: 0,
		})
		.await
		.unwrap();
	assert!(!output.done);
	assert_eq!(count_clean_keys(&index), 1);
}
