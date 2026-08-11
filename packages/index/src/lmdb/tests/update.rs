use {super::super::Index, tangram_client::prelude::*};

#[tokio::test]
async fn separates_update_queues() {
	let (_dir, index) = super::new_index();
	let id = tg::object::Id::new(tg::object::Kind::Blob, &vec![0].into());
	let id = tg::Either::Left(id);
	let user = tg::user::Id::new();

	let mut transaction = index.env.write_txn().unwrap();
	Index::enqueue_update_with_kind(
		&index.db,
		&index.subspace,
		&mut transaction,
		id.clone(),
		super::super::update::Kind::Grant(tg::grant::Principal::User(user.clone())),
		super::super::update::Source::Put,
		None,
	)
	.unwrap();
	Index::enqueue_update_with_kind(
		&index.db,
		&index.subspace,
		&mut transaction,
		id.clone(),
		super::super::update::Kind::Node,
		super::super::update::Source::Put,
		None,
	)
	.unwrap();
	Index::enqueue_update_with_kind(
		&index.db,
		&index.subspace,
		&mut transaction,
		id,
		super::super::update::Kind::Storage(super::super::update::StorageKind::Add(
			crate::usage::Account::User(user),
		)),
		super::super::update::Source::Put,
		None,
	)
	.unwrap();
	transaction.commit().unwrap();

	for kind in [
		crate::update::Kind::Grant,
		crate::update::Kind::Node,
		crate::update::Kind::Storage,
	] {
		assert!(
			index
				.try_get_oldest_update_transaction_id(kind)
				.await
				.unwrap()
				.is_some()
		);
	}

	let output = index
		.update_batch(crate::update::Kind::Node, 100, 0, 1)
		.await
		.unwrap();
	assert_eq!(output.count, 1);
	assert_eq!(
		index
			.try_get_oldest_update_transaction_id(crate::update::Kind::Node)
			.await
			.unwrap(),
		None
	);
	assert!(
		index
			.try_get_oldest_update_transaction_id(crate::update::Kind::Grant)
			.await
			.unwrap()
			.is_some()
	);
	assert!(
		index
			.try_get_oldest_update_transaction_id(crate::update::Kind::Storage)
			.await
			.unwrap()
			.is_some()
	);

	let output = index
		.update_batch(crate::update::Kind::Grant, 100, 0, 1)
		.await
		.unwrap();
	assert_eq!(output.count, 1);
	assert_eq!(
		index
			.try_get_oldest_update_transaction_id(crate::update::Kind::Grant)
			.await
			.unwrap(),
		None
	);
	assert!(
		index
			.try_get_oldest_update_transaction_id(crate::update::Kind::Storage)
			.await
			.unwrap()
			.is_some()
	);

	let output = index
		.update_batch(crate::update::Kind::Storage, 100, 0, 1)
		.await
		.unwrap();
	assert_eq!(output.count, 1);
	assert_eq!(
		index
			.try_get_oldest_update_transaction_id(crate::update::Kind::Storage)
			.await
			.unwrap(),
		None
	);
}
