use {
	super::super::{Index, Key},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

async fn drain_grant_updates(index: &Index) {
	loop {
		let output = index
			.update_batch(crate::update::Kind::Grant, 100)
			.await
			.unwrap();
		if output.count == 0 {
			break;
		}
	}
}

#[tokio::test]
async fn implicit_grants_become_non_expiring_when_a_process_relationship_is_added() {
	let (_dir, index) = super::new_index();
	let command = object_id(0);
	let creator = tg::Principal::User(tg::user::Id::new());
	let process = tg::process::Id::new();
	let subject = tg::authorization::Subject::Process(process.clone());
	let subtree = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	);
	index
		.put_grants(&[crate::grant::put::Arg {
			created_at: 0,
			creator: Some(creator),
			implicit: Some(Some(10)),
			permissions: subtree.into(),
			resource: command.clone().into(),
			subject: subject.clone(),
			time_to_touch: None,
		}])
		.await
		.unwrap();
	drain_grant_updates(&index).await;

	index
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::PutProcess(crate::process::put::Arg {
				cached: false,
				children: None,
				command: command.clone(),
				data: None,
				error: None,
				id: process.clone(),
				log: None,
				metadata: tg::process::Metadata::default(),
				options: tg::referent::Options::default(),
				output: None,
				parent: None,
				sandbox: None,
				storage: crate::process::Storage::default(),
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 0,
			})],
		})
		.await
		.unwrap();
	drain_grant_updates(&index).await;

	let value = process_implicit_value(&index, &command, &process, subtree);
	assert_eq!(value.implicit, Some(None));
}

fn object_id(n: usize) -> tg::object::Id {
	tg::object::Id::new(tg::object::Kind::Blob, &n.to_le_bytes().to_vec().into())
}

fn process_implicit_value(
	index: &Index,
	object: &tg::object::Id,
	process: &tg::process::Id,
	permission: tg::authorization::Permission,
) -> super::super::grant::GrantValue {
	let transaction = index.env.read_txn().unwrap();
	let key = Key::Grant(super::super::grant::Key::ResourceGrant {
		creator: Some(tg::Principal::Process(process.clone())),
		permission,
		resource: object.clone().into(),
		subject: tg::authorization::Subject::Process(process.clone()),
	});
	let key = Index::pack(&index.subspace, &key);
	let value = index.db.get(&transaction, &key).unwrap().unwrap();

	super::super::grant::GrantValue::deserialize(value).unwrap()
}

fn implicit_grant_expiration_exists(
	index: &Index,
	object: &tg::object::Id,
	process: &tg::process::Id,
	permission: tg::authorization::Permission,
	expires_at: i64,
) -> bool {
	let transaction = index.env.read_txn().unwrap();
	let key = Key::Grant(super::super::grant::Key::GrantExpiresAt {
		creator: Some(tg::Principal::Process(process.clone())),
		expires_at,
		permission,
		resource: object.clone().into(),
		source: super::super::grant::GrantSource::Implicit,
		subject: tg::authorization::Subject::Process(process.clone()),
	});
	let key = Index::pack(&index.subspace, &key);

	index.db.get(&transaction, &key).unwrap().is_some()
}

#[tokio::test]
async fn process_permissions_promote_to_non_expiring_implicit_grants() {
	let (_dir, index) = super::new_index();
	let child = object_id(1);
	let process = tg::process::Id::new();
	let unrelated = object_id(2);
	let wrapper = object_id(0);
	let put_object = |id: tg::object::Id, children| {
		crate::batch::Item::PutObject(crate::object::put::Arg {
			checkout: None,
			children,
			id,
			metadata: tg::object::Metadata::default(),
			put: [1; 16],
			storage: crate::object::Storage::default(),
			time_to_touch: std::time::Duration::ZERO,
			touched_at: 0,
		})
	};
	let node = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Node,
	);
	let subtree = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	);
	let subject = tg::authorization::Subject::Process(process.clone());
	let creator = tg::Principal::Process(process.clone());
	index
		.batch(crate::batch::Arg {
			items: vec![
				put_object(child.clone(), BTreeSet::new()),
				put_object(unrelated.clone(), BTreeSet::new()),
				put_object(wrapper.clone(), BTreeSet::from([child.clone()])),
				crate::batch::Item::PutProcess(crate::process::put::Arg {
					cached: false,
					children: None,
					command: wrapper.clone(),
					data: None,
					error: None,
					id: process.clone(),
					log: None,
					metadata: tg::process::Metadata::default(),
					options: tg::referent::Options::default(),
					output: None,
					parent: None,
					sandbox: None,
					storage: crate::process::Storage::default(),
					time_to_touch: std::time::Duration::ZERO,
					touched_at: 0,
				}),
				crate::batch::Item::PutGrant(crate::grant::put::Arg {
					created_at: 0,
					creator: Some(creator.clone()),
					implicit: Some(None),
					permissions: node.into(),
					resource: wrapper.clone().into(),
					subject: subject.clone(),
					time_to_touch: None,
				}),
			],
		})
		.await
		.unwrap();
	drain_grant_updates(&index).await;

	index
		.put_grants(&[
			crate::grant::put::Arg {
				created_at: 0,
				creator: Some(creator.clone()),
				implicit: Some(Some(10)),
				permissions: subtree.into(),
				resource: child.clone().into(),
				subject: subject.clone(),
				time_to_touch: None,
			},
			crate::grant::put::Arg {
				created_at: 0,
				creator: Some(creator.clone()),
				implicit: Some(Some(10)),
				permissions: subtree.into(),
				resource: unrelated.clone().into(),
				subject: subject.clone(),
				time_to_touch: None,
			},
		])
		.await
		.unwrap();
	drain_grant_updates(&index).await;

	let child_value = process_implicit_value(&index, &child, &process, subtree);
	assert_eq!(child_value.implicit, Some(None));
	assert!(!implicit_grant_expiration_exists(
		&index, &child, &process, subtree, 10,
	));
	let wrapper_value = process_implicit_value(&index, &wrapper, &process, subtree);
	assert_eq!(wrapper_value.implicit, Some(None));
	let unrelated_value = process_implicit_value(&index, &unrelated, &process, subtree);
	assert_eq!(unrelated_value.implicit, Some(Some(10)));
	assert!(implicit_grant_expiration_exists(
		&index, &unrelated, &process, subtree, 10,
	));

	index
		.delete_grants(&[crate::grant::delete::Arg {
			creator: Some(creator),
			implicit: Some(Some(10)),
			permissions: subtree.into(),
			resource: child.clone().into(),
			subject,
		}])
		.await
		.unwrap();
	drain_grant_updates(&index).await;

	let child_value = process_implicit_value(&index, &child, &process, subtree);
	assert_eq!(child_value.implicit, Some(None));
}

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
		super::super::update::Kind::Grant(tg::authorization::Subject::User(user.clone())),
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
		super::super::update::Kind::Storage(super::super::update::StorageKind::Add {
			account: crate::usage::Account::User(user),
			touched_at: 0,
		}),
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
		.update_batch(crate::update::Kind::Node, 100)
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
		.update_batch(crate::update::Kind::Grant, 100)
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
		.update_batch(crate::update::Kind::Storage, 100)
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
