use {
	super::{super::Index, new_index},
	std::str::FromStr as _,
	tangram_client::prelude::*,
};

fn try_get_group(index: &Index, id: &tg::group::Id) -> Option<crate::group::Group> {
	let transaction = index.env.read_txn().unwrap();
	Index::try_get_group_with_transaction(&index.db, &index.subspace, &transaction, id).unwrap()
}

fn try_get_organization(
	index: &Index,
	id: &tg::organization::Id,
) -> Option<crate::organization::Organization> {
	let transaction = index.env.read_txn().unwrap();
	Index::try_get_organization_with_transaction(&index.db, &index.subspace, &transaction, id)
		.unwrap()
}

fn try_get_user(index: &Index, id: &tg::user::Id) -> Option<crate::user::User> {
	let transaction = index.env.read_txn().unwrap();
	Index::try_get_user_with_transaction(&index.db, &index.subspace, &transaction, id).unwrap()
}

#[tokio::test]
async fn partial_account_updates_preserve_billing() {
	let (_dir, index) = new_index();
	let new_organization = tg::organization::Id::new();
	let new_user = tg::user::Id::new();
	let organization = tg::organization::Id::new();
	let user = tg::user::Id::new();
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutOrganization(crate::organization::put::Arg {
				billing: None,
				id: new_organization.clone(),
				specifier: tg::Specifier::from_str("new_organization").unwrap(),
			}),
			crate::batch::Item::PutOrganization(crate::organization::put::Arg {
				billing: Some(true),
				id: organization.clone(),
				specifier: tg::Specifier::from_str("organization").unwrap(),
			}),
			crate::batch::Item::PutUser(crate::user::put::Arg {
				billing: None,
				id: new_user.clone(),
				specifier: tg::Specifier::from_str("new_user").unwrap(),
			}),
			crate::batch::Item::PutUser(crate::user::put::Arg {
				billing: Some(true),
				id: user.clone(),
				specifier: tg::Specifier::from_str("user").unwrap(),
			}),
		],
	};
	index.batch(arg).await.unwrap();

	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutOrganization(crate::organization::put::Arg {
				billing: None,
				id: organization.clone(),
				specifier: tg::Specifier::from_str("organization").unwrap(),
			}),
			crate::batch::Item::PutUser(crate::user::put::Arg {
				billing: None,
				id: user.clone(),
				specifier: tg::Specifier::from_str("user").unwrap(),
			}),
		],
	};
	index.batch(arg).await.unwrap();

	assert!(
		!try_get_organization(&index, &new_organization)
			.unwrap()
			.billing
	);
	assert!(try_get_organization(&index, &organization).unwrap().billing);
	assert!(!try_get_user(&index, &new_user).unwrap().billing);
	assert!(try_get_user(&index, &user).unwrap().billing);
}

#[tokio::test]
async fn process_and_finalization_share_transaction() {
	let (_dir, index) = new_index();
	let process = tg::process::Id::new();
	let before = index.get_transaction_id().await.unwrap();
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutProcess(crate::process::put::Arg {
				children: Some(Vec::new()),
				command: tg::command::Id::new(b"command").into(),
				data: None,
				error: crate::process::put::Field::Missing,
				id: process.clone(),
				log: crate::process::put::Field::Missing,
				metadata: tg::process::Metadata::default(),
				output: crate::process::put::Field::Missing,
				parent: None,
				sandbox: None,
				stored: crate::process::Stored::default(),
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 0,
			}),
			crate::batch::Item::EnqueueFinalization(crate::finalization::Item::Process(
				process.clone(),
			)),
		],
	};
	index.batch(arg).await.unwrap();
	let after = index.get_transaction_id().await.unwrap();

	assert_eq!(after, before + 1);
	assert!(
		index
			.try_get_processes(std::slice::from_ref(&process))
			.await
			.unwrap()[0]
			.is_some()
	);
	let entries = index
		.finalization_batch(crate::finalization::Kind::Process, 1, 0, 1)
		.await
		.unwrap();
	assert_eq!(entries.len(), 1);
	assert_eq!(entries[0].item, crate::finalization::Item::Process(process));
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
