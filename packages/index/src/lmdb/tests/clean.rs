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

fn count_subject_grants(index: &Index, subject: &tg::authorization::Subject) -> usize {
	let transaction = index.env.read_txn().unwrap();
	let prefix = Index::pack(
		&index.subspace,
		&(Kind::SubjectGrant.to_i32().unwrap(), subject.to_string()),
	);
	index
		.db
		.prefix_iter(&transaction, &prefix)
		.unwrap()
		.map(Result::unwrap)
		.count()
}

#[tokio::test]
async fn deleting_a_process_deletes_all_grants_it_holds() {
	let (_dir, index) = super::new_index();
	let command = tg::object::Id::new(tg::object::Kind::Blob, &vec![0].into());
	let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![1].into());
	let process = tg::process::Id::new();
	let creator = tg::Principal::Process(process.clone());
	let subject = tg::authorization::Subject::Process(process.clone());
	let subtree = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	);
	let put_object = |id| {
		crate::batch::Item::PutObject(crate::object::put::Arg {
			checkout: None,
			children: BTreeSet::new(),
			id,
			metadata: tg::object::Metadata::default(),
			stored: crate::object::Stored::default(),
			time_to_touch: std::time::Duration::ZERO,
			touched_at: 0,
		})
	};
	index
		.batch(crate::batch::Arg {
			items: vec![
				put_object(command.clone()),
				put_object(object.clone()),
				crate::batch::Item::PutProcess(crate::process::put::Arg {
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
					stored: crate::process::Stored::default(),
					time_to_touch: std::time::Duration::ZERO,
					touched_at: 0,
				}),
				crate::batch::Item::PutGrant(crate::grant::put::Arg {
					created_at: 0,
					creator: Some(creator.clone()),
					implicit: Some(None),
					permissions: subtree.into(),
					resource: command.into(),
					subject: subject.clone(),
					time_to_touch: None,
				}),
				crate::batch::Item::PutGrant(crate::grant::put::Arg {
					created_at: 0,
					creator: Some(creator),
					implicit: Some(Some(10)),
					permissions: subtree.into(),
					resource: object.into(),
					subject: subject.clone(),
					time_to_touch: None,
				}),
			],
		})
		.await
		.unwrap();
	assert_eq!(count_subject_grants(&index, &subject), 2);

	loop {
		let output = index
			.clean(crate::clean::Arg {
				batch_size: 100,
				max_object_touched_at: i64::MAX,
				max_process_touched_at: i64::MAX,
				max_sandbox_touched_at: i64::MAX,
				now: i64::MAX,
				partition_end: 1,
				partition_start: 0,
			})
			.await
			.unwrap();
		if output.done {
			break;
		}
	}

	assert_eq!(count_subject_grants(&index, &subject), 0);
}

#[tokio::test]
async fn account_and_entity_candidates_share_the_clean_batch() {
	let (_dir, index) = super::new_index();
	let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![0].into());
	let account = crate::usage::Account::User(tg::user::Id::new());
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutObject(crate::object::put::Arg {
				checkout: None,
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
