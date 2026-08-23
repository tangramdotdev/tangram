use {
	super::{
		super::{Index, Key},
		new_index,
	},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

#[tokio::test]
async fn process_object_grants_walk_and_write_in_one_batch() {
	let (_dir, index) = new_index();
	let child = object_id(1);
	let child_inaccessible = object_id(4);
	let leaf = object_id(2);
	let process = tg::process::Id::new();
	let root = object_id(0);
	let root_inaccessible = object_id(3);
	let creator = tg::Principal::Process(process.clone());
	let subject = tg::authorization::Subject::Process(process.clone());
	let node = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Node,
	);
	let subtree = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	);
	let put_object = |id, children| {
		crate::batch::Item::PutObject(crate::object::put::Arg {
			checkout: None,
			children,
			id,
			metadata: tg::object::Metadata::default(),
			stored: crate::object::Stored::default(),
			time_to_touch: std::time::Duration::ZERO,
			touched_at: 0,
		})
	};
	let put_grant = |resource: tg::object::Id, permission: tg::authorization::Permission| {
		crate::batch::Item::PutGrant(crate::grant::put::Arg {
			created_at: 0,
			creator: Some(creator.clone()),
			implicit: Some(Some(100)),
			permissions: permission.into(),
			resource: tg::Id::from(resource),
			subject: subject.clone(),
			time_to_touch: None,
		})
	};
	index
		.batch(crate::batch::Arg {
			items: vec![
				put_object(child_inaccessible.clone(), BTreeSet::new()),
				put_object(leaf.clone(), BTreeSet::new()),
				put_object(root_inaccessible.clone(), BTreeSet::new()),
				put_object(
					child.clone(),
					BTreeSet::from([child_inaccessible.clone(), leaf.clone()]),
				),
				put_object(
					root.clone(),
					BTreeSet::from([child.clone(), root_inaccessible.clone()]),
				),
				put_grant(root.clone(), node),
				put_grant(child.clone(), node),
				put_grant(leaf.clone(), subtree),
			],
		})
		.await
		.unwrap();

	let command = tg::command::Id::new(b"command");
	index
		.batch(crate::batch::Arg {
			items: vec![
				crate::batch::Item::PutProcess(crate::process::put::Arg {
					cached: false,
					children: None,
					command: command.into(),
					data: None,
					error: None,
					id: process.clone(),
					log: None,
					metadata: tg::process::Metadata::default(),
					options: tg::referent::Options::default(),
					output: Some(Some(vec![root.clone()])),
					parent: None,
					sandbox: None,
					stored: crate::process::Stored::default(),
					time_to_touch: std::time::Duration::ZERO,
					touched_at: 0,
				}),
				crate::batch::Item::PutProcessObjectGrants(crate::process::object::grant::Arg {
					created_at: 0,
					expires_at: None,
					principal: creator.clone(),
					process: process.clone(),
					roots: vec![crate::process::object::grant::Root {
						object: root.clone(),
						permissions: None,
					}],
					time_to_touch: None,
				}),
			],
		})
		.await
		.unwrap();

	assert_eq!(
		process_grant(&index, &process, &root, node)
			.unwrap()
			.implicit,
		Some(None)
	);
	assert_eq!(
		process_grant(&index, &process, &child, node)
			.unwrap()
			.implicit,
		Some(None)
	);
	assert_eq!(
		process_grant(&index, &process, &leaf, subtree)
			.unwrap()
			.implicit,
		Some(None)
	);
	assert!(process_grant(&index, &process, &root_inaccessible, node).is_none());
	assert!(process_grant(&index, &process, &child_inaccessible, node).is_none());
}

fn object_id(value: u64) -> tg::object::Id {
	tg::object::Id::new(tg::object::Kind::Blob, &value.to_le_bytes().to_vec().into())
}

fn process_grant(
	index: &Index,
	process: &tg::process::Id,
	object: &tg::object::Id,
	permission: tg::authorization::Permission,
) -> Option<super::super::grant::GrantValue> {
	let transaction = index.env.read_txn().unwrap();
	let key = Key::Grant(super::super::grant::Key::ResourceGrant {
		creator: Some(tg::Principal::Process(process.clone())),
		permission,
		resource: object.clone().into(),
		subject: tg::authorization::Subject::Process(process.clone()),
	});
	let key = Index::pack(&index.subspace, &key);
	let value = index.db.get(&transaction, &key).unwrap()?;

	Some(super::super::grant::GrantValue::deserialize(value).unwrap())
}
