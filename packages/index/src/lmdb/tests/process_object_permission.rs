use {
	super::{
		super::{Index, Key},
		new_index,
	},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

#[tokio::test]
async fn process_object_permissions_walk_and_write_in_one_batch() {
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
			put: [1; 16],
			storage: crate::object::Storage::default(),
			time_to_touch: std::time::Duration::ZERO,
			touched_at: 0,
		})
	};
	let put_permission = |resource: tg::object::Id, permission: tg::authorization::Permission| {
		crate::batch::Item::PutPermission(crate::permission::put::Arg {
			created_at: 0,
			creator: Some(creator.clone()),
			permissions: permission.into(),
			resource: tg::Id::from(resource),
			source: crate::permission::Source::Direct {
				expires_at: Some(100),
			},
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
				put_permission(root.clone(), node),
				put_permission(child.clone(), node),
				put_permission(leaf.clone(), subtree),
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
					command: Some(vec![command.clone().into()]),
					command_id: command.into(),
					data: None,
					error: None,
					id: process.clone(),
					location: None,
					log: None,
					metadata: tg::process::Metadata::default(),
					options: tg::referent::Options::default(),
					output: Some(Some(vec![root.clone()])),
					parent: None,
					sandbox: None,
					storage: crate::process::Storage::default(),
					time_to_touch: std::time::Duration::ZERO,
					touched_at: 0,
				}),
				crate::batch::Item::PutProcessObjectPermissions(
					crate::process::object::permission::Arg {
						authorize: crate::authorize::Config::default(),
						created_at: 0,
						expires_at: None,
						principal: creator.clone(),
						process: process.clone(),
						roots: vec![crate::process::object::permission::Root {
							object: root.clone(),
							permissions: None,
						}],
						time_to_touch: None,
					},
				),
			],
		})
		.await
		.unwrap();

	assert_eq!(
		process_permission(&index, &process, &root, node)
			.unwrap()
			.direct,
		Some(None)
	);
	assert_eq!(
		process_permission(&index, &process, &child, node)
			.unwrap()
			.direct,
		Some(None)
	);
	assert_eq!(
		process_permission(&index, &process, &leaf, subtree)
			.unwrap()
			.direct,
		Some(None)
	);
	assert!(process_permission(&index, &process, &root_inaccessible, node).is_none());
	assert!(process_permission(&index, &process, &child_inaccessible, node).is_none());
}

#[tokio::test]
async fn process_object_permissions_require_search_unless_subtree_is_proven() {
	let search = crate::authorize::SearchConfig {
		max_depth: 0,
		max_edges: 0,
		max_nodes: 0,
		..Default::default()
	};
	let authorize = crate::authorize::Config {
		ancestor: search,
		descendant: search,
		..Default::default()
	};
	let (_dir, index) = new_index();
	let object = object_id(0);
	index
		.batch(crate::batch::Arg {
			items: vec![crate::batch::Item::PutObject(crate::object::put::Arg {
				checkout: None,
				children: BTreeSet::new(),
				id: object.clone(),
				metadata: tg::object::Metadata::default(),
				put: [1; 16],
				storage: crate::object::Storage::default(),
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 0,
			})],
		})
		.await
		.unwrap();
	let node = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Node,
	);
	let subtree = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	);
	for permissions in [None, Some(node.into()), Some(subtree.into())] {
		let process = tg::process::Id::new();
		let root = crate::process::object::permission::Root {
			object: object.clone(),
			permissions,
		};
		let permission = crate::process::object::permission::Arg {
			authorize,
			created_at: 0,
			expires_at: None,
			principal: tg::Principal::Process(process.clone()),
			process: process.clone(),
			roots: vec![root],
			time_to_touch: None,
		};
		let arg = crate::batch::Arg {
			items: vec![crate::batch::Item::PutProcessObjectPermissions(permission)],
		};
		let result = index.batch(arg).await;
		if permissions.is_some_and(|permissions| permissions.contains(subtree)) {
			result.unwrap();
			assert_eq!(
				process_permission(&index, &process, &object, subtree)
					.unwrap()
					.direct,
				Some(None)
			);
		} else {
			let error = result.unwrap_err();
			assert!(
				error
					.to_string()
					.contains("process object permission authorization search exhausted")
			);
			assert!(process_permission(&index, &process, &object, subtree).is_none());
		}
		assert!(process_permission(&index, &process, &object, node).is_none());
	}
}

#[tokio::test]
async fn process_object_permissions_require_permanent_permissions() {
	let (_dir, index) = new_index();
	let process = tg::process::Id::new();
	let reader = tg::user::Id::new();
	let node_reader = tg::user::Id::new();
	let root = object_id(10);
	let child = object_id(11);
	let subtree = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	);
	let node = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Node,
	);
	let mut items = Vec::new();
	for (id, children) in [
		(root.clone(), BTreeSet::from([child.clone()])),
		(child.clone(), BTreeSet::new()),
	] {
		let arg = crate::object::put::Arg {
			checkout: None,
			children,
			id,
			metadata: tg::object::Metadata::default(),
			put: [1; 16],
			storage: crate::object::Storage::default(),
			time_to_touch: std::time::Duration::ZERO,
			touched_at: 0,
		};
		items.push(crate::batch::Item::PutObject(arg));
	}
	for (reader, permission) in [
		(
			&reader,
			tg::authorization::permission::process::Permission::NodeOutput,
		),
		(
			&node_reader,
			tg::authorization::permission::process::Permission::Node,
		),
	] {
		let arg = crate::permission::put::Arg {
			created_at: 0,
			creator: None,
			permissions: tg::authorization::Permission::Process(permission).into(),
			resource: process.clone().into(),
			source: crate::permission::Source::Grant,
			subject: tg::authorization::Subject::User(reader.clone()),
			time_to_touch: None,
		};
		items.push(crate::batch::Item::PutPermission(arg));
	}
	let arg = crate::batch::Arg { items };
	index.batch(arg).await.unwrap();
	let process_arg = crate::process::put::Arg {
		cached: false,
		children: None,
		command: Some(vec![tg::command::Id::new(b"command").into()]),
		command_id: tg::command::Id::new(b"command").into(),
		data: None,
		error: None,
		id: process.clone(),
		location: None,
		log: None,
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: Some(Some(vec![root.clone()])),
		parent: None,
		sandbox: None,
		storage: crate::process::Storage::default(),
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	};
	let disabled = crate::authorize::SearchConfig {
		max_depth: 0,
		max_edges: 0,
		max_nodes: 0,
		page_size: 1,
	};
	let configs = [
		crate::authorize::Config {
			descendant: disabled,
			..Default::default()
		},
		crate::authorize::Config {
			ancestor: disabled,
			..Default::default()
		},
	];
	let args = [root.clone(), child.clone()]
		.into_iter()
		.map(|object| crate::authorize::Arg {
			requested: subtree.into(),
			required: subtree.into(),
			resource: tg::Selector::Id(object.into()),
			tokens: Vec::new(),
		})
		.collect::<Vec<_>>();

	// Add a permanent permission, then replay the relationship without changing the permission.
	for (proven, expected) in [(false, false), (true, true), (false, true)] {
		if proven {
			let permission = crate::permission::put::Arg {
				created_at: 0,
				creator: Some(tg::Principal::Process(process.clone())),
				permissions: subtree.into(),
				resource: root.clone().into(),
				source: crate::permission::Source::Direct { expires_at: None },
				subject: tg::authorization::Subject::Process(process.clone()),
				time_to_touch: None,
			};
			let arg = crate::batch::Arg {
				items: vec![crate::batch::Item::PutPermission(permission)],
			};
			index.batch(arg).await.unwrap();
		}
		let arg = crate::batch::Arg {
			items: vec![crate::batch::Item::PutProcess(process_arg.clone())],
		};
		index.batch(arg).await.unwrap();
		let transaction = index.env.read_txn().unwrap();
		let key = Key::Process(super::super::process::Key::ProcessObject {
			kind: crate::process::object::Kind::Output,
			object: root.clone(),
			process: process.clone(),
		});
		let key = Index::pack(&index.subspace, &key);
		assert_eq!(
			index.db.get(&transaction, &key).unwrap(),
			Some([].as_slice())
		);
		drop(transaction);
		for config in configs {
			for principal in [
				tg::Principal::User(reader.clone()),
				tg::Principal::Process(process.clone()),
			] {
				let outcomes = index
					.authorize_batch(&args, config, &principal)
					.await
					.unwrap();
				assert_eq!(
					outcomes
						.iter()
						.all(|outcome| matches!(outcome, crate::authorize::Outcome::Authorized(_))),
					expected
				);
			}
			let outcomes = index
				.authorize_batch(&args, config, &tg::Principal::User(node_reader.clone()))
				.await
				.unwrap();
			assert!(
				outcomes
					.iter()
					.all(|outcome| !matches!(outcome, crate::authorize::Outcome::Authorized(_)))
			);
		}
		assert_eq!(
			process_permission(&index, &process, &root, subtree).is_some(),
			expected
		);
		assert!(process_permission(&index, &process, &root, node).is_none());
	}
}

fn object_id(value: u64) -> tg::object::Id {
	tg::object::Id::new(tg::object::Kind::Blob, &value.to_le_bytes().to_vec().into())
}

fn process_permission(
	index: &Index,
	process: &tg::process::Id,
	object: &tg::object::Id,
	permission: tg::authorization::Permission,
) -> Option<super::super::permission::PermissionValue> {
	let transaction = index.env.read_txn().unwrap();
	let key = Key::Permission(super::super::permission::Key::ResourcePermission {
		creator: Some(tg::Principal::Process(process.clone())),
		permission,
		resource: object.clone().into(),
		subject: tg::authorization::Subject::Process(process.clone()),
	});
	let key = Index::pack(&index.subspace, &key);
	let value = index.db.get(&transaction, &key).unwrap()?;

	Some(super::super::permission::PermissionValue::deserialize(value).unwrap())
}
