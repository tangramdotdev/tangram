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
async fn process_children_are_stored_separately_from_data() {
	let (_dir, index) = new_index();
	let mut ids = (0..3).map(|_| tg::process::Id::new()).collect::<Vec<_>>();
	ids.sort();
	ids.reverse();
	let expected = ids
		.into_iter()
		.enumerate()
		.map(|(index, id)| tg::process::data::Child {
			cached: index == 0,
			process: tg::Referent::with_node(id),
		})
		.collect::<Vec<_>>();
	let command = tg::command::Id::new(b"command");
	let missing = tg::process::Id::new();
	let process = tg::process::Id::new();
	let data = tg::process::Data {
		actual_checksum: None,
		cacheable: false,
		children: Some(expected.clone()),
		command: tg::Referent::with_node(command.clone()),
		created_at: 0,
		debug: None,
		error: None,
		exit: Some(0),
		expected_checksum: None,
		finished_at: Some(0),
		host: String::new(),
		log: None,
		output: None,
		retry: false,
		sandbox: tg::sandbox::Id::new(),
		started_at: Some(0),
		status: tg::process::Status::Finished,
		stderr: tg::process::Stdio::default(),
		stdin: tg::process::Stdio::default(),
		stdout: tg::process::Stdio::default(),
		tty: None,
	};
	let arg = crate::batch::Arg {
		items: vec![crate::batch::Item::PutProcess(crate::process::put::Arg {
			cached: false,
			children: Some(expected.clone()),
			command: command.into(),
			data: Some(data),
			error: Some(None),
			id: process.clone(),
			log: Some(None),
			metadata: tg::process::Metadata::default(),
			options: tg::referent::Options::default(),
			output: Some(None),
			parent: None,
			sandbox: None,
			storage: crate::process::Storage::default(),
			time_to_touch: std::time::Duration::ZERO,
			touched_at: 0,
		})],
	};
	index.batch(arg).await.unwrap();

	let indexed = index
		.try_get_processes(std::slice::from_ref(&process))
		.await
		.unwrap()
		.pop()
		.unwrap()
		.unwrap();
	assert!(indexed.data.unwrap().children.is_none());
	assert!(indexed.set.children);
	let children = index
		.try_get_process_children(&process, std::io::SeekFrom::Start(0), 10)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(children.len(), expected.len());
	for (actual, expected) in std::iter::zip(&children, &expected) {
		assert_eq!(actual.cached, expected.cached);
		assert_eq!(actual.process.node, expected.process.node);
	}
	let children = index
		.try_get_process_children(&process, std::io::SeekFrom::End(-2), 1)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(children.len(), 1);
	assert_eq!(children[0].cached, expected[1].cached);
	assert_eq!(children[0].process.node, expected[1].process.node);
	let children = index
		.try_get_process_children(&process, std::io::SeekFrom::Start(1), 2)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(
		children
			.iter()
			.map(|child| &child.process.node)
			.collect::<Vec<_>>(),
		expected[1..]
			.iter()
			.map(|child| &child.process.node)
			.collect::<Vec<_>>(),
	);
	let children = index
		.try_get_process_children(&process, std::io::SeekFrom::End(-2), 2)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(
		children
			.iter()
			.map(|child| &child.process.node)
			.collect::<Vec<_>>(),
		expected[1..]
			.iter()
			.map(|child| &child.process.node)
			.collect::<Vec<_>>(),
	);
	let children = index
		.try_get_process_children(&missing, std::io::SeekFrom::Start(0), 10)
		.await
		.unwrap();
	assert!(children.is_none());
}

#[tokio::test]
async fn incomplete_process_children_have_values() {
	let (_dir, index) = new_index();
	let child = tg::process::Id::new();
	let command = tg::command::Id::new(b"command");
	let parent = tg::process::Id::new();
	let entry = tg::referent::Options {
		name: Some("child".into()),
		..Default::default()
	};
	let child_data = tg::process::data::Child {
		cached: true,
		process: tg::Referent::new(child.clone(), entry),
	};
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutProcess(crate::process::put::Arg {
				cached: false,
				children: None,
				command: command.clone().into(),
				data: None,
				error: None,
				id: parent.clone(),
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
			crate::batch::Item::PutProcess(crate::process::put::Arg {
				cached: child_data.cached,
				children: None,
				command: command.into(),
				data: None,
				error: None,
				id: child.clone(),
				log: None,
				metadata: tg::process::Metadata::default(),
				options: child_data.process.options.clone(),
				output: None,
				parent: Some(parent.clone()),
				sandbox: None,
				storage: crate::process::Storage::default(),
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 0,
			}),
		],
	};
	index.batch(arg).await.unwrap();

	let process = index
		.try_get_processes(std::slice::from_ref(&parent))
		.await
		.unwrap()
		.pop()
		.unwrap()
		.unwrap();
	assert!(!process.set.children);
	let children = index
		.try_get_process_children(&parent, std::io::SeekFrom::Start(0), 1)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(children.len(), 1);
	assert!(children[0].cached);
	assert_eq!(children[0].process.node, child);
	assert_eq!(children[0].process.options.name.as_deref(), Some("child"));
}

#[tokio::test]
async fn process_children_must_be_unique() {
	let (_dir, index) = new_index();
	let child = tg::process::data::Child {
		cached: false,
		process: tg::Referent::with_node(tg::process::Id::new()),
	};
	let arg = crate::batch::Arg {
		items: vec![crate::batch::Item::PutProcess(crate::process::put::Arg {
			cached: false,
			children: Some(vec![child.clone(), child]),
			command: tg::command::Id::new(b"command").into(),
			data: None,
			error: None,
			id: tg::process::Id::new(),
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
	};
	let error = index.batch(arg).await.unwrap_err();
	assert!(
		error
			.to_string()
			.contains("process children must be unique")
	);
}

#[tokio::test]
async fn process_and_log_compaction_share_transaction() {
	let (_dir, index) = new_index();
	let process = tg::process::Id::new();
	let before = index.get_transaction_id().await.unwrap();
	let arg = crate::batch::Arg {
		items: vec![
			crate::batch::Item::PutProcess(crate::process::put::Arg {
				cached: false,
				children: Some(Vec::new()),
				command: tg::command::Id::new(b"command").into(),
				data: None,
				error: Some(None),
				id: process.clone(),
				log: Some(None),
				metadata: tg::process::Metadata::default(),
				options: tg::referent::Options::default(),
				output: Some(None),
				parent: None,
				sandbox: None,
				storage: crate::process::Storage::default(),
				time_to_touch: std::time::Duration::ZERO,
				touched_at: 0,
			}),
			crate::batch::Item::EnqueueLogCompaction(process.clone()),
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
	let entries = index.log_compaction_batch(1).await.unwrap();
	assert_eq!(entries.len(), 1);
	assert_eq!(entries[0].process, process);
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
