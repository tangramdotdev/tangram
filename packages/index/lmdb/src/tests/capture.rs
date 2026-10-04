use {
	super::new_index,
	crate::{Config, Index},
	tangram_client::prelude::*,
	tangram_index::permission::capture::enqueue,
};

#[tokio::test]
async fn permission_capture_reads_are_bounded_and_leave_entries_until_completed() {
	let (_directory, index) = new_index();
	for byte in 1..=3 {
		let mut arg = arg(
			(tg::process::Id::new().into(), None),
			vec![root(object_id(&[byte]))],
		);
		arg.id = vec![byte; 16];
		enqueue(&index, arg).await;
	}
	assert!(
		index
			.permission_capture_batch(8, 1, 2)
			.await
			.unwrap()
			.is_empty()
	);
	assert!(
		index
			.permission_capture_batch(0, 0, 1)
			.await
			.unwrap()
			.is_empty()
	);
	let first = index.permission_capture_batch(2, 0, 1).await.unwrap();
	let second = index.permission_capture_batch(2, 0, 1).await.unwrap();
	assert_eq!(first.len(), 2);
	assert_eq!(
		first.iter().map(|entry| &entry.arg.id).collect::<Vec<_>>(),
		second.iter().map(|entry| &entry.arg.id).collect::<Vec<_>>()
	);
	assert!(first.iter().all(|entry| entry.partition == 0));
	for entry in &first {
		index.complete_permission_capture(entry).await.unwrap();
	}
	for entry in &first {
		index.complete_permission_capture(entry).await.unwrap();
	}
	let remaining = index.permission_capture_batch(8, 0, 1).await.unwrap();
	assert_eq!(remaining.len(), 1);
	assert_eq!(remaining[0].arg.id, vec![3; 16]);
	for entry in &remaining {
		index.complete_permission_capture(entry).await.unwrap();
	}
	assert!(
		index
			.permission_capture_batch(8, 0, 1)
			.await
			.unwrap()
			.is_empty()
	);
	let transaction = index.env.read_txn().unwrap();
	let prefix = index
		.subspace
		.pack(&(crate::Kind::PermissionCapture as i32,));
	assert_eq!(
		index.db.prefix_iter(&transaction, &prefix).unwrap().count(),
		0
	);
}

#[tokio::test]
async fn permission_capture_retries_failed_entries_with_new_arrivals() {
	let (_directory, index) = new_index();
	for byte in 1..=2 {
		let mut arg = arg(
			(tg::process::Id::new().into(), None),
			vec![root(object_id(&[byte]))],
		);
		arg.id = vec![byte; 16];
		enqueue(&index, arg).await;
	}
	let entries = index.permission_capture_batch(2, 0, 1).await.unwrap();
	assert_eq!(entries.len(), 2);
	index
		.complete_permission_capture(&entries[1])
		.await
		.unwrap();
	for byte in 3..=5 {
		let mut arg = arg(
			(tg::process::Id::new().into(), None),
			vec![root(object_id(&[byte]))],
		);
		arg.id = vec![byte; 16];
		enqueue(&index, arg).await;
		let retry = index.permission_capture_batch(2, 0, 1).await.unwrap();
		assert_eq!(retry.len(), 2);
		assert_eq!(retry[0].arg.id, entries[0].arg.id);
		assert_eq!(retry[1].arg.id, vec![byte; 16]);
		index.complete_permission_capture(&retry[1]).await.unwrap();
	}
}

#[tokio::test]
async fn restarting_permission_capture_retains_original_roots_and_partial_permissions() {
	let (directory, index) = new_index();
	let first = object_id(b"partial");
	let second = object_id(b"pending");
	let process = tg::process::Id::new();
	let mut first_root = root(first.clone());
	first_root.options.tokens = tokens(&first, "first");
	let arg = arg(
		(process.clone().into(), None),
		vec![first_root, root(second.clone())],
	);
	enqueue(&index, arg.clone()).await;
	let entries = index.permission_capture_batch(1, 0, 1).await.unwrap();
	assert_eq!(entries.len(), 1);
	let write = permission_args(
		0,
		(arg.resource.clone(), arg.version.clone()),
		vec![(first.clone().into(), node())],
	);
	write_permissions(&index, write.clone()).await;
	write_permissions(&index, write).await;
	drop(index);
	let config = Config {
		map_size: 1 << 30,
		max_process_depth: None,
		path: directory.path().join("index"),
		posix_sem_prefix: None,
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total: 1,
		write_operation_batch_size: 1,
	};
	let index = Index::new(&config).unwrap();
	let entries = index.permission_capture_batch(1, 0, 1).await.unwrap();
	assert_eq!(entries[0].arg.roots.len(), 2);
	assert_eq!(
		entries[0].arg.roots[0].options.tokens,
		arg.roots[0].options.tokens
	);
	let subject = tg::authorization::Subject::Process(process);
	assert_eq!(permissions(&index, &first, &subject).len(), 1);
	assert_eq!(permissions(&index, &second, &subject).len(), 0);
	let write = permission_args(
		0,
		(arg.resource.clone(), arg.version.clone()),
		vec![(second.clone().into(), node())],
	);
	write_permissions(&index, write).await;
	for entry in &entries {
		index.complete_permission_capture(entry).await.unwrap();
	}
	assert_eq!(permissions(&index, &second, &subject).len(), 1);
	assert!(
		index
			.permission_capture_batch(1, 0, 1)
			.await
			.unwrap()
			.is_empty()
	);
}

#[tokio::test]
async fn stale_tag_permission_capture_cannot_restore_permissions_or_delegations() {
	let (_directory, index) = new_index();
	let object = object_id(b"tagged");
	let id = tg::tag::Id::new();
	let mut tag = tangram_index::tag::put::Arg {
		account: None,
		id: id.clone(),
		name: "test".into(),
		parent: None,
		specifier: "test".parse().unwrap(),
		target: tg::Either::Left(object.clone()),
		touched_at: 0,
		version: "old".into(),
	};
	index.put_tags(std::slice::from_ref(&tag)).await.unwrap();
	let destination = (id.clone().into(), Some("old".into()));
	let arg = arg(destination.clone(), vec![root(object.clone())]);
	let delegation = tangram_index::delegation::put::Arg {
		expires_at: 100,
		resource: object.clone().into(),
		source: tg::authorization::Subject::Root,
		subject: tg::authorization::Subject::Tag(id.clone()),
		version: Some("old".into()),
	};
	let batch = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutDelegation(delegation.clone()),
			tangram_index::batch::Item::EnqueuePermissionCapture(arg.clone()),
		],
	};
	index.batch(batch).await.unwrap();
	enqueue(&index, arg.clone()).await;
	let entries = index.permission_capture_batch(1, 0, 1).await.unwrap();
	let write = permission_args(
		0,
		destination.clone(),
		vec![(object.clone().into(), subtree())],
	);
	write_permissions(&index, write).await;
	let subject = tg::authorization::Subject::Tag(id.clone());
	assert_eq!(permissions(&index, &object, &subject).len(), 1);
	tag.version = "new".into();
	tag.target = tg::Either::Left(object_id(b"new-target"));
	index.put_tags(&[tag]).await.unwrap();
	let mut writes = permission_args(0, destination, vec![(object.clone().into(), subtree())]);
	let process = tg::process::Id::new();
	writes.extend(permission_args(
		0,
		(process.clone().into(), None),
		vec![(object.clone().into(), subtree())],
	));
	let mut items = vec![tangram_index::batch::Item::PutDelegation(
		delegation.clone(),
	)];
	items.extend(
		writes
			.into_iter()
			.map(tangram_index::batch::Item::PutPermission),
	);
	let batch = tangram_index::batch::Arg { items };
	index.batch(batch).await.unwrap();
	let process_subject = tg::authorization::Subject::Process(process);
	assert_eq!(permissions(&index, &object, &process_subject).len(), 1);
	index.delete_tags(std::slice::from_ref(&id)).await.unwrap();
	let destination = (id.clone().into(), Some("new".into()));
	let writes = permission_args(0, destination, vec![(object.clone().into(), subtree())]);
	write_permissions(&index, writes).await;
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutDelegation(delegation)],
	};
	index.batch(batch).await.unwrap();
	enqueue(&index, arg).await;
	assert_eq!(
		permissions(
			&index,
			&object,
			&tg::authorization::Subject::Tag(id.clone())
		)
		.len(),
		0
	);
	for entry in &entries {
		index.complete_permission_capture(entry).await.unwrap();
	}
	assert!(
		index
			.permission_capture_batch(8, 0, 1)
			.await
			.unwrap()
			.is_empty()
	);
	let transaction = index.env.read_txn().unwrap();
	let prefix = Index::pack(
		&index.subspace,
		&(
			crate::Kind::DelegationSubject as i32,
			tg::authorization::Subject::Tag(id).to_string(),
		),
	);
	let delegations = Index::get_delegations_with_prefix(
		&index.db,
		&index.subspace,
		&transaction,
		&prefix,
		usize::MAX,
	)
	.unwrap();
	assert_eq!(delegations.len(), 0);
}

#[tokio::test]
async fn immediate_permissions_have_no_permission_capture_entry() {
	let (_directory, index) = new_index();
	let object = object_id(b"fast");
	let process = tg::process::Id::new();
	let write = permission_args(
		0,
		(process.clone().into(), None),
		vec![(object.clone().into(), subtree())],
	);
	write_permissions(&index, write).await;
	assert_ne!(
		permissions(
			&index,
			&object,
			&tg::authorization::Subject::Process(process)
		)
		.len(),
		0
	);
	assert!(
		index
			.permission_capture_batch(8, 0, 1)
			.await
			.unwrap()
			.is_empty()
	);
}

#[tokio::test]
async fn captured_permissions_preserve_node_permissions_on_each_descendant() {
	let (_directory, index) = new_index();
	let directory = object_id(b"directory");
	let file = object_id(b"file");
	let process = tg::process::Id::new();
	let roots = vec![root(directory.clone()), root(file.clone())];
	let write = permission_args(
		0,
		(process.clone().into(), None),
		roots.into_iter().map(|root| (root.node, node())).collect(),
	);
	write_permissions(&index, write).await;
	let subject = tg::authorization::Subject::Process(process);
	for object in [directory, file] {
		let permissions = permissions(&index, &object, &subject);
		assert_eq!(permissions.len(), 1);
		assert_eq!(
			permissions[0].permission,
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node
			)
		);
	}
}

async fn write_permissions(index: &Index, args: Vec<tangram_index::permission::put::Arg>) {
	index.put_permissions(&args).await.unwrap();
}

fn permission_args(
	created_at: i64,
	destination: (tg::Id, Option<String>),
	permissions: Vec<(tg::Id, tg::authorization::permission::Set)>,
) -> Vec<tangram_index::permission::put::Arg> {
	let (resource, version) = destination;
	let (creator, subject) = if resource.kind() == tg::id::Kind::Process {
		let id = tg::process::Id::try_from(resource).unwrap();
		(
			Some(tg::Principal::Process(id.clone())),
			tg::authorization::Subject::Process(id),
		)
	} else {
		let id = tg::tag::Id::try_from(resource).unwrap();
		(None, tg::authorization::Subject::Tag(id))
	};
	permissions
		.into_iter()
		.map(
			|(resource, permissions)| tangram_index::permission::put::Arg {
				created_at,
				creator: creator.clone(),
				permissions,
				resource,
				source: tangram_index::permission::Source::Direct { expires_at: None },
				subject: subject.clone(),
				time_to_touch: None,
				version: version.clone(),
			},
		)
		.collect()
}

#[tokio::test]
async fn process_children_and_objects_report_completeness_and_preserve_child_options() {
	let (_directory, index) = new_index();
	let process = tg::process::Id::new();
	let command = object_id(b"process-command");
	let output = object_id(b"process-output");
	let child = tg::process::Id::new();
	let options = tg::referent::Options {
		name: Some("child".into()),
		tokens: tokens(&output, "second"),
		..Default::default()
	};
	let referent = tg::Referent {
		node: child.clone(),
		options: options.clone(),
	};
	let child = tg::process::data::Child {
		cached: false,
		process: referent,
	};
	let mut process_arg = tangram_index::process::put::Arg {
		cached: false,
		children: Some(vec![child.clone()]),
		command: None,
		command_id: command.clone(),
		data: None,
		error: None,
		id: process.clone(),
		location: None,
		log: None,
		metadata: tg::process::Metadata::default(),
		options: tg::referent::Options::default(),
		output: None,
		parent: None,
		sandbox: None,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	};
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(process_arg.clone())],
	};
	index.batch(batch).await.unwrap();
	let children = index
		.try_get_process_children_and_objects(&process)
		.await
		.unwrap()
		.unwrap();
	assert!(!children.complete);
	process_arg.command = Some(vec![command.clone()]);
	process_arg.error = Some(None);
	process_arg.log = Some(None);
	process_arg.output = Some(Some(vec![output.clone()]));
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(process_arg)],
	};
	index.batch(batch).await.unwrap();
	let children = index
		.try_get_process_children_and_objects(&process)
		.await
		.unwrap()
		.unwrap();
	assert!(children.complete);
	let nodes = children.nodes;
	assert_eq!(nodes.len(), 3);
	assert!(
		nodes
			.iter()
			.any(|node| node.node == tg::Id::from(command.clone()))
	);
	assert!(
		nodes
			.iter()
			.any(|node| node.node == tg::Id::from(output.clone()))
	);
	let node = nodes
		.iter()
		.find(|node| node.node == tg::Id::from(child.process.node.clone()))
		.unwrap();
	assert_eq!(
		node.options,
		child.without_location_and_tokens().process.options
	);
}

fn object_id(bytes: &[u8]) -> tg::object::Id {
	tg::file::Id::new(bytes).into()
}

fn subtree() -> tg::authorization::permission::Set {
	tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Subtree,
	)
	.into()
}

fn node() -> tg::authorization::permission::Set {
	tg::authorization::Permission::Object(tg::authorization::permission::object::Permission::Node)
		.into()
}

fn root(object: tg::object::Id) -> tg::Referent<tg::Id> {
	tg::Referent::with_node(object.into())
}

fn arg(destination: (tg::Id, Option<String>), roots: Vec<tg::Referent<tg::Id>>) -> enqueue::Arg {
	let (resource, version) = destination;
	enqueue::Arg {
		id: vec![1; 16],
		principal: tg::Principal::Root,
		resource,
		roots,
		version,
	}
}

async fn enqueue(index: &Index, arg: enqueue::Arg) {
	index.enqueue_permission_capture(arg).await.unwrap();
}

fn permissions(
	index: &Index,
	object: &tg::object::Id,
	subject: &tg::authorization::Subject,
) -> Vec<crate::permission::PermissionEntry> {
	let transaction = index.env.read_txn().unwrap();
	let entries = Index::get_resource_permission_entries_for_subject_with_transaction(
		&index.db,
		&index.subspace,
		&transaction,
		&object.clone().into(),
		subject,
	)
	.unwrap();
	assert!(entries.iter().all(|entry| entry.direct == Some(None)));
	entries
}

fn tokens(object: &tg::object::Id, name: &str) -> tg::authorization::Tokens {
	let key = tg::authorization::PrivateKey::generate(name, tg::authorization::Algorithm::Ed25519)
		.unwrap();
	let body = tg::authorization::Body {
		expires_at: 100,
		permissions: vec![tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		)],
		resource: if name == "third" {
			object_id(b"remote-dependency").into()
		} else {
			object.clone().into()
		},
	};
	let token = tg::authorization::Token::sign(body, &key).unwrap();
	tg::authorization::Tokens::with_authorization([token])
}
