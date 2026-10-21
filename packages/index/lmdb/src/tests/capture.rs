use {
	super::new_index,
	crate::{Config, Index},
	std::collections::BTreeSet,
	tangram_client::prelude::*,
	tangram_index::permission::capture::{Capture, enqueue},
};

#[tokio::test]
async fn permission_capture_queue_coalesces_and_preserves_new_attempts() {
	let (_directory, index) = new_index();
	let resource = object_id(b"queued").into();
	let arg = enqueue::Arg { resource };
	index.enqueue_permission_capture(arg.clone()).await.unwrap();
	let first = index
		.permission_capture_batch(1, 0, 1)
		.await
		.unwrap()
		.pop()
		.unwrap();
	index.enqueue_permission_capture(arg.clone()).await.unwrap();
	index.complete_permission_capture(&first).await.unwrap();
	let second = index.permission_capture_batch(8, 0, 1).await.unwrap();
	assert_eq!(second.len(), 1);
	assert_ne!(second[0].attempt, first.attempt);
	index.complete_permission_capture(&second[0]).await.unwrap();
	assert!(
		index
			.permission_capture_batch(8, 0, 1)
			.await
			.unwrap()
			.is_empty()
	);
	// A stale completion must also leave an attempt enqueued after deletion intact.
	index.enqueue_permission_capture(arg).await.unwrap();
	index.complete_permission_capture(&second[0]).await.unwrap();
	assert_eq!(
		index.permission_capture_batch(8, 0, 1).await.unwrap().len(),
		1
	);
	assert!(
		index
			.permission_capture_batch(0, 0, 1)
			.await
			.unwrap()
			.is_empty()
	);
	assert!(
		index
			.permission_capture_batch(8, 1, 2)
			.await
			.unwrap()
			.is_empty()
	);
}

#[tokio::test]
async fn permission_capture_records_preserve_sources_and_conditional_deletion() {
	let (_directory, index) = new_index();
	let object = object_id(b"contexts");
	let mut first = arg(
		(tg::process::Id::new().into(), None),
		vec![root(object.clone())],
	);
	first.resource.options.tokens = tokens(&object, "first");
	let mut second = first.clone();
	second.principal = tg::Principal::Anonymous;
	enqueue(&index, first.clone()).await;
	enqueue(&index, second.clone()).await;
	enqueue(&index, first.clone()).await;
	let records = index
		.get_permission_captures(&object.clone().into())
		.await
		.unwrap();
	assert_eq!(records.len(), 2);
	assert!(records.contains(&first));
	assert!(records.contains(&second));
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::DeletePermissionCapture(first)],
	};
	index.batch(batch).await.unwrap().unwrap();
	let records = index.get_permission_captures(&object.into()).await.unwrap();
	assert_eq!(records.len(), 1);
	assert_eq!(records[0].principal, second.principal);
}

#[tokio::test]
async fn restarting_permission_capture_retains_context_and_queue() {
	let (directory, index) = new_index();
	let object = object_id(b"restart");
	let mut capture = arg(
		(tg::process::Id::new().into(), None),
		vec![root(object.clone())],
	);
	capture.resource.options.tokens = tokens(&object, "first");
	enqueue(&index, capture.clone()).await;
	drop(index);
	let config = Config {
		map_size: 1 << 30,
		max_process_depth: None,
		max_readers: 1_000,
		object_permission_time_to_touch: std::time::Duration::ZERO,
		path: directory.path().join("index"),
		posix_sem_prefix: None,
		process_data_fragment_size: 16,
		process_permission_time_to_touch: std::time::Duration::ZERO,
		read_channel_capacity: 256,
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total: 1,
		write_channel_capacity: 256,
		write_operation_batch_size: 1,
	};
	let index = Index::new(&config).unwrap();
	let records = index
		.get_permission_captures(&object.clone().into())
		.await
		.unwrap();
	assert_eq!(records.len(), 1);
	assert_eq!(
		records[0].resource.options.tokens,
		capture.resource.options.tokens
	);
	let entry = index
		.permission_capture_batch(1, 0, 1)
		.await
		.unwrap()
		.pop()
		.unwrap();
	assert_eq!(entry.resource, tg::Id::from(object));
	index.complete_permission_capture(&entry).await.unwrap();
	assert_eq!(
		index
			.get_permission_captures(&entry.resource)
			.await
			.unwrap()
			.len(),
		1
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
			tangram_index::batch::Item::PutPermissionCapture(arg.clone()),
		],
	};
	index.batch(batch).await.unwrap().unwrap();
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
	index.batch(batch).await.unwrap().unwrap();
	let process_subject = tg::authorization::Subject::Process(process);
	assert_eq!(permissions(&index, &object, &process_subject).len(), 1);
	index.delete_tags(std::slice::from_ref(&id)).await.unwrap();
	let destination = (id.clone().into(), Some("new".into()));
	let writes = permission_args(0, destination, vec![(object.clone().into(), subtree())]);
	write_permissions(&index, writes).await;
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutDelegation(delegation)],
	};
	index.batch(batch).await.unwrap().unwrap();
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
	assert_eq!(
		index
			.get_permission_captures(&object.clone().into())
			.await
			.unwrap(),
		[]
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
		permissions: Vec::new(),
		principal: tg::Principal::Process(process.clone()),
		sandbox: None,
		storage: tg::process::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	};
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutProcess(process_arg.clone())],
	};
	index.batch(batch).await.unwrap().unwrap();
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
	index.batch(batch).await.unwrap().unwrap();
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
	assert_eq!(node.options, child.process.options);
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

fn arg(destination: (tg::Id, Option<String>), roots: Vec<tg::Referent<tg::Id>>) -> Capture {
	let (subject, version) = destination;
	Capture {
		expires_at: 1_000,
		principal: tg::Principal::Root,
		resource: roots.into_iter().next().unwrap(),
		sources: BTreeSet::new(),
		subject,
		version,
	}
}

async fn enqueue(index: &Index, arg: Capture) {
	let mut items = vec![tangram_index::batch::Item::PutPermissionCapture(
		arg.clone(),
	)];
	for root in std::iter::once(arg.resource) {
		let enqueue = enqueue::Arg {
			resource: root.node,
		};
		items.push(tangram_index::batch::Item::EnqueuePermissionCapture(
			enqueue,
		));
	}
	let batch = tangram_index::batch::Arg { items };
	index.batch(batch).await.unwrap().unwrap();
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

#[tokio::test]
async fn terminal_capture_removes_all_contexts_and_the_reverse_key() {
	let (_directory, index) = new_index();
	let object = object_id(b"terminal");
	let process = tg::process::Id::new();
	let capture = arg((process.clone().into(), None), vec![root(object.clone())]);
	enqueue(&index, capture.clone()).await;
	let mut other = capture.clone();
	other.principal = tg::Principal::Anonymous;
	enqueue(&index, other).await;
	let writes = permission_args(
		0,
		(process.clone().into(), None),
		vec![(object.clone().into(), subtree())],
	);
	write_permissions(&index, writes).await;
	enqueue(&index, capture).await;
	assert_eq!(
		index.get_permission_captures(&object.into()).await.unwrap(),
		[]
	);
	let transaction = index.env.read_txn().unwrap();
	let prefix = index.subspace.pack(&(
		crate::Kind::PermissionCaptureSubject as i32,
		tg::Id::from(process).to_bytes().as_ref(),
	));
	assert_eq!(
		index.db.prefix_iter(&transaction, &prefix).unwrap().count(),
		0
	);
}

#[tokio::test]
async fn permission_updates_notify_capture_only_for_subjects_that_can_extend_authority() {
	let (_directory, index) = new_index();

	// Add a capture for a user without a notification.
	let object = object_id(b"notify");
	let process = tg::process::Id::new();
	let user = tg::user::Id::new();
	let mut capture = arg((process.clone().into(), None), vec![root(object.clone())]);
	capture.principal = tg::Principal::User(user.clone());
	capture.sources = BTreeSet::from([tg::authorization::Subject::User(user.clone())]);
	let batch = tangram_index::batch::Arg {
		items: vec![tangram_index::batch::Item::PutPermissionCapture(capture)],
	};
	index.batch(batch).await.unwrap().unwrap();

	// Verify that permissions for an unrelated user and for the capture subject do not notify the worker.
	let other = tg::authorization::Subject::User(tg::user::Id::new());
	assert!(!notified(&index, &object, other).await);
	let subject = tg::authorization::Subject::Process(process);
	assert!(!notified(&index, &object, subject).await);

	// Verify that permissions for the source and for subjects that can reach the principal notify the worker.
	let subjects = [
		tg::authorization::Subject::User(user),
		tg::authorization::Subject::Group(tg::group::Id::new()),
		tg::authorization::Subject::Process(tg::process::Id::new()),
		tg::authorization::Subject::Sync(tg::sync::Id::new()),
	];
	for subject in subjects {
		assert!(notified(&index, &object, subject).await);
	}
}

#[tokio::test]
async fn permission_captures_extend_and_expire() {
	let (_directory, index) = new_index();
	let object = object_id(b"expire");
	let user = tg::user::Id::new();
	let resource = tg::Id::from(object.clone());
	let source = tg::authorization::Subject::User(user.clone());

	// Put a capture and then the same capture with a later expiration.
	let mut capture = arg(
		(tg::process::Id::new().into(), None),
		vec![root(object.clone())],
	);
	capture.expires_at = 10;
	capture.principal = tg::Principal::User(user);
	capture.sources = BTreeSet::from([source.clone()]);
	let mut extended = capture.clone();
	extended.expires_at = 30;
	let batch = tangram_index::batch::Arg {
		items: vec![
			tangram_index::batch::Item::PutPermissionCapture(capture),
			tangram_index::batch::Item::PutPermissionCapture(extended.clone()),
		],
	};
	index.batch(batch).await.unwrap().unwrap();
	assert_eq!(
		index.get_permission_captures(&resource).await.unwrap(),
		vec![extended.clone()]
	);

	// Clean before the later expiration and verify that the capture remains.
	clean(&index, 20).await;
	assert_eq!(
		index.get_permission_captures(&resource).await.unwrap(),
		vec![extended]
	);

	// Clean after the later expiration and verify that the capture and its source key are removed.
	clean(&index, 40).await;
	assert_eq!(
		index.get_permission_captures(&resource).await.unwrap(),
		Vec::new()
	);
	let transaction = index.env.read_txn().unwrap();
	assert!(
		!Index::has_permission_captures_for_source_with_transaction(
			&index.db,
			&index.subspace,
			&transaction,
			&resource,
			&source,
		)
		.unwrap()
	);
}

async fn clean(index: &Index, now: i64) {
	let arg = tangram_index::clean::Arg {
		batch_size: 100,
		max_object_touched_at: i64::MIN,
		max_process_touched_at: i64::MIN,
		max_sandbox_touched_at: i64::MIN,
		now,
		partition_end: 1,
		partition_start: 0,
	};
	index.clean(arg).await.unwrap();
}

async fn notified(
	index: &Index,
	object: &tg::object::Id,
	subject: tg::authorization::Subject,
) -> bool {
	// Grant permissions to the subject and index them.
	let source = tangram_index::permission::Source::Direct {
		expires_at: Some(i64::MAX),
	};
	let arg = tangram_index::permission::put::Arg {
		created_at: 0,
		creator: None,
		permissions: subtree(),
		resource: object.clone().into(),
		source,
		subject,
		time_to_touch: None,
		version: None,
	};
	write_permissions(index, vec![arg]).await;
	index
		.update_batch(tangram_index::update::Kind::Permission, 100)
		.await
		.unwrap();

	// Read and complete the notifications.
	let entries = index.permission_capture_batch(8, 0, 1).await.unwrap();
	for entry in &entries {
		index.complete_permission_capture(entry).await.unwrap();
	}

	!entries.is_empty()
}
