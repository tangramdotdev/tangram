use {
	super::super::{
		Config, Index, Key, grant::Key as GrantKey, group::Key as GroupKey,
		object::Key as ObjectKey, organization::Key as OrganizationKey, process::Key as ProcessKey,
		sandbox::Key as SandboxKey,
	},
	heed as lmdb,
	std::time::Instant,
	tangram_client::prelude::*,
};

fn object_id(n: usize) -> tg::object::Id {
	tg::object::Id::new(tg::object::Kind::Blob, &n.to_le_bytes().to_vec().into())
}

fn put(index: &Index, txn: &mut lmdb::RwTxn<'_>, key: &Key) {
	let key = Index::pack(&index.subspace, key);
	index.db.put(txn, &key, &[]).unwrap();
}

fn put_value(index: &Index, txn: &mut lmdb::RwTxn<'_>, key: &Key, value: &[u8]) {
	let key = Index::pack(&index.subspace, key);
	index.db.put(txn, &key, value).unwrap();
}

fn object_permission(
	permission: tg::authorization::permission::object::Permission,
) -> tg::authorization::Permission {
	tg::authorization::Permission::Object(permission)
}

fn object_permissions(
	permissions: impl IntoIterator<Item = tg::authorization::permission::object::Permission>,
) -> tg::authorization::permission::Set {
	let mut set = tg::authorization::permission::object::Set::empty();
	for permission in permissions {
		set.insert(tg::authorization::permission::object::Set::from_permission(
			permission,
		));
	}
	tg::authorization::permission::Set::Object(set)
}

fn put_object(index: &Index, txn: &mut lmdb::RwTxn<'_>, object: &tg::object::Id) {
	put(index, txn, &Key::Object(ObjectKey::Object(object.clone())));
}

fn put_child(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	parent: &tg::object::Id,
	child: &tg::object::Id,
) {
	put(
		index,
		txn,
		&Key::Object(ObjectKey::ObjectChild {
			object: parent.clone(),
			child: child.clone(),
		}),
	);
	put(
		index,
		txn,
		&Key::Object(ObjectKey::ChildObject {
			child: child.clone(),
			object: parent.clone(),
		}),
	);
}

fn put_grant(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	resource: &tg::object::Id,
	user: &tg::user::Id,
	permission: tg::authorization::permission::object::Permission,
) {
	put_resource_grant(
		index,
		txn,
		resource.clone().into(),
		tg::authorization::Subject::User(user.clone()),
		object_permission(permission),
	);
}

fn put_process(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	process: &tg::process::Id,
	sandbox: &tg::sandbox::Id,
) {
	let value = crate::process::Process {
		data: None,
		metadata: tg::process::Metadata::default(),
		reference_count: 0,
		sandbox: Some(sandbox.clone()),
		set: crate::process::Set::default(),
		stored: crate::process::Stored::default(),
		touched_at: 0,
	}
	.serialize()
	.unwrap();
	put_value(
		index,
		txn,
		&Key::Process(ProcessKey::Process(process.clone())),
		&value,
	);
}

fn put_process_child(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	process: &tg::process::Id,
	child: &tg::process::Id,
) {
	put(
		index,
		txn,
		&Key::Process(ProcessKey::ProcessChild {
			child: child.clone(),
			process: process.clone(),
		}),
	);
	put(
		index,
		txn,
		&Key::Process(ProcessKey::ChildProcess {
			child: child.clone(),
			parent: process.clone(),
		}),
	);
}

fn put_process_object(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	process: &tg::process::Id,
	object: &tg::object::Id,
	kind: crate::process::object::Kind,
) {
	put(
		index,
		txn,
		&Key::Process(ProcessKey::ProcessObject {
			kind,
			object: object.clone(),
			process: process.clone(),
		}),
	);
}

fn put_resource_grant(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	resource: tg::Id,
	subject: tg::authorization::Subject,
	permission: tg::authorization::Permission,
) {
	put(
		index,
		txn,
		&Key::Grant(GrantKey::ResourceGrant {
			creator: None,
			permission,
			resource,
			subject,
		}),
	);
}

fn put_sandbox(index: &Index, txn: &mut lmdb::RwTxn<'_>, sandbox: &tg::sandbox::Id) {
	let value = crate::sandbox::Sandbox {
		account: None,
		created_at: 0,
		data: None,
		reference_count: 0,
		runner: None,
		touched_at: 0,
	}
	.serialize()
	.unwrap();
	put_value(
		index,
		txn,
		&Key::Sandbox(SandboxKey::Sandbox(sandbox.clone())),
		&value,
	);
}

fn new_index() -> (tempfile::TempDir, Index) {
	let dir = tempfile::TempDir::new().unwrap();
	let index = Index::new(&Config {
		authorize: super::super::AuthorizeConfig {
			object_subtree: crate::authorize::ObjectSubtreeConfig::default(),
			process_subtree: crate::authorize::ProcessSubtreeConfig::default(),
		},
		map_size: 1 << 30,
		max_process_depth: None,
		path: dir.path().join("index"),
		read_batch_size: 64,
		read_concurrency: 4,
		usage_partition_total: 1,
		write_batch_size: 100_000,
	})
	.unwrap();
	(dir, index)
}

async fn authorize(
	index: &Index,
	args: Vec<crate::authorize::Arg>,
	user: &tg::user::Id,
) -> Vec<Option<crate::authorize::Output>> {
	index
		.authorize_batch(&args, &tg::Principal::User(user.clone()))
		.await
		.unwrap()
}

async fn is_authorized(
	index: &Index,
	resource: tg::Id,
	permission: tg::authorization::Permission,
	principal: &tg::Principal,
) -> bool {
	let permissions = tg::authorization::permission::Set::from(permission);
	let arg = crate::authorize::Arg {
		permissions,
		resource: tg::Selector::Id(resource),
		token: None,
	};
	let output = index.authorize_batch(&[arg], principal).await.unwrap();
	output[0]
		.as_ref()
		.is_some_and(|output| output.permissions.contains(permissions))
}

async fn authorize_secs(index: &Index, resource: &tg::object::Id, user: &tg::user::Id) -> f64 {
	let node = object_permissions([tg::authorization::permission::object::Permission::Node]);
	let arg = crate::authorize::Arg {
		permissions: node,
		resource: tg::Selector::Id(resource.clone().into()),
		token: None,
	};
	let start = Instant::now();
	let output = index
		.authorize_batch(&[arg], &tg::Principal::User(user.clone()))
		.await
		.unwrap();
	let elapsed = start.elapsed().as_secs_f64();
	assert!(
		output[0]
			.as_ref()
			.is_some_and(|output| output.permissions.contains(node)),
		"the node should be authorized via the root's subtree grant"
	);
	elapsed
}

async fn deny_secs(index: &Index, resource: &tg::object::Id, user: &tg::user::Id) -> f64 {
	let node = object_permissions([tg::authorization::permission::object::Permission::Node]);
	let arg = crate::authorize::Arg {
		permissions: node,
		resource: tg::Selector::Id(resource.clone().into()),
		token: None,
	};
	let start = Instant::now();
	let output = index
		.authorize_batch(&[arg], &tg::Principal::User(user.clone()))
		.await
		.unwrap();
	let elapsed = start.elapsed().as_secs_f64();
	assert!(!output[0].as_ref().unwrap().permissions.contains(node));
	elapsed
}

#[tokio::test]
async fn authorize_new_specifier_with_parent_write_permission() {
	let (_dir, index) = new_index();
	let alice = tg::user::Id::new();
	let group = tg::group::Id::new();
	let outsider = tg::user::Id::new();
	let writer = tg::user::Id::new();
	let mut txn = index.env.write_txn().unwrap();
	Index::put_users_with_transaction(
		&index.db,
		&index.subspace,
		&mut txn,
		&[crate::user::put::Arg {
			billing: Some(false),
			id: alice.clone(),
			specifier: "alice".parse().unwrap(),
		}],
	)
	.unwrap();
	Index::put_groups_with_transaction(
		&index.db,
		&index.subspace,
		&mut txn,
		&[crate::group::put::Arg {
			id: group,
			parent: Some(alice.clone().into()),
			specifier: "alice/taken".parse().unwrap(),
		}],
	)
	.unwrap();
	put_resource_grant(
		&index,
		&mut txn,
		alice.into(),
		tg::authorization::Subject::User(writer.clone()),
		tg::authorization::Permission::User(tg::authorization::permission::user::Permission::Write),
	);
	txn.commit().unwrap();

	let permission =
		tg::authorization::Permission::Tag(tg::authorization::permission::tag::Permission::Write);
	let permissions = tg::authorization::permission::Set::from_permission(permission);
	let args = [
		crate::authorize::Arg {
			permissions,
			resource: tg::Selector::Specifier("alice/new".parse().unwrap()),
			token: None,
		},
		crate::authorize::Arg {
			permissions,
			resource: tg::Selector::Specifier("alice/taken".parse().unwrap()),
			token: None,
		},
		crate::authorize::Arg {
			permissions,
			resource: tg::Selector::Specifier("unclaimed/new".parse().unwrap()),
			token: None,
		},
	];
	let outputs = index
		.authorize_batch(&args, &tg::Principal::User(writer))
		.await
		.unwrap();
	assert!(
		outputs[0]
			.as_ref()
			.is_some_and(|output| output.permissions.contains(permission))
	);
	assert!(outputs[1].is_none());
	assert!(outputs[2].is_none());

	let outputs = index
		.authorize_batch(&args[..1], &tg::Principal::User(outsider))
		.await
		.unwrap();
	assert!(
		outputs[0]
			.as_ref()
			.is_some_and(|output| !output.permissions.contains(permission))
	);
}

#[tokio::test]
async fn authorize_inherits_a_process_grant_through_its_sandbox() {
	let (_dir, index) = new_index();
	let node_reader = tg::user::Id::new();
	let object = object_id(0);
	let outsider = tg::user::Id::new();
	let process = tg::process::Id::new();
	let process_reader = tg::user::Id::new();
	let process_writer = tg::user::Id::new();
	let sandbox = tg::sandbox::Id::new();
	let sandbox_reader = tg::user::Id::new();
	let sandbox_writer = tg::user::Id::new();
	let subtree_reader = tg::user::Id::new();
	let target = tg::sandbox::Id::new();
	let node = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Node,
	);
	let process_read = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Read,
	);
	let process_write = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Write,
	);
	let sandbox_read = tg::authorization::Permission::Sandbox(
		tg::authorization::permission::sandbox::Permission::Read,
	);
	let sandbox_write = tg::authorization::Permission::Sandbox(
		tg::authorization::permission::sandbox::Permission::Write,
	);
	let process_subtree = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Subtree,
	);
	let subtree = object_permission(tg::authorization::permission::object::Permission::Subtree);
	let mut txn = index.env.write_txn().unwrap();
	put_object(&index, &mut txn, &object);
	put_sandbox(&index, &mut txn, &sandbox);
	put_sandbox(&index, &mut txn, &target);
	put_process(&index, &mut txn, &process, &sandbox);
	put_resource_grant(
		&index,
		&mut txn,
		object.clone().into(),
		tg::authorization::Subject::Process(process.clone()),
		subtree,
	);
	put_resource_grant(
		&index,
		&mut txn,
		target.clone().into(),
		tg::authorization::Subject::Process(process.clone()),
		sandbox_write,
	);
	put_resource_grant(
		&index,
		&mut txn,
		sandbox.clone().into(),
		tg::authorization::Subject::User(sandbox_reader.clone()),
		sandbox_read,
	);
	put_resource_grant(
		&index,
		&mut txn,
		sandbox.clone().into(),
		tg::authorization::Subject::User(sandbox_writer.clone()),
		sandbox_write,
	);
	for (user, permission) in [
		(node_reader.clone(), node),
		(process_reader.clone(), process_read),
		(process_writer.clone(), process_write),
		(subtree_reader.clone(), process_subtree),
	] {
		put_resource_grant(
			&index,
			&mut txn,
			process.clone().into(),
			tg::authorization::Subject::User(user),
			permission,
		);
	}
	txn.commit().unwrap();

	for (principal, expected_read, expected_write) in [
		(tg::Principal::Process(process), true, true),
		(tg::Principal::Sandbox(sandbox), true, true),
		(tg::Principal::User(sandbox_reader), true, false),
		(tg::Principal::User(sandbox_writer), true, true),
		(tg::Principal::User(node_reader), false, false),
		(tg::Principal::User(subtree_reader), false, false),
		(tg::Principal::User(process_reader), true, false),
		(tg::Principal::User(process_writer), true, true),
		(tg::Principal::User(outsider), false, false),
	] {
		assert_eq!(
			is_authorized(&index, object.clone().into(), subtree, &principal).await,
			expected_read,
		);
		assert_eq!(
			is_authorized(&index, target.clone().into(), sandbox_write, &principal,).await,
			expected_write,
		);
	}
}

#[tokio::test]
async fn authorize_flows_sandbox_permissions_to_its_processes() {
	let (_dir, index) = new_index();
	let process = tg::process::Id::new();
	let reader = tg::user::Id::new();
	let sandbox = tg::sandbox::Id::new();
	let writer = tg::user::Id::new();
	let read = tg::authorization::Permission::Sandbox(
		tg::authorization::permission::sandbox::Permission::Read,
	);
	let write = tg::authorization::Permission::Sandbox(
		tg::authorization::permission::sandbox::Permission::Write,
	);
	let mut txn = index.env.write_txn().unwrap();
	put_sandbox(&index, &mut txn, &sandbox);
	put_process(&index, &mut txn, &process, &sandbox);
	put_resource_grant(
		&index,
		&mut txn,
		sandbox.clone().into(),
		tg::authorization::Subject::User(reader.clone()),
		read,
	);
	put_resource_grant(
		&index,
		&mut txn,
		sandbox.into(),
		tg::authorization::Subject::User(writer.clone()),
		write,
	);
	txn.commit().unwrap();

	for permission in [
		tg::authorization::permission::process::Permission::Node,
		tg::authorization::permission::process::Permission::NodeCommand,
		tg::authorization::permission::process::Permission::NodeError,
		tg::authorization::permission::process::Permission::NodeLog,
		tg::authorization::permission::process::Permission::NodeOutput,
		tg::authorization::permission::process::Permission::Read,
		tg::authorization::permission::process::Permission::Subtree,
		tg::authorization::permission::process::Permission::SubtreeCommand,
		tg::authorization::permission::process::Permission::SubtreeError,
		tg::authorization::permission::process::Permission::SubtreeLog,
		tg::authorization::permission::process::Permission::SubtreeOutput,
	] {
		let permission = tg::authorization::Permission::Process(permission);
		assert!(
			is_authorized(
				&index,
				process.clone().into(),
				permission,
				&tg::Principal::User(reader.clone()),
			)
			.await
		);
	}
	let write = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Write,
	);
	assert!(
		!is_authorized(
			&index,
			process.clone().into(),
			write,
			&tg::Principal::User(reader),
		)
		.await
	);
	assert!(is_authorized(&index, process.into(), write, &tg::Principal::User(writer),).await);
}

#[tokio::test]
async fn authorize_derives_process_permissions_without_materialized_grants() {
	let (_dir, index) = new_index();
	let child = tg::process::Id::new();
	let parent = tg::process::Id::new();
	let sandbox = tg::sandbox::Id::new();
	let user = tg::user::Id::new();
	let mut txn = index.env.write_txn().unwrap();
	put_sandbox(&index, &mut txn, &sandbox);
	put_process(&index, &mut txn, &child, &sandbox);
	put_process(&index, &mut txn, &parent, &sandbox);
	put_process_child(&index, &mut txn, &parent, &child);
	let node = tg::authorization::Permission::Process(
		tg::authorization::permission::process::Permission::Node,
	);
	for process in [&child, &parent] {
		put_resource_grant(
			&index,
			&mut txn,
			process.clone().into(),
			tg::authorization::Subject::User(user.clone()),
			node,
		);
		for (n, kind) in [
			(0, crate::process::object::Kind::Command),
			(1, crate::process::object::Kind::Error),
			(2, crate::process::object::Kind::Log),
			(3, crate::process::object::Kind::Output),
		] {
			let object = object_id(n + usize::from(process == &parent) * 4);
			put_object(&index, &mut txn, &object);
			put_process_object(&index, &mut txn, process, &object, kind);
			put_grant(
				&index,
				&mut txn,
				&object,
				&user,
				tg::authorization::permission::object::Permission::Subtree,
			);
		}
	}
	txn.commit().unwrap();

	for permission in [
		tg::authorization::permission::process::Permission::NodeCommand,
		tg::authorization::permission::process::Permission::NodeError,
		tg::authorization::permission::process::Permission::NodeLog,
		tg::authorization::permission::process::Permission::NodeOutput,
		tg::authorization::permission::process::Permission::Subtree,
		tg::authorization::permission::process::Permission::SubtreeCommand,
		tg::authorization::permission::process::Permission::SubtreeError,
		tg::authorization::permission::process::Permission::SubtreeLog,
		tg::authorization::permission::process::Permission::SubtreeOutput,
	] {
		let permission = tg::authorization::Permission::Process(permission);
		assert!(
			is_authorized(
				&index,
				parent.clone().into(),
				permission,
				&tg::Principal::User(user.clone()),
			)
			.await
		);
	}
}

// Authorizing an object walks its ancestry for a covering grant. The work must
// grow linearly with the depth of the ancestry.
#[tokio::test]
async fn authorize_deep_chain_scales_linearly() {
	const DEPTH: usize = 1000;

	let (_dir, index) = new_index();

	// Build a chain nodes[0] (root) -> ... -> nodes[DEPTH] and grant the user only
	// the root subtree. Nothing materializes it onto the descendants, so
	// authorizing one must walk up to the root.
	let user = tg::user::Id::new();
	let nodes: Vec<tg::object::Id> = (0..=DEPTH).map(object_id).collect();
	let mut txn = index.env.write_txn().unwrap();
	for i in 0..=DEPTH {
		put_object(&index, &mut txn, &nodes[i]);
		if i > 0 {
			put_child(&index, &mut txn, &nodes[i - 1], &nodes[i]);
		}
	}
	put_grant(
		&index,
		&mut txn,
		&nodes[0],
		&user,
		tg::authorization::permission::object::Permission::Subtree,
	);
	txn.commit().unwrap();

	// Compare a ratio of two depths rather than an absolute time, so the bound is
	// machine-independent.
	let base = authorize_secs(&index, &nodes[DEPTH / 4], &user).await;
	let deep = authorize_secs(&index, &nodes[DEPTH], &user).await;
	let ratio = deep / base;
	eprintln!(
		"depth {} = {:.1}ms, depth {DEPTH} = {:.1}ms, ratio = {ratio:.1}x",
		DEPTH / 4,
		base * 1e3,
		deep * 1e3,
	);
	assert!(
		ratio < 6.0,
		"authorization compounded {ratio:.1}x over a 4x deeper chain: it is super-linear in the chain depth"
	);
}

#[tokio::test]
async fn authorize_wide_fanout_scales_linearly() {
	const BASE: usize = 512;
	const WIDE: usize = 2048;

	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let base = object_id(0);
	let wide = object_id(1);
	let mut txn = index.env.write_txn().unwrap();
	put_object(&index, &mut txn, &base);
	put_object(&index, &mut txn, &wide);
	for i in 0..WIDE {
		let parent = object_id(i + 2);
		put_object(&index, &mut txn, &parent);
		put_child(&index, &mut txn, &parent, &wide);
		if i < BASE {
			put_child(&index, &mut txn, &parent, &base);
		}
	}
	txn.commit().unwrap();

	let base_elapsed = deny_secs(&index, &base, &user).await;
	let wide_elapsed = deny_secs(&index, &wide, &user).await;
	let ratio = wide_elapsed / base_elapsed;
	eprintln!(
		"width {BASE} = {:.1}ms, width {WIDE} = {:.1}ms, ratio = {ratio:.1}x",
		base_elapsed * 1e3,
		wide_elapsed * 1e3,
	);
	assert!(
		ratio < 6.0,
		"authorization compounded {ratio:.1}x over a 4x wider graph: it is super-linear in the graph width"
	);
}

#[tokio::test]
async fn authorize_does_not_share_token_results_between_batch_arguments() {
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let parent = object_id(0);
	let child = object_id(1);
	let mut txn = index.env.write_txn().unwrap();
	put_object(&index, &mut txn, &parent);
	put_object(&index, &mut txn, &child);
	put_child(&index, &mut txn, &parent, &child);
	txn.commit().unwrap();

	let node = object_permissions([tg::authorization::permission::object::Permission::Node]);
	let args = vec![
		crate::authorize::Arg {
			permissions: node,
			resource: tg::Selector::Id(child.clone().into()),
			token: Some(tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![object_permission(
					tg::authorization::permission::object::Permission::Subtree,
				)],
				resource: parent.into(),
			}),
		},
		crate::authorize::Arg {
			permissions: node,
			resource: tg::Selector::Id(child.into()),
			token: None,
		},
	];
	let reversed = vec![args[1].clone(), args[0].clone()];
	let output = authorize(&index, args, &user).await;
	assert!(output[0].as_ref().unwrap().permissions.contains(node));
	assert!(!output[1].as_ref().unwrap().permissions.contains(node));
	let output = authorize(&index, reversed, &user).await;
	assert!(!output[0].as_ref().unwrap().permissions.contains(node));
	assert!(output[1].as_ref().unwrap().permissions.contains(node));
}

#[tokio::test]
async fn authorize_keeps_ordinary_and_derived_subtree_results_separate() {
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let root = object_id(0);
	let child = object_id(1);
	let leaf = object_id(2);
	let mut txn = index.env.write_txn().unwrap();
	for object in [&root, &child, &leaf] {
		put_object(&index, &mut txn, object);
		put_grant(
			&index,
			&mut txn,
			object,
			&user,
			tg::authorization::permission::object::Permission::Node,
		);
	}
	put_child(&index, &mut txn, &root, &child);
	put_child(&index, &mut txn, &child, &leaf);
	txn.commit().unwrap();

	let subtree = object_permissions([tg::authorization::permission::object::Permission::Subtree]);
	let args = vec![
		crate::authorize::Arg {
			permissions: subtree,
			resource: tg::Selector::Id(root.into()),
			token: None,
		},
		crate::authorize::Arg {
			permissions: subtree,
			resource: tg::Selector::Id(child.into()),
			token: None,
		},
	];
	let output = authorize(&index, args, &user).await;
	assert!(
		output
			.iter()
			.all(|output| output.as_ref().unwrap().permissions.contains(subtree))
	);
}

#[tokio::test]
async fn authorize_prunes_a_covered_subtree_before_loading_its_children() {
	const CHILDREN: usize = 2048;

	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let root = object_id(0);
	let covered = object_id(1);
	let mut txn = index.env.write_txn().unwrap();
	put_object(&index, &mut txn, &root);
	put_object(&index, &mut txn, &covered);
	put_child(&index, &mut txn, &root, &covered);
	put_grant(
		&index,
		&mut txn,
		&root,
		&user,
		tg::authorization::permission::object::Permission::Node,
	);
	put_grant(
		&index,
		&mut txn,
		&covered,
		&user,
		tg::authorization::permission::object::Permission::Subtree,
	);
	for i in 0..CHILDREN {
		let child = object_id(i + 2);
		put_object(&index, &mut txn, &child);
		put_child(&index, &mut txn, &covered, &child);
	}
	txn.commit().unwrap();

	let subtree = object_permissions([tg::authorization::permission::object::Permission::Subtree]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: subtree,
			resource: tg::Selector::Id(root.into()),
			token: None,
		}],
		&user,
	)
	.await;
	assert!(output[0].as_ref().unwrap().permissions.contains(subtree));
}

#[tokio::test]
async fn authorize_visits_shared_descendants_once() {
	const LAYERS: usize = 10;

	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let root = object_id(0);
	let layers = (0..LAYERS)
		.map(|layer| [object_id(layer * 2 + 1), object_id(layer * 2 + 2)])
		.collect::<Vec<_>>();
	let mut txn = index.env.write_txn().unwrap();
	put_object(&index, &mut txn, &root);
	put_grant(
		&index,
		&mut txn,
		&root,
		&user,
		tg::authorization::permission::object::Permission::Node,
	);
	for layer in &layers {
		for object in layer {
			put_object(&index, &mut txn, object);
			put_grant(
				&index,
				&mut txn,
				object,
				&user,
				tg::authorization::permission::object::Permission::Node,
			);
		}
	}
	for child in &layers[0] {
		put_child(&index, &mut txn, &root, child);
	}
	for pair in layers.windows(2) {
		for parent in &pair[0] {
			for child in &pair[1] {
				put_child(&index, &mut txn, parent, child);
			}
		}
	}
	txn.commit().unwrap();

	let subtree = object_permissions([tg::authorization::permission::object::Permission::Subtree]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: subtree,
			resource: tg::Selector::Id(root.into()),
			token: None,
		}],
		&user,
	)
	.await;
	assert!(output[0].as_ref().unwrap().permissions.contains(subtree));
}

#[tokio::test]
async fn authorize_subtree_ignores_a_visited_child_at_the_depth_limit() {
	const DEPTH: usize = 16;

	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let objects = (0..=DEPTH).map(object_id).collect::<Vec<_>>();
	let mut txn = index.env.write_txn().unwrap();
	for object in &objects {
		put_object(&index, &mut txn, object);
		put_grant(
			&index,
			&mut txn,
			object,
			&user,
			tg::authorization::permission::object::Permission::Node,
		);
	}
	for pair in objects.windows(2) {
		put_child(&index, &mut txn, &pair[0], &pair[1]);
	}
	put_child(&index, &mut txn, objects.last().unwrap(), &objects[0]);
	txn.commit().unwrap();

	let subtree = object_permissions([tg::authorization::permission::object::Permission::Subtree]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: subtree,
			resource: tg::Selector::Id(objects[0].clone().into()),
			token: None,
		}],
		&user,
	)
	.await;
	assert!(output[0].as_ref().unwrap().permissions.contains(subtree));
}

#[tokio::test]
async fn authorize_accumulates_permissions_from_different_proofs() {
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let root = object_id(0);
	let child = object_id(1);
	let mut txn = index.env.write_txn().unwrap();
	for object in [&root, &child] {
		put_object(&index, &mut txn, object);
		put_grant(
			&index,
			&mut txn,
			object,
			&user,
			tg::authorization::permission::object::Permission::Node,
		);
	}
	put_child(&index, &mut txn, &root, &child);
	txn.commit().unwrap();

	let permissions = object_permissions([
		tg::authorization::permission::object::Permission::Node,
		tg::authorization::permission::object::Permission::Subtree,
	]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions,
			resource: tg::Selector::Id(root.into()),
			token: None,
		}],
		&user,
	)
	.await;
	assert!(
		output[0]
			.as_ref()
			.unwrap()
			.permissions
			.contains(permissions)
	);
}

#[tokio::test]
async fn authorize_ordinary_cycle_with_an_authorized_escape() {
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let first = object_id(0);
	let second = object_id(1);
	let granted = object_id(2);
	let mut txn = index.env.write_txn().unwrap();
	for object in [&first, &second, &granted] {
		put_object(&index, &mut txn, object);
	}
	put_child(&index, &mut txn, &first, &second);
	put_child(&index, &mut txn, &second, &first);
	put_child(&index, &mut txn, &granted, &second);
	put_grant(
		&index,
		&mut txn,
		&granted,
		&user,
		tg::authorization::permission::object::Permission::Subtree,
	);
	txn.commit().unwrap();

	let node = object_permissions([tg::authorization::permission::object::Permission::Node]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: node,
			resource: tg::Selector::Id(first.into()),
			token: None,
		}],
		&user,
	)
	.await;
	assert!(output[0].as_ref().unwrap().permissions.contains(node));
}

#[tokio::test]
async fn authorize_descendant_node_proof_can_walk_upward() {
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let object = object_id(0);
	let parent_tag = tg::tag::Id::new();
	let child_tag = tg::tag::Id::new();
	let mut txn = index.env.write_txn().unwrap();
	put_object(&index, &mut txn, &object);
	Index::put_tags_with_transaction(
		&index.db,
		&index.subspace,
		&mut txn,
		&[
			crate::tag::put::Arg {
				account: None,
				id: parent_tag.clone(),
				name: "parent".into(),
				parent: None,
				permissions: Vec::new(),
				specifier: "parent".parse().unwrap(),
				target: tg::Either::Left(object.clone()),
			},
			crate::tag::put::Arg {
				account: None,
				id: child_tag,
				name: "child".into(),
				parent: Some(parent_tag.clone().into()),
				permissions: vec![object_permission(
					tg::authorization::permission::object::Permission::Node,
				)],
				specifier: "parent/child".parse().unwrap(),
				target: tg::Either::Left(object.clone()),
			},
		],
	)
	.unwrap();
	put(
		&index,
		&mut txn,
		&Key::Grant(GrantKey::ResourceGrant {
			creator: None,
			permission: tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Read,
			),
			resource: parent_tag.into(),
			subject: tg::authorization::Subject::User(user.clone()),
		}),
	);
	txn.commit().unwrap();

	let subtree = object_permissions([tg::authorization::permission::object::Permission::Subtree]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: subtree,
			resource: tg::Selector::Id(object.into()),
			token: None,
		}],
		&user,
	)
	.await;
	assert!(output[0].as_ref().unwrap().permissions.contains(subtree));
}

#[tokio::test]
async fn authorize_uses_cached_subject_membership() {
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let inner = tg::group::Id::new();
	let outer = tg::group::Id::new();
	let organization = tg::organization::Id::new();
	let object = object_id(0);
	let mut txn = index.env.write_txn().unwrap();
	put_object(&index, &mut txn, &object);
	put(
		&index,
		&mut txn,
		&Key::Group(GroupKey::GroupMember {
			group: inner.clone(),
			member: user.clone().into(),
		}),
	);
	put(
		&index,
		&mut txn,
		&Key::Group(GroupKey::MemberGroup {
			member: user.clone().into(),
			group: inner.clone(),
		}),
	);
	put(
		&index,
		&mut txn,
		&Key::Group(GroupKey::GroupMember {
			group: outer.clone(),
			member: inner.clone().into(),
		}),
	);
	put(
		&index,
		&mut txn,
		&Key::Group(GroupKey::MemberGroup {
			member: inner.into(),
			group: outer.clone(),
		}),
	);
	put(
		&index,
		&mut txn,
		&Key::Organization(OrganizationKey::OrganizationMember {
			organization: organization.clone(),
			member: outer.clone().into(),
		}),
	);
	put(
		&index,
		&mut txn,
		&Key::Organization(OrganizationKey::MemberOrganization {
			member: outer.into(),
			organization: organization.clone(),
		}),
	);
	put(
		&index,
		&mut txn,
		&Key::Grant(GrantKey::ResourceGrant {
			creator: None,
			permission: object_permission(tg::authorization::permission::object::Permission::Node),
			resource: object.clone().into(),
			subject: tg::authorization::Subject::Organization(organization),
		}),
	);
	txn.commit().unwrap();

	let node = object_permissions([tg::authorization::permission::object::Permission::Node]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: node,
			resource: tg::Selector::Id(object.into()),
			token: None,
		}],
		&user,
	)
	.await;
	assert!(output[0].as_ref().unwrap().permissions.contains(node));
}
