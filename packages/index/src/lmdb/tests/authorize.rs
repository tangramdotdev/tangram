use {
	super::super::{
		Config, Index, Key, grant::Key as GrantKey, group::Key as GroupKey,
		object::Key as ObjectKey, organization::Key as OrganizationKey,
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

fn object_permission(
	permission: tg::grant::permission::object::Permission,
) -> tg::grant::Permission {
	tg::grant::Permission::Object(permission)
}

fn object_permissions(
	permissions: impl IntoIterator<Item = tg::grant::permission::object::Permission>,
) -> tg::grant::permission::Set {
	let mut set = tg::grant::permission::object::Set::empty();
	for permission in permissions {
		set.insert(tg::grant::permission::object::Set::from_permission(
			permission,
		));
	}
	tg::grant::permission::Set::Object(set)
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
	permission: tg::grant::permission::object::Permission,
) {
	put(
		index,
		txn,
		&Key::Grant(GrantKey::ResourceGrant {
			resource: resource.clone().into(),
			principal: tg::grant::Principal::User(user.clone()),
			creator: None,
			permission: object_permission(permission),
		}),
	);
}

fn new_index() -> (tempfile::TempDir, Index) {
	let dir = tempfile::TempDir::new().unwrap();
	let index = Index::new(&Config {
		authorize: super::super::AuthorizeConfig {
			object_subtree: crate::authorize::ObjectSubtreeConfig::default(),
		},
		map_size: 1 << 30,
		max_items_per_transaction: 100_000,
		path: dir.path().join("index"),
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

async fn authorize_secs(index: &Index, resource: &tg::object::Id, user: &tg::user::Id) -> f64 {
	let node = object_permissions([tg::grant::permission::object::Permission::Node]);
	let arg = crate::authorize::Arg {
		permissions: node,
		resource: tg::grant::Resource::Id(resource.clone().into()),
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
	let node = object_permissions([tg::grant::permission::object::Permission::Node]);
	let arg = crate::authorize::Arg {
		permissions: node,
		resource: tg::grant::Resource::Id(resource.clone().into()),
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
		tg::grant::permission::object::Permission::Subtree,
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

	let node = object_permissions([tg::grant::permission::object::Permission::Node]);
	let args = vec![
		crate::authorize::Arg {
			permissions: node,
			resource: tg::grant::Resource::Id(child.clone().into()),
			token: Some(tg::grant::Body {
				expires_at: i64::MAX,
				permissions: vec![object_permission(
					tg::grant::permission::object::Permission::Subtree,
				)],
				resource: tg::grant::Resource::Id(parent.into()),
			}),
		},
		crate::authorize::Arg {
			permissions: node,
			resource: tg::grant::Resource::Id(child.into()),
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
			tg::grant::permission::object::Permission::Node,
		);
	}
	put_child(&index, &mut txn, &root, &child);
	put_child(&index, &mut txn, &child, &leaf);
	txn.commit().unwrap();

	let subtree = object_permissions([tg::grant::permission::object::Permission::Subtree]);
	let args = vec![
		crate::authorize::Arg {
			permissions: subtree,
			resource: tg::grant::Resource::Id(root.into()),
			token: None,
		},
		crate::authorize::Arg {
			permissions: subtree,
			resource: tg::grant::Resource::Id(child.into()),
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
		tg::grant::permission::object::Permission::Node,
	);
	put_grant(
		&index,
		&mut txn,
		&covered,
		&user,
		tg::grant::permission::object::Permission::Subtree,
	);
	for i in 0..CHILDREN {
		let child = object_id(i + 2);
		put_object(&index, &mut txn, &child);
		put_child(&index, &mut txn, &covered, &child);
	}
	txn.commit().unwrap();

	let subtree = object_permissions([tg::grant::permission::object::Permission::Subtree]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: subtree,
			resource: tg::grant::Resource::Id(root.into()),
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
		tg::grant::permission::object::Permission::Node,
	);
	for layer in &layers {
		for object in layer {
			put_object(&index, &mut txn, object);
			put_grant(
				&index,
				&mut txn,
				object,
				&user,
				tg::grant::permission::object::Permission::Node,
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

	let subtree = object_permissions([tg::grant::permission::object::Permission::Subtree]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: subtree,
			resource: tg::grant::Resource::Id(root.into()),
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
			tg::grant::permission::object::Permission::Node,
		);
	}
	for pair in objects.windows(2) {
		put_child(&index, &mut txn, &pair[0], &pair[1]);
	}
	put_child(&index, &mut txn, objects.last().unwrap(), &objects[0]);
	txn.commit().unwrap();

	let subtree = object_permissions([tg::grant::permission::object::Permission::Subtree]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: subtree,
			resource: tg::grant::Resource::Id(objects[0].clone().into()),
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
			tg::grant::permission::object::Permission::Node,
		);
	}
	put_child(&index, &mut txn, &root, &child);
	txn.commit().unwrap();

	let permissions = object_permissions([
		tg::grant::permission::object::Permission::Node,
		tg::grant::permission::object::Permission::Subtree,
	]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions,
			resource: tg::grant::Resource::Id(root.into()),
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
		tg::grant::permission::object::Permission::Subtree,
	);
	txn.commit().unwrap();

	let node = object_permissions([tg::grant::permission::object::Permission::Node]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: node,
			resource: tg::grant::Resource::Id(first.into()),
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
	Index::task_put_tags(
		&index.db,
		&index.subspace,
		&mut txn,
		&[
			crate::tag::put::Arg {
				id: parent_tag.clone(),
				item: tg::Either::Left(object.clone()),
				name: "parent".into(),
				parent: None,
				permissions: Vec::new(),
				specifier: "parent".parse().unwrap(),
			},
			crate::tag::put::Arg {
				id: child_tag,
				item: tg::Either::Left(object.clone()),
				name: "child".into(),
				parent: Some(parent_tag.clone().into()),
				permissions: vec![object_permission(
					tg::grant::permission::object::Permission::Node,
				)],
				specifier: "parent/child".parse().unwrap(),
			},
		],
	)
	.unwrap();
	put(
		&index,
		&mut txn,
		&Key::Grant(GrantKey::ResourceGrant {
			resource: parent_tag.into(),
			principal: tg::grant::Principal::User(user.clone()),
			creator: None,
			permission: tg::grant::Permission::Tag(tg::grant::permission::tag::Permission::Read),
		}),
	);
	txn.commit().unwrap();

	let subtree = object_permissions([tg::grant::permission::object::Permission::Subtree]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: subtree,
			resource: tg::grant::Resource::Id(object.into()),
			token: None,
		}],
		&user,
	)
	.await;
	assert!(output[0].as_ref().unwrap().permissions.contains(subtree));
}

#[tokio::test]
async fn authorize_uses_cached_principal_membership() {
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
			resource: object.clone().into(),
			principal: tg::grant::Principal::Organization(organization),
			creator: None,
			permission: object_permission(tg::grant::permission::object::Permission::Node),
		}),
	);
	txn.commit().unwrap();

	let node = object_permissions([tg::grant::permission::object::Permission::Node]);
	let output = authorize(
		&index,
		vec![crate::authorize::Arg {
			permissions: node,
			resource: tg::grant::Resource::Id(object.into()),
			token: None,
		}],
		&user,
	)
	.await;
	assert!(output[0].as_ref().unwrap().permissions.contains(node));
}
