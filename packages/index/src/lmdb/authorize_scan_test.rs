use {
	super::{Config, Index, Key, grant::Key as GrantKey, object::Key as ObjectKey},
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

async fn authorize_secs(index: &Index, resource: &tg::object::Id, user: &tg::user::Id) -> f64 {
	let node = tg::grant::permission::Set::from_permission(tg::grant::Permission::Object(
		tg::grant::permission::object::Permission::Node,
	));
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

// Authorizing an object walks its ancestry for a covering grant. Re-queueing
// every incomplete frame once per undecided ancestor makes a single
// authorization O(depth^2), so a node four times as deep costs about sixteen
// times as much.
#[tokio::test]
async fn authorize_deep_chain_is_super_linear() {
	const DEPTH: usize = 1000;

	let dir = tempfile::TempDir::new().unwrap();
	let index = Index::new(&Config {
		authorize: super::AuthorizeConfig {
			object_subtree: crate::authorize::ObjectSubtreeConfig::default(),
		},
		map_size: 1 << 30,
		max_items_per_transaction: 100_000,
		path: dir.path().join("index"),
	})
	.unwrap();

	// Build a chain nodes[0] (root) -> ... -> nodes[DEPTH] and grant the user only
	// the root subtree. Nothing materializes it onto the descendants, so
	// authorizing one must walk up to the root.
	let user = tg::user::Id::new();
	let nodes: Vec<tg::object::Id> = (0..=DEPTH).map(object_id).collect();
	let mut txn = index.env.write_txn().unwrap();
	for i in 0..=DEPTH {
		put(
			&index,
			&mut txn,
			&Key::Object(ObjectKey::Object(nodes[i].clone())),
		);
		if i > 0 {
			put(
				&index,
				&mut txn,
				&Key::Object(ObjectKey::ChildObject {
					child: nodes[i].clone(),
					object: nodes[i - 1].clone(),
				}),
			);
		}
	}
	put(
		&index,
		&mut txn,
		&Key::Grant(GrantKey::ResourceGrant {
			resource: nodes[0].clone().into(),
			principal: tg::grant::Principal::User(user.clone()),
			creator: None,
			permission: tg::grant::Permission::Object(
				tg::grant::permission::object::Permission::Subtree,
			),
		}),
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
