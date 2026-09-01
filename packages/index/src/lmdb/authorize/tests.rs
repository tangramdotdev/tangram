use {
	crate::lmdb::{Config, Index, Key, grant::Key as GrantKey, object::Key as ObjectKey},
	heed as lmdb,
	tangram_client::prelude::*,
};

// The ancestor search visits parents in key order, so the identifiers must sort in the order the
// search is expected to visit them.
fn ordered_object_id(n: usize) -> tg::object::Id {
	let mut body = [0; 32];
	body[..size_of::<usize>()].copy_from_slice(&n.to_be_bytes());
	let id = tg::Id::new(tg::id::Kind::Blob, tg::id::Body::Blake3(body));
	tg::object::Id::try_from(id).unwrap()
}

fn new_index() -> (tempfile::TempDir, Index) {
	let dir = tempfile::TempDir::new().unwrap();
	let config = Config {
		map_size: 1 << 30,
		max_process_depth: None,
		path: dir.path().join("index"),
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total: 1,
		write_operation_batch_size: 100_000,
	};
	let index = Index::new(&config).unwrap();
	(dir, index)
}

fn put(index: &Index, txn: &mut lmdb::RwTxn<'_>, key: &Key, value: &[u8]) {
	let key = Index::pack(&index.subspace, key);
	index.db.put(txn, &key, value).unwrap();
}

fn put_object(index: &Index, txn: &mut lmdb::RwTxn<'_>, object: &tg::object::Id) {
	let key = Key::Object(ObjectKey::Object(object.clone()));
	put(index, txn, &key, &[]);
}

fn put_child(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	parent: &tg::object::Id,
	child: &tg::object::Id,
) {
	let key = Key::Object(ObjectKey::ObjectChild {
		child: child.clone(),
		object: parent.clone(),
	});
	put(index, txn, &key, &[]);
	let key = Key::Object(ObjectKey::ChildObject {
		child: child.clone(),
		object: parent.clone(),
	});
	put(index, txn, &key, &[]);
}

fn put_grant(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	resource: &tg::object::Id,
	user: &tg::user::Id,
	permission: tg::authorization::permission::object::Permission,
) {
	let value = crate::lmdb::grant::GrantValue {
		explicit: true,
		..Default::default()
	}
	.serialize()
	.unwrap();
	let key = Key::Grant(GrantKey::ResourceGrant {
		creator: None,
		permission: tg::authorization::Permission::Object(permission),
		resource: resource.clone().into(),
		subject: tg::authorization::Subject::User(user.clone()),
	});
	put(index, txn, &key, &value);
}

// The ancestor search abandons a frontier that already holds the proof.
//
//     p1 ... p1088        the decoy's parents, more numerous than the edge budget
//        \   |   /
//         decoy   proof   the proof carries a grant for the requester
//             \   /
//             target      the resource being authorized
//
// Object ids are constructed to sort in visit order, so the decoy is the first of the target's two
// parents the search reaches. Reading the target's parents enqueues both at depth one and costs two
// edges. The decoy pops first, and paging through its parents charges one edge each until the
// budget runs out, at which point the walk returns Exhausted as a whole. The proof has been queued
// one hop from the target since the second edge and is never examined; the abandoned stack holds
// it, the decoy's unfinished pagination, and 61 of the decoy's parents at depth two.
#[tokio::test]
async fn ancestor_search_must_not_abort_with_the_proof_enqueued() {
	// Use the default budgets, and size the decoy's fan-in against the edge budget rather than
	// hardcoding it, so raising the budget cannot make this test pass.
	let authorize = crate::authorize::Config::default();
	let ancestor = authorize.ancestor;
	let fanin = ancestor.max_edges + ancestor.page_size;

	// Create the graph.
	let (_dir, index) = new_index();
	let user = tg::user::Id::new();
	let decoy = ordered_object_id(0);
	let proof = ordered_object_id(fanin + 1);
	let target = ordered_object_id(fanin + 2);
	let mut txn = index.env.write_txn().unwrap();
	put_object(&index, &mut txn, &decoy);
	put_object(&index, &mut txn, &proof);
	put_object(&index, &mut txn, &target);
	put_child(&index, &mut txn, &decoy, &target);
	put_child(&index, &mut txn, &proof, &target);
	for i in 1..=fanin {
		let object = ordered_object_id(i);
		put_object(&index, &mut txn, &object);
		put_child(&index, &mut txn, &object, &decoy);
	}
	let subtree = tg::authorization::permission::object::Permission::Subtree;
	put_grant(&index, &mut txn, &proof, &user, subtree);
	txn.commit().unwrap();

	// Authorize the target.
	let permission = tg::authorization::Permission::Object(
		tg::authorization::permission::object::Permission::Node,
	);
	let principal = tg::Principal::User(user);
	let transaction = index.env.read_txn().unwrap();
	let requested = tg::authorization::permission::Set::from_permission(permission);
	let arg = crate::authorize::Arg {
		requested,
		required: requested,
		resource: tg::Selector::Id(target.into()),
		token: None,
	};
	let outcomes = Index::authorize_batch_with_transaction(
		crate::authorize::facts::Cache::new(),
		authorize,
		&index.db,
		&index.subspace,
		&transaction,
		std::slice::from_ref(&arg),
		&principal,
	)
	.unwrap();

	assert!(matches!(
		outcomes[0],
		crate::authorize::Outcome::Authorized(_)
	));
}
