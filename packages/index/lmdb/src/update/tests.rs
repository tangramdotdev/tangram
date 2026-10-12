use {
	super::ParentCandidates,
	crate::{
		Config, Index, Key,
		permission::{PermissionIndexEntry, PermissionSource},
	},
	heed as lmdb,
	std::collections::BTreeSet,
	tangram_client::prelude::*,
};

fn new_index() -> (tempfile::TempDir, Index) {
	let dir = tempfile::TempDir::new().unwrap();
	let config = Config {
		map_size: 1 << 30,
		max_process_depth: None,
		max_readers: 1_000,
		path: dir.path().join("index"),
		posix_sem_prefix: None,
		read_channel_capacity: 256,
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total: 1,
		write_channel_capacity: 256,
		write_operation_batch_size: 100_000,
	};
	let index = Index::new(&config).unwrap();
	(dir, index)
}

fn put(index: &Index, txn: &mut lmdb::RwTxn<'_>, key: &Key, value: &[u8]) {
	let key = Index::pack(&index.subspace, key);
	index.db.put(txn, &key, value).unwrap();
}

fn put_permission(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	process: &tg::process::Id,
	subject: &tg::authorization::Subject,
) {
	let resource = tg::Id::from(process.clone());
	let entry = PermissionIndexEntry {
		creator: None,
		expires_at: None,
		permission: tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		),
		resource: &resource,
		subject,
	};
	Index::put_permission_index_entry(
		&index.db,
		&index.subspace,
		txn,
		&entry,
		PermissionSource::Grant,
		None,
	)
	.unwrap();
}

#[tokio::test]
async fn parent_processes_for_subject_read_the_smaller_side() {
	let (_dir, index) = new_index();
	let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![0].into());
	let child = tg::process::Id::new();
	let parents = (0..8).map(|_| tg::process::Id::new()).collect::<Vec<_>>();

	// Give one subject fewer permissions than there are parents and another subject more, each with a permission on one parent and on processes that are not parents.
	let few = tg::authorization::Subject::Process(parents[0].clone());
	let many = tg::authorization::Subject::User(tg::user::Id::new());
	let mut txn = index.env.write_txn().unwrap();
	for parent in &parents {
		let key = Key::Object(crate::object::Key::ObjectProcess {
			kind: tangram_index::process::object::Kind::Log,
			object: object.clone(),
			process: parent.clone(),
		});
		put(&index, &mut txn, &key, &[]);
		let key = Key::Process(crate::process::Key::ChildProcess {
			child: child.clone(),
			parent: parent.clone(),
		});
		put(&index, &mut txn, &key, &0i64.to_be_bytes());
	}
	put_permission(&index, &mut txn, &parents[0], &few);
	put_permission(&index, &mut txn, &tg::process::Id::new(), &few);
	put_permission(&index, &mut txn, &parents[1], &many);
	for _ in 0..16 {
		put_permission(&index, &mut txn, &tg::process::Id::new(), &many);
	}
	txn.commit().unwrap();

	// Verify that the candidates come from the smaller side.
	let txn = index.env.read_txn().unwrap();
	let candidates = Index::get_parent_candidates_with_transaction(
		&index.db,
		&index.subspace,
		&txn,
		parents.iter().cloned().map(Ok),
		&few,
	)
	.unwrap();
	assert!(matches!(candidates, ParentCandidates::Subject(_)));
	let candidates = Index::get_parent_candidates_with_transaction(
		&index.db,
		&index.subspace,
		&txn,
		parents.iter().cloned().map(Ok),
		&many,
	)
	.unwrap();
	assert!(matches!(candidates, ParentCandidates::Parents(_)));

	// Verify that both sides yield only the parents on which the subject holds a permission.
	for (subject, parent) in [(&few, &parents[0]), (&many, &parents[1])] {
		let expected = BTreeSet::from([parent.clone()]);
		let processes = Index::get_object_processes_for_subject_with_transaction(
			&index.db,
			&index.subspace,
			&txn,
			&object,
			subject,
		)
		.unwrap();
		assert_eq!(processes, expected);
		let processes = Index::get_process_parents_for_subject_with_transaction(
			&index.db,
			&index.subspace,
			&txn,
			&child,
			subject,
		)
		.unwrap();
		assert_eq!(processes, expected);
	}
}
