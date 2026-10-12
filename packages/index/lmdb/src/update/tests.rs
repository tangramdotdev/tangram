use {
	super::{ParentCandidates, object_resource, process_resource},
	crate::{
		Config, Index, Key, Kind,
		permission::{PermissionIndexEntry, PermissionSource},
	},
	heed as lmdb,
	num_traits::ToPrimitive as _,
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

fn object_id(n: usize) -> tg::object::Id {
	tg::object::Id::new(tg::object::Kind::Blob, &n.to_le_bytes().to_vec().into())
}

fn put(index: &Index, txn: &mut lmdb::RwTxn<'_>, key: &Key, value: &[u8]) {
	let key = Index::pack(&index.subspace, key);
	index.db.put(txn, &key, value).unwrap();
}

fn put_delegation(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	object: &tg::object::Id,
	subject: &tg::authorization::Subject,
) {
	let arg = tangram_index::delegation::put::Arg {
		expires_at: i64::MAX,
		resource: object.clone().into(),
		source: tg::authorization::Subject::User(tg::user::Id::new()),
		subject: subject.clone(),
		version: None,
	};
	Index::put_delegation_with_transaction(&index.db, &index.subspace, txn, &arg).unwrap();
}

fn put_permission(
	index: &Index,
	txn: &mut lmdb::RwTxn<'_>,
	resource: &tg::Id,
	subject: &tg::authorization::Subject,
) {
	let permission = if resource.kind() == tg::id::Kind::Process {
		tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		)
	} else {
		tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Node,
		)
	};
	let entry = PermissionIndexEntry {
		creator: None,
		expires_at: None,
		permission,
		resource,
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
async fn parents_for_subject_read_the_smaller_side() {
	let (_dir, index) = new_index();
	let object = object_id(0);
	let child = tg::process::Id::new();
	let processes = (0..8).map(|_| tg::process::Id::new()).collect::<Vec<_>>();
	let objects = (1..9).map(object_id).collect::<Vec<_>>();

	// Give one subject fewer permissions and delegations than there are parents and another subject more, each with a permission or a delegation on some parents and permissions on resources that are not parents.
	let few = tg::authorization::Subject::Process(processes[0].clone());
	let many = tg::authorization::Subject::User(tg::user::Id::new());
	let mut txn = index.env.write_txn().unwrap();
	for (process, parent) in std::iter::zip(&processes, &objects) {
		let key = Key::Object(crate::object::Key::ObjectProcess {
			kind: tangram_index::process::object::Kind::Log,
			object: object.clone(),
			process: process.clone(),
		});
		put(&index, &mut txn, &key, &[]);
		let key = Key::Process(crate::process::Key::ChildProcess {
			child: child.clone(),
			parent: process.clone(),
		});
		put(&index, &mut txn, &key, &0i64.to_be_bytes());
		let key = Key::Object(crate::object::Key::ChildObject {
			child: object.clone(),
			object: parent.clone(),
		});
		put(&index, &mut txn, &key, &[]);
	}
	put_permission(&index, &mut txn, &processes[0].clone().into(), &few);
	put_permission(&index, &mut txn, &objects[0].clone().into(), &few);
	put_delegation(&index, &mut txn, &objects[1], &few);
	put_permission(&index, &mut txn, &tg::process::Id::new().into(), &few);
	put_permission(&index, &mut txn, &object_id(100).into(), &few);
	put_permission(&index, &mut txn, &processes[1].clone().into(), &many);
	put_permission(&index, &mut txn, &objects[2].clone().into(), &many);
	put_delegation(&index, &mut txn, &objects[3], &many);
	for n in 0..16 {
		put_permission(&index, &mut txn, &tg::process::Id::new().into(), &many);
		put_permission(&index, &mut txn, &object_id(200 + n).into(), &many);
	}
	txn.commit().unwrap();

	// Verify that the candidates come from the smaller side.
	let txn = index.env.read_txn().unwrap();
	let child_bytes = child.to_bytes();
	let process_prefix = Index::pack(
		&index.subspace,
		&(Kind::ChildProcess.to_i32().unwrap(), child_bytes.as_ref()),
	);
	let object_bytes = object.to_bytes();
	let object_prefix = Index::pack(
		&index.subspace,
		&(Kind::ChildObject.to_i32().unwrap(), object_bytes.as_ref()),
	);
	for (subject, expected) in [(&few, false), (&many, true)] {
		let candidates = Index::get_parent_candidates_with_transaction(
			&index.db,
			&index.subspace,
			&txn,
			&process_prefix,
			|key| {
				let Key::Process(crate::process::Key::ChildProcess { parent, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(parent)
			},
			subject,
			&[Kind::SubjectPermission],
			process_resource,
		)
		.unwrap();
		assert_eq!(matches!(candidates, ParentCandidates::Parents(_)), expected);
		let candidates = Index::get_parent_candidates_with_transaction(
			&index.db,
			&index.subspace,
			&txn,
			&object_prefix,
			|key| {
				let Key::Object(crate::object::Key::ChildObject { object, .. }) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				Ok(object)
			},
			subject,
			&[Kind::SubjectPermission, Kind::DelegationSubject],
			object_resource,
		)
		.unwrap();
		assert_eq!(matches!(candidates, ParentCandidates::Parents(_)), expected);
	}

	// Verify that both sides yield only the parents on which the subject holds a permission or a delegation.
	let cases = [
		(&few, &processes[0], [&objects[0], &objects[1]]),
		(&many, &processes[1], [&objects[2], &objects[3]]),
	];
	for (subject, process, parents) in cases {
		let expected = BTreeSet::from([process.clone()]);
		let output = Index::get_object_processes_for_subject_with_transaction(
			&index.db,
			&index.subspace,
			&txn,
			&object,
			subject,
		)
		.unwrap();
		assert_eq!(output, expected);
		let output = Index::get_process_parents_for_subject_with_transaction(
			&index.db,
			&index.subspace,
			&txn,
			&child,
			subject,
		)
		.unwrap();
		assert_eq!(output, expected);
		let expected = parents.into_iter().cloned().collect::<BTreeSet<_>>();
		let output = Index::get_object_parents_for_subject_with_transaction(
			&index.db,
			&index.subspace,
			&txn,
			&object,
			subject,
		)
		.unwrap();
		assert_eq!(output, expected);
	}
}
