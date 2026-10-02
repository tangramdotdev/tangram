use {
	super::{Item, ItemKind},
	crate::{Config, Index},
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

#[test]
fn candidates_merge_kind_ranges_by_timestamp() {
	let dir = tempfile::TempDir::new().unwrap();
	let config = Config {
		map_size: 1 << 30,
		max_process_depth: None,
		path: dir.path().join("index"),
		posix_sem_prefix: None,
		read_request_batch_size: 64,
		read_transaction_concurrency: 4,
		usage_partition_total: 1,
		write_operation_batch_size: 1,
	};
	let index = Index::new(&config).unwrap();
	let account = tangram_index::usage::Account::User(tg::user::Id::new());
	let object = tg::object::Id::new(tg::object::Kind::Blob, &vec![0].into());
	let process = tg::process::Id::new();
	let sandbox = tg::sandbox::Id::new();
	let checkout = tg::tag::Id::new().into();
	let keys = [
		(
			7,
			ItemKind::AccountObject,
			crate::clean::Key::AccountObject {
				account: account.clone(),
				object: object.clone(),
				touched_at: 7,
			},
		),
		(
			11,
			ItemKind::AccountProcess,
			crate::clean::Key::AccountProcess {
				account,
				process: process.clone(),
				touched_at: 11,
			},
		),
		(
			7,
			ItemKind::Checkout,
			crate::clean::Key::Checkout {
				id: checkout,
				touched_at: 7,
			},
		),
		(
			2,
			ItemKind::Object,
			crate::clean::Key::Object {
				id: object.clone(),
				touched_at: 2,
			},
		),
		(
			7,
			ItemKind::Object,
			crate::clean::Key::Object {
				id: object,
				touched_at: 7,
			},
		),
		(
			7,
			ItemKind::Process,
			crate::clean::Key::Process {
				id: process.clone(),
				touched_at: 7,
			},
		),
		(
			11,
			ItemKind::Process,
			crate::clean::Key::Process {
				id: process,
				touched_at: 11,
			},
		),
		(
			1,
			ItemKind::Sandbox,
			crate::clean::Key::Sandbox {
				id: sandbox.clone(),
				touched_at: 1,
			},
		),
		(
			13,
			ItemKind::Sandbox,
			crate::clean::Key::Sandbox {
				id: sandbox,
				touched_at: 13,
			},
		),
	];

	// Insert keys from each kind with interleaved timestamps and ties.
	let mut transaction = index.env.write_txn().unwrap();
	let mut keys = keys.map(|(touched_at, kind, key)| {
		let key = Index::pack(&index.subspace, &crate::Key::Clean(key));
		index.db.put(&mut transaction, &key, &[]).unwrap();
		(touched_at, kind.to_i32().unwrap(), key)
	});
	transaction.commit().unwrap();
	keys.sort_by_key(|(touched_at, kind, _)| (*touched_at, *kind));
	let cutoffs = [
		(ItemKind::Checkout, 7),
		(ItemKind::Object, 7),
		(ItemKind::Process, 11),
		(ItemKind::Sandbox, 13),
		(ItemKind::AccountObject, 7),
		(ItemKind::AccountProcess, 11),
	];
	let transaction = index.env.write_txn().unwrap();
	for batch_size in [0, 1, 4, 20] {
		let candidates = Index::clean_candidates(
			&index.db,
			&index.subspace,
			&transaction,
			&cutoffs,
			batch_size,
		)
		.unwrap();
		let actual = candidates
			.into_iter()
			.map(|candidate| {
				let kind = match candidate.item {
					Item::AccountObject { .. } => ItemKind::AccountObject,
					Item::AccountProcess { .. } => ItemKind::AccountProcess,
					Item::Checkout(_) => ItemKind::Checkout,
					Item::Object(_) => ItemKind::Object,
					Item::Process(_) => ItemKind::Process,
					Item::Sandbox(_) => ItemKind::Sandbox,
				};
				(candidate.touched_at, kind.to_i32().unwrap())
			})
			.collect::<Vec<_>>();
		let expected = keys
			.iter()
			.take(batch_size)
			.map(|(touched_at, kind, _)| (*touched_at, *kind))
			.collect::<Vec<_>>();
		assert_eq!(actual, expected);
	}
}
