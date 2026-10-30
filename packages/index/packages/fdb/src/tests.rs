use {
	super::Index,
	foundationdb as fdb,
	futures::FutureExt as _,
	std::{collections::BTreeMap, ops::ControlFlow, panic::AssertUnwindSafe, sync::OnceLock},
	tangram_client::prelude::*,
};

mod process;
mod reader;

static NETWORK: OnceLock<fdb::api::NetworkAutoStop> = OnceLock::new();

pub(crate) async fn run(options: &crate::Options, test: impl AsyncFnOnce(&Index)) {
	NETWORK.get_or_init(|| {
		// SAFETY: The network is initialized once and outlives every test database and runtime.
		unsafe { fdb::boot() }
	});
	let index = Index::new(options).unwrap();
	let result = AssertUnwindSafe(test(&index)).catch_unwind().await;
	crate::run(&index.database, async |txn| {
		let (begin, end) = index.subspace.range();
		txn.clear_range(&begin, &end);
		Ok(ControlFlow::Break(()))
	})
	.await
	.unwrap();
	if let Err(error) = result {
		std::panic::resume_unwind(error);
	}
}

pub(crate) fn directory(children: &[(&str, tg::object::Id)]) -> tangram_index::object::put::Arg {
	let entries = children
		.iter()
		.map(|(name, id)| ((*name).to_owned(), id.to_string()))
		.collect::<BTreeMap<_, _>>();
	let data: tg::directory::Data =
		serde_json::from_value(serde_json::json!({ "entries": entries })).unwrap();
	let bytes = data.serialize().unwrap();
	let id = tg::object::Id::new(tg::object::Kind::Directory, &bytes);
	let metadata = tg::object::Metadata {
		node: tg::object::metadata::Node {
			size: bytes.len() as u64,
			solvable: false,
			solved: true,
		},
		subtree: tg::object::metadata::Subtree::default(),
	};
	tangram_index::object::put::Arg {
		account: None,
		checkout: None,
		children: children.iter().map(|(_, id)| id.clone()).collect(),
		id,
		metadata,
		put: [1; 16],
		storage: tg::object::storage::Set::NODE,
		time_to_touch: std::time::Duration::ZERO,
		touched_at: 0,
	}
}

pub(crate) async fn put(index: &Index, objects: Vec<tangram_index::object::put::Arg>) {
	let items = objects
		.into_iter()
		.map(tangram_index::Item::PutObject)
		.collect();
	let arg = tangram_index::write::Arg { items };
	index.write(arg).await.unwrap().unwrap();
}
