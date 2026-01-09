use {
	crate::{
		Server,
		index::fdb::{Key, ObjectValue},
	},
	foundationdb as fdb,
	foundationdb_tuple::TuplePack as _,
	std::sync::Arc,
	tangram_client as tg,
};

impl Server {
	pub(crate) async fn try_get_object_metadata_fdb(
		&self,
		database: &Arc<fdb::Database>,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let key = Key::Object(id).pack_to_vec();
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?;

		match value {
			Some(bytes) => {
				let value: ObjectValue = tangram_serialize::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize object"))?;
				Ok(Some(tg::object::Metadata {
					node: tg::object::metadata::Node {
						size: value.node_size,
						solvable: value.node_solvable,
						solved: value.node_solved,
					},
					subtree: tg::object::metadata::Subtree {
						count: value.subtree_count,
						depth: value.subtree_depth,
						size: value.subtree_size,
						solvable: value.subtree_solvable,
						solved: value.subtree_solved,
					},
				}))
			},
			None => Ok(None),
		}
	}

	pub(crate) async fn try_get_object_metadata_batch_fdb(
		&self,
		database: &Arc<fdb::Database>,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let mut results = Vec::with_capacity(ids.len());
		for id in ids {
			let key = Key::Object(id).pack_to_vec();
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get object"))?;

			match value {
				Some(bytes) => {
					let value: ObjectValue = tangram_serialize::from_slice(&bytes)
						.map_err(|source| tg::error!(!source, "failed to deserialize object"))?;
					results.push(Some(tg::object::Metadata {
						node: tg::object::metadata::Node {
							size: value.node_size,
							solvable: value.node_solvable,
							solved: value.node_solved,
						},
						subtree: tg::object::metadata::Subtree {
							count: value.subtree_count,
							depth: value.subtree_depth,
							size: value.subtree_size,
							solvable: value.subtree_solvable,
							solved: value.subtree_solved,
						},
					}));
				},
				None => results.push(None),
			}
		}
		Ok(results)
	}
}
