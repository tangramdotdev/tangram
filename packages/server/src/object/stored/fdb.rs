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
	pub(crate) async fn try_get_object_stored_fdb(
		&self,
		database: &Arc<fdb::Database>,
		id: &tg::object::Id,
	) -> tg::Result<Option<super::Output>> {
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
				Ok(Some(super::Output {
					subtree: value.subtree_stored,
				}))
			},
			None => Ok(None),
		}
	}

	pub(crate) async fn try_get_object_stored_batch_fdb(
		&self,
		database: &Arc<fdb::Database>,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<super::Output>>> {
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
					results.push(Some(super::Output {
						subtree: value.subtree_stored,
					}));
				},
				None => results.push(None),
			}
		}
		Ok(results)
	}

	pub(crate) async fn try_get_object_stored_and_metadata_fdb(
		&self,
		database: &Arc<fdb::Database>,
		id: &tg::object::Id,
	) -> tg::Result<Option<(super::Output, tg::object::Metadata)>> {
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
				Ok(Some((
					super::Output {
						subtree: value.subtree_stored,
					},
					tg::object::Metadata {
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
					},
				)))
			},
			None => Ok(None),
		}
	}

	pub(crate) async fn try_get_object_stored_and_metadata_batch_fdb(
		&self,
		database: &Arc<fdb::Database>,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(super::Output, tg::object::Metadata)>>> {
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
					results.push(Some((
						super::Output {
							subtree: value.subtree_stored,
						},
						tg::object::Metadata {
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
						},
					)));
				},
				None => results.push(None),
			}
		}
		Ok(results)
	}

	pub(crate) async fn try_touch_object_and_get_stored_and_metadata_fdb(
		&self,
		database: &Arc<fdb::Database>,
		id: &tg::object::Id,
		touched_at: i64,
	) -> tg::Result<Option<(super::Output, tg::object::Metadata)>> {
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let key = Key::Object(id).pack_to_vec();
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get object"))?;

		match existing {
			Some(bytes) => {
				let mut value: ObjectValue = tangram_serialize::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize object"))?;

				if touched_at > value.touched_at {
					let old_touched_at = value.touched_at;
					value.touched_at = touched_at;

					let value_bytes = tangram_serialize::to_vec(&value)
						.map_err(|source| tg::error!(!source, "failed to serialize object"))?;
					txn.set(&key, &value_bytes);

					// Update secondary index.
					let ref_zero = value.reference_count == Some(0);
					let old_key = Key::ObjectByTouchedAt {
						touched_at: old_touched_at,
						reference_count_zero: ref_zero,
						id,
					}
					.pack_to_vec();
					txn.clear(&old_key);

					let new_key = Key::ObjectByTouchedAt {
						touched_at: value.touched_at,
						reference_count_zero: ref_zero,
						id,
					}
					.pack_to_vec();
					txn.set(&new_key, &[]);

					txn.commit()
						.await
						.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
				}

				Ok(Some((
					super::Output {
						subtree: value.subtree_stored,
					},
					tg::object::Metadata {
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
					},
				)))
			},
			None => Ok(None),
		}
	}

	pub(crate) async fn try_touch_object_and_get_stored_and_metadata_batch_fdb(
		&self,
		database: &Arc<fdb::Database>,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(super::Output, tg::object::Metadata)>>> {
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let mut results = Vec::with_capacity(ids.len());
		let mut modified = false;

		for id in ids {
			let key = Key::Object(id).pack_to_vec();
			let existing = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get object"))?;

			match existing {
				Some(bytes) => {
					let mut value: ObjectValue = tangram_serialize::from_slice(&bytes)
						.map_err(|source| tg::error!(!source, "failed to deserialize object"))?;

					if touched_at > value.touched_at {
						let old_touched_at = value.touched_at;
						value.touched_at = touched_at;

						let value_bytes = tangram_serialize::to_vec(&value)
							.map_err(|source| tg::error!(!source, "failed to serialize object"))?;
						txn.set(&key, &value_bytes);

						// Update secondary index.
						let ref_zero = value.reference_count == Some(0);
						let old_key = Key::ObjectByTouchedAt {
							touched_at: old_touched_at,
							reference_count_zero: ref_zero,
							id,
						}
						.pack_to_vec();
						txn.clear(&old_key);

						let new_key = Key::ObjectByTouchedAt {
							touched_at: value.touched_at,
							reference_count_zero: ref_zero,
							id,
						}
						.pack_to_vec();
						txn.set(&new_key, &[]);

						modified = true;
					}

					results.push(Some((
						super::Output {
							subtree: value.subtree_stored,
						},
						tg::object::Metadata {
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
						},
					)));
				},
				None => results.push(None),
			}
		}

		if modified {
			txn.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(results)
	}
}
