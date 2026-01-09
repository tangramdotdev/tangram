use {
	crate::{
		Server,
		index::fdb::{Key, ProcessValue},
	},
	foundationdb as fdb,
	foundationdb_tuple::TuplePack as _,
	std::sync::Arc,
	tangram_client as tg,
};

impl Server {
	pub(crate) async fn try_get_process_stored_fdb(
		&self,
		database: &Arc<fdb::Database>,
		id: &tg::process::Id,
	) -> tg::Result<Option<super::Output>> {
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let key = Key::Process(id).pack_to_vec();
		let value = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process"))?;

		match value {
			Some(bytes) => {
				let value: ProcessValue = tangram_serialize::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize process"))?;
				Ok(Some(super::Output {
					node_command: value.node_command_stored,
					node_error: value.node_error_stored,
					node_log: value.node_log_stored,
					node_output: value.node_output_stored,
					subtree_command: value.subtree_command_stored,
					subtree_error: value.subtree_error_stored,
					subtree_log: value.subtree_log_stored,
					subtree_output: value.subtree_output_stored,
					subtree: value.subtree_stored,
				}))
			},
			None => Ok(None),
		}
	}

	pub(crate) async fn try_get_process_stored_batch_fdb(
		&self,
		database: &Arc<fdb::Database>,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<super::Output>>> {
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let mut results = Vec::with_capacity(ids.len());
		for id in ids {
			let key = Key::Process(id).pack_to_vec();
			let value = txn
				.get(&key, false)
				.await
				.map_err(|source| tg::error!(!source, "failed to get process"))?;

			match value {
				Some(bytes) => {
					let value: ProcessValue = tangram_serialize::from_slice(&bytes)
						.map_err(|source| tg::error!(!source, "failed to deserialize process"))?;
					results.push(Some(super::Output {
						node_command: value.node_command_stored,
						node_error: value.node_error_stored,
						node_log: value.node_log_stored,
						node_output: value.node_output_stored,
						subtree_command: value.subtree_command_stored,
						subtree_error: value.subtree_error_stored,
						subtree_log: value.subtree_log_stored,
						subtree_output: value.subtree_output_stored,
						subtree: value.subtree_stored,
					}));
				},
				None => results.push(None),
			}
		}
		Ok(results)
	}

	pub(crate) async fn try_get_process_stored_and_metadata_batch_fdb(
		&self,
		database: &Arc<fdb::Database>,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(super::Output, tg::process::Metadata)>>> {
		let stored = self.try_get_process_stored_batch_fdb(database, ids).await?;
		let metadata = self
			.try_get_process_metadata_batch_fdb(database, ids)
			.await?;

		Ok(stored
			.into_iter()
			.zip(metadata)
			.map(|(s, m)| match (s, m) {
				(Some(s), Some(m)) => Some((s, m)),
				_ => None,
			})
			.collect())
	}

	pub(crate) async fn try_touch_process_and_get_stored_and_metadata_fdb(
		&self,
		database: &Arc<fdb::Database>,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(super::Output, tg::process::Metadata)>> {
		let txn = database
			.create_trx()
			.map_err(|source| tg::error!(!source, "failed to create transaction"))?;

		let key = Key::Process(id).pack_to_vec();
		let existing = txn
			.get(&key, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get process"))?;

		match existing {
			Some(bytes) => {
				let mut value: ProcessValue = tangram_serialize::from_slice(&bytes)
					.map_err(|source| tg::error!(!source, "failed to deserialize process"))?;

				if touched_at > value.touched_at {
					let old_touched_at = value.touched_at;
					value.touched_at = touched_at;

					let value_bytes = tangram_serialize::to_vec(&value)
						.map_err(|source| tg::error!(!source, "failed to serialize process"))?;
					txn.set(&key, &value_bytes);

					// Update secondary index.
					let ref_zero = value.reference_count == Some(0);
					let old_key = Key::ProcessByTouchedAt {
						touched_at: old_touched_at,
						reference_count_zero: ref_zero,
						id,
					}
					.pack_to_vec();
					txn.clear(&old_key);

					let new_key = Key::ProcessByTouchedAt {
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

				let stored = super::Output {
					node_command: value.node_command_stored,
					node_error: value.node_error_stored,
					node_log: value.node_log_stored,
					node_output: value.node_output_stored,
					subtree_command: value.subtree_command_stored,
					subtree_error: value.subtree_error_stored,
					subtree_log: value.subtree_log_stored,
					subtree_output: value.subtree_output_stored,
					subtree: value.subtree_stored,
				};

				let metadata = tg::process::Metadata {
					node: tg::process::metadata::Node {
						command: tg::object::metadata::Subtree {
							count: value.node_command_count,
							depth: value.node_command_depth,
							size: value.node_command_size,
							solvable: None,
							solved: None,
						},
						error: tg::object::metadata::Subtree {
							count: value.node_error_count,
							depth: value.node_error_depth,
							size: value.node_error_size,
							solvable: None,
							solved: None,
						},
						log: tg::object::metadata::Subtree {
							count: value.node_log_count,
							depth: value.node_log_depth,
							size: value.node_log_size,
							solvable: None,
							solved: None,
						},
						output: tg::object::metadata::Subtree {
							count: value.node_output_count,
							depth: value.node_output_depth,
							size: value.node_output_size,
							solvable: None,
							solved: None,
						},
					},
					subtree: tg::process::metadata::Subtree {
						count: value.subtree_count,
						command: tg::object::metadata::Subtree {
							count: value.subtree_command_count,
							depth: value.subtree_command_depth,
							size: value.subtree_command_size,
							solvable: None,
							solved: None,
						},
						error: tg::object::metadata::Subtree {
							count: value.subtree_error_count,
							depth: value.subtree_error_depth,
							size: value.subtree_error_size,
							solvable: None,
							solved: None,
						},
						log: tg::object::metadata::Subtree {
							count: value.subtree_log_count,
							depth: value.subtree_log_depth,
							size: value.subtree_log_size,
							solvable: None,
							solved: None,
						},
						output: tg::object::metadata::Subtree {
							count: value.subtree_output_count,
							depth: value.subtree_output_depth,
							size: value.subtree_output_size,
							solvable: None,
							solved: None,
						},
					},
				};

				Ok(Some((stored, metadata)))
			},
			None => Ok(None),
		}
	}

	pub(crate) async fn try_touch_process_and_get_stored_and_metadata_batch_fdb(
		&self,
		database: &Arc<fdb::Database>,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(super::Output, tg::process::Metadata)>>> {
		let mut results = Vec::with_capacity(ids.len());
		for id in ids {
			let result = self
				.try_touch_process_and_get_stored_and_metadata_fdb(database, id, touched_at)
				.await?;
			results.push(result);
		}
		Ok(results)
	}
}
