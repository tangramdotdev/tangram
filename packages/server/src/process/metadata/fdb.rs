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
	pub(crate) async fn try_get_process_metadata_fdb(
		&self,
		database: &Arc<fdb::Database>,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
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
				Ok(Some(tg::process::Metadata {
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
				}))
			},
			None => Ok(None),
		}
	}

	pub(crate) async fn try_get_process_metadata_batch_fdb(
		&self,
		database: &Arc<fdb::Database>,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
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
					results.push(Some(tg::process::Metadata {
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
					}));
				},
				None => results.push(None),
			}
		}
		Ok(results)
	}
}
