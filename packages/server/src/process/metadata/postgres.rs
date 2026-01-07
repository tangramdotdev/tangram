use {
	crate::Server,
	indoc::{formatdoc, indoc},
	std::collections::HashMap,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_process_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		// Get a connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get an index connection"))?;

		// Get the process metadata.
		let p = connection.p();
		#[derive(db::row::Deserialize)]
		struct Row {
			node_command_count: Option<u64>,
			node_command_depth: Option<u64>,
			node_command_size: Option<u64>,
			node_error_count: Option<u64>,
			node_error_depth: Option<u64>,
			node_error_size: Option<u64>,
			node_log_count: Option<u64>,
			node_log_depth: Option<u64>,
			node_log_size: Option<u64>,
			node_output_count: Option<u64>,
			node_output_depth: Option<u64>,
			node_output_size: Option<u64>,
			subtree_command_count: Option<u64>,
			subtree_command_depth: Option<u64>,
			subtree_command_size: Option<u64>,
			subtree_error_count: Option<u64>,
			subtree_error_depth: Option<u64>,
			subtree_error_size: Option<u64>,
			subtree_log_count: Option<u64>,
			subtree_log_depth: Option<u64>,
			subtree_log_size: Option<u64>,
			subtree_output_count: Option<u64>,
			subtree_output_depth: Option<u64>,
			subtree_output_size: Option<u64>,
			subtree_count: Option<u64>,
		}
		let statement = formatdoc!(
			"
				select
					node_command_count,
					node_command_depth,
					node_command_size,
					node_error_count,
					node_error_depth,
					node_error_size,
					node_log_count,
					node_log_depth,
					node_log_size,
					node_output_count,
					node_output_depth,
					node_output_size,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_error_count,
					subtree_error_depth,
					subtree_error_size,
					subtree_log_count,
					subtree_log_depth,
					subtree_log_size,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_count
				from processes
				where processes.id = {p}1;
			"
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| {
				let node = tg::process::metadata::Node {
					command: tg::object::metadata::Subtree {
						count: row.node_command_count,
						depth: row.node_command_depth,
						size: row.node_command_size,
						solvable: None,
						solved: None,
					},
					error: tg::object::metadata::Subtree {
						count: row.node_error_count,
						depth: row.node_error_depth,
						size: row.node_error_size,
						solvable: None,
						solved: None,
					},
					log: tg::object::metadata::Subtree {
						count: row.node_log_count,
						depth: row.node_log_depth,
						size: row.node_log_size,
						solvable: None,
						solved: None,
					},
					output: tg::object::metadata::Subtree {
						count: row.node_output_count,
						depth: row.node_output_depth,
						size: row.node_output_size,
						solvable: None,
						solved: None,
					},
				};
				let subtree = tg::process::metadata::Subtree {
					command: tg::object::metadata::Subtree {
						count: row.subtree_command_count,
						depth: row.subtree_command_depth,
						size: row.subtree_command_size,
						solvable: None,
						solved: None,
					},
					error: tg::object::metadata::Subtree {
						count: row.subtree_error_count,
						depth: row.subtree_error_depth,
						size: row.subtree_error_size,
						solvable: None,
						solved: None,
					},
					log: tg::object::metadata::Subtree {
						count: row.subtree_log_count,
						depth: row.subtree_log_depth,
						size: row.subtree_log_size,
						solvable: None,
						solved: None,
					},
					output: tg::object::metadata::Subtree {
						count: row.subtree_output_count,
						depth: row.subtree_output_depth,
						size: row.subtree_output_size,
						solvable: None,
						solved: None,
					},
					count: row.subtree_count,
				};
				tg::process::Metadata { node, subtree }
			});

		// Drop the index connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_process_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}

		// Get a connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the process metadata.
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(try_from = "Vec<u8>")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_size: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_error_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_error_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_error_size: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_log_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_log_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_log_size: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_size: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_size: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_error_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_error_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_error_size: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_log_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_log_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_log_size: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_size: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_count: Option<u64>,
		}
		let statement = indoc!(
			"
				select
					processes.id,
					node_command_count,
					node_command_depth,
					node_command_size,
					node_error_count,
					node_error_depth,
					node_error_size,
					node_log_count,
					node_log_depth,
					node_log_size,
					node_output_count,
					node_output_depth,
					node_output_size,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_error_count,
					subtree_error_depth,
					subtree_error_size,
					subtree_log_count,
					subtree_log_depth,
					subtree_log_size,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_count
				from unnest($1::bytea[]) as ids (id)
				left join processes on processes.id = ids.id;
			"
		);
		let ids_bytes = ids
			.iter()
			.map(|id| id.to_bytes().to_vec())
			.collect::<Vec<_>>();
		let output = connection
			.inner()
			.query(statement, &[&ids_bytes])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|row| {
				let row = <Row as db::postgres::row::Deserialize>::deserialize(&row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				let metadata = tg::process::Metadata {
					node: tg::process::metadata::Node {
						command: tg::object::metadata::Subtree {
							count: row.node_command_count,
							depth: row.node_command_depth,
							size: row.node_command_size,
							solvable: None,
							solved: None,
						},
						error: tg::object::metadata::Subtree {
							count: row.node_error_count,
							depth: row.node_error_depth,
							size: row.node_error_size,
							solvable: None,
							solved: None,
						},
						log: tg::object::metadata::Subtree {
							count: row.node_log_count,
							depth: row.node_log_depth,
							size: row.node_log_size,
							solvable: None,
							solved: None,
						},
						output: tg::object::metadata::Subtree {
							count: row.node_output_count,
							depth: row.node_output_depth,
							size: row.node_output_size,
							solvable: None,
							solved: None,
						},
					},
					subtree: tg::process::metadata::Subtree {
						command: tg::object::metadata::Subtree {
							count: row.subtree_command_count,
							depth: row.subtree_command_depth,
							size: row.subtree_command_size,
							solvable: None,
							solved: None,
						},
						error: tg::object::metadata::Subtree {
							count: row.subtree_error_count,
							depth: row.subtree_error_depth,
							size: row.subtree_error_size,
							solvable: None,
							solved: None,
						},
						log: tg::object::metadata::Subtree {
							count: row.subtree_log_count,
							depth: row.subtree_log_depth,
							size: row.subtree_log_size,
							solvable: None,
							solved: None,
						},
						output: tg::object::metadata::Subtree {
							count: row.subtree_output_count,
							depth: row.subtree_output_depth,
							size: row.subtree_output_size,
							solvable: None,
							solved: None,
						},
						count: row.subtree_count,
					},
				};
				Ok((row.id, metadata))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;

		// Drop the connection.
		drop(connection);

		// Return the results in the same order as the input ids.
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(output)
	}
}
