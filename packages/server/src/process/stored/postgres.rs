use {
	super::Output,
	crate::Server,
	indoc::indoc,
	itertools::Itertools as _,
	std::{collections::HashMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_process_stored_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the metadata.
		#[derive(db::row::Deserialize)]
		struct Row {
			node_command_stored: bool,
			node_output_stored: bool,
			subtree_command_stored: bool,
			subtree_output_stored: bool,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select
					node_command_stored,
					node_output_stored,
					subtree_command_stored,
					subtree_output_stored,
					subtree_stored
				from processes
				where id = $1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| Output {
				node_command: row.node_command_stored,
				node_output: row.node_output_stored,
				subtree_command: row.subtree_command_stored,
				subtree_output: row.subtree_output_stored,
				subtree: row.subtree_stored,
			});

		// Drop the connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_process_stored_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<Vec<u8>>>")]
			id: Option<tg::process::Id>,
			node_command_stored: bool,
			node_output_stored: bool,
			subtree_command_stored: bool,
			subtree_output_stored: bool,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select
					processes.id,
					node_command_stored,
					node_output_stored,
					subtree_command_stored,
					subtree_output_stored,
					subtree_stored
				from unnest($1::bytea[]) as ids (id)
				left join processes on processes.id = ids.id;
			",
		);
		let outputs = connection
			.inner()
			.query(
				statement,
				&[&ids
					.iter()
					.map(|id| id.to_bytes().to_vec())
					.collect::<Vec<_>>()],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.iter()
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.map_ok(|row| {
				row.id?;
				let output = Output {
					node_command: row.node_command_stored,
					node_output: row.node_output_stored,
					subtree_command: row.subtree_command_stored,
					subtree_output: row.subtree_output_stored,
					subtree: row.subtree_stored,
				};
				Some(output)
			})
			.collect::<tg::Result<_>>()?;
		Ok(outputs)
	}

	pub(crate) async fn try_get_process_stored_and_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::postgres::value::TryFrom<Vec<u8>>")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_size: Option<u64>,
			node_command_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_size: Option<u64>,
			node_output_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_size: Option<u64>,
			subtree_command_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_size: Option<u64>,
			subtree_output_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_count: Option<u64>,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				select
					processes.id,
					node_command_count,
					node_command_depth,
					node_command_size,
					node_command_stored,
					node_output_count,
					node_output_depth,
					node_output_size,
					node_output_stored,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_stored,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_stored,
					subtree_count,
					subtree_stored
				from unnest($1::bytea[]) as ids (id)
				left join processes on processes.id = ids.id;
			",
		);
		let output = connection
			.inner()
			.query(
				statement,
				&[&ids
					.iter()
					.map(|id| id.to_bytes().to_vec())
					.collect::<Vec<_>>()],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.iter()
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.map_ok(|row| {
				let stored = Output {
					node_command: row.node_command_stored,
					node_output: row.node_output_stored,
					subtree_command: row.subtree_command_stored,
					subtree_output: row.subtree_output_stored,
					subtree: row.subtree_stored,
				};
				let node = tg::process::metadata::Node {
					command: tg::object::metadata::Subtree {
						count: row.node_command_count,
						depth: row.node_command_depth,
						size: row.node_command_size,
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
					output: tg::object::metadata::Subtree {
						count: row.subtree_output_count,
						depth: row.subtree_output_depth,
						size: row.subtree_output_size,
						solvable: None,
						solved: None,
					},
					count: row.subtree_count,
				};
				let metadata = tg::process::Metadata { node, subtree };
				(row.id, (stored, metadata))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(output)
	}

	pub(crate) async fn try_touch_process_and_get_stored_and_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(Output, tg::process::Metadata)>> {
		// Get a database connection.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		#[derive(db::row::Deserialize)]
		struct Row {
			node_command_count: Option<u64>,
			node_command_depth: Option<u64>,
			node_command_size: Option<u64>,
			node_command_stored: bool,
			node_output_count: Option<u64>,
			node_output_depth: Option<u64>,
			node_output_size: Option<u64>,
			node_output_stored: bool,
			subtree_command_count: Option<u64>,
			subtree_command_depth: Option<u64>,
			subtree_command_size: Option<u64>,
			subtree_command_stored: bool,
			subtree_output_count: Option<u64>,
			subtree_output_depth: Option<u64>,
			subtree_output_size: Option<u64>,
			subtree_output_stored: bool,
			subtree_count: Option<u64>,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				update processes
				set touched_at = greatest($1::int8, touched_at)
				where id = $2
				returning
					node_command_count,
					node_command_depth,
					node_command_size,
					node_command_stored,
					node_output_count,
					node_output_depth,
					node_output_size,
					node_output_stored,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_stored,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_stored,
					subtree_count,
					subtree_stored;
			",
		);
		let params = db::params![touched_at, id.to_bytes()];
		let row = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the connection.
		drop(connection);

		let output = row.map(|row| {
			let stored = Output {
				node_command: row.node_command_stored,
				node_output: row.node_output_stored,
				subtree_command: row.subtree_command_stored,
				subtree_output: row.subtree_output_stored,
				subtree: row.subtree_stored,
			};
			let node = tg::process::metadata::Node {
				command: tg::object::metadata::Subtree {
					count: row.node_command_count,
					depth: row.node_command_depth,
					size: row.node_command_size,
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
				output: tg::object::metadata::Subtree {
					count: row.subtree_output_count,
					depth: row.subtree_output_depth,
					size: row.subtree_output_size,
					solvable: None,
					solved: None,
				},
				count: row.subtree_count,
			};
			let metadata = tg::process::Metadata { node, subtree };
			(stored, metadata)
		});

		Ok(output)
	}

	pub(crate) async fn try_touch_process_and_get_stored_and_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		let options = tangram_futures::retry::Options::default();
		tangram_futures::retry(&options, || {
			self.try_touch_process_and_get_stored_and_metadata_batch_postgres_attempt(
				database, ids, touched_at,
			)
		})
		.await
	}

	async fn try_touch_process_and_get_stored_and_metadata_batch_postgres_attempt(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<ControlFlow<Vec<Option<(Output, tg::process::Metadata)>>, tg::Error>> {
		if ids.is_empty() {
			return Ok(ControlFlow::Break(vec![]));
		}
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::postgres::value::TryFrom<Vec<u8>>")]
			id: tg::process::Id,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_command_size: Option<u64>,
			node_command_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			node_output_size: Option<u64>,
			node_output_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_command_size: Option<u64>,
			subtree_command_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_output_size: Option<u64>,
			subtree_output_stored: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			subtree_count: Option<u64>,
			subtree_stored: bool,
		}
		let statement = indoc!(
			"
				with locked as (
					select processes.id
					from processes
					join unnest($2::bytea[]) as ids (id) on processes.id = ids.id
					order by processes.id
					for update
				)
				update processes
				set touched_at = greatest($1::int8, touched_at)
				from locked
				where processes.id = locked.id
				returning
					processes.id,
					node_command_count,
					node_command_depth,
					node_command_size,
					node_command_stored,
					node_output_count,
					node_output_depth,
					node_output_size,
					node_output_stored,
					subtree_command_count,
					subtree_command_depth,
					subtree_command_size,
					subtree_command_stored,
					subtree_output_count,
					subtree_output_depth,
					subtree_output_size,
					subtree_output_stored,
					subtree_count,
					subtree_stored;
			",
		);
		let result = connection
			.inner()
			.query(
				statement,
				&[
					&touched_at,
					&ids.iter()
						.map(|id| id.to_bytes().to_vec())
						.collect::<Vec<_>>(),
				],
			)
			.await;
		let output = match result {
			Ok(output) => output,
			Err(error) if db::postgres::util::error_is_retryable(&error) => {
				let error = tg::error!(!error, "failed to execute the statement");
				return Ok(ControlFlow::Continue(error));
			},
			Err(error) => {
				let error = tg::error!(!error, "failed to execute the statement");
				return Err(error);
			},
		};
		let output = output
			.iter()
			.map(|row| {
				<Row as db::postgres::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.map_ok(|row| {
				let stored = Output {
					node_command: row.node_command_stored,
					node_output: row.node_output_stored,
					subtree_command: row.subtree_command_stored,
					subtree_output: row.subtree_output_stored,
					subtree: row.subtree_stored,
				};
				let node = tg::process::metadata::Node {
					command: tg::object::metadata::Subtree {
						count: row.node_command_count,
						depth: row.node_command_depth,
						size: row.node_command_size,
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
					output: tg::object::metadata::Subtree {
						count: row.subtree_output_count,
						depth: row.subtree_output_depth,
						size: row.subtree_output_size,
						solvable: None,
						solved: None,
					},
					count: row.subtree_count,
				};
				let metadata = tg::process::Metadata { node, subtree };
				(row.id, (stored, metadata))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(ControlFlow::Break(output))
	}
}
