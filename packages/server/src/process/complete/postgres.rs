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
	pub(crate) async fn try_get_process_complete_postgres(
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
			children_complete: bool,
			children_commands_complete: bool,
			children_outputs_complete: bool,
			command_complete: bool,
			output_complete: bool,
		}
		let statement = indoc!(
			"
				select
					children_complete,
					children_commands_complete,
					children_outputs_complete,
					command_complete,
					output_complete
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
				children: row.children_complete,
				children_commands: row.children_commands_complete,
				children_outputs: row.children_outputs_complete,
				command: row.command_complete,
				output: row.output_complete,
			});

		// Drop the connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_process_complete_batch_postgres(
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
			children_complete: bool,
			children_commands_complete: bool,
			children_outputs_complete: bool,
			command_complete: bool,
			output_complete: bool,
		}
		let statement = indoc!(
			"
				select
					processes.id,
					children_complete,
					children_commands_complete,
					children_outputs_complete,
					command_complete,
					output_complete
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
					children: row.children_complete,
					children_commands: row.children_commands_complete,
					children_outputs: row.children_outputs_complete,
					command: row.command_complete,
					output: row.output_complete,
				};
				Some(output)
			})
			.collect::<tg::Result<_>>()?;
		Ok(outputs)
	}

	pub(crate) async fn try_get_process_complete_and_metadata_batch_postgres(
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
			children_complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_count: Option<u64>,
			children_commands_complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_commands_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_commands_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_commands_weight: Option<u64>,
			children_outputs_complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_outputs_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_outputs_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_outputs_weight: Option<u64>,
			command_complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			command_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			command_weight: Option<u64>,
			output_complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			output_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			output_weight: Option<u64>,
		}
		let statement = indoc!(
			"
				select
					processes.id,
					children_complete,
					children_count,
					children_commands_complete,
					children_commands_count,
					children_commands_depth,
					children_commands_weight,
					children_outputs_complete,
					children_outputs_count,
					children_outputs_depth,
					children_outputs_weight,
					command_complete,
					command_count,
					command_depth,
					command_weight,
					output_complete,
					output_count,
					output_depth,
					output_weight
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
				let complete = Output {
					children: row.children_complete,
					children_commands: row.children_commands_complete,
					children_outputs: row.children_outputs_complete,
					command: row.command_complete,
					output: row.output_complete,
				};
				let children = tg::process::metadata::Children {
					count: row.children_count,
				};
				let command = tg::object::Metadata {
					count: row.command_count,
					depth: row.command_depth,
					weight: row.command_weight,
				};
				let children_commands = tg::object::Metadata {
					count: row.children_commands_count,
					depth: row.children_commands_depth,
					weight: row.children_commands_weight,
				};
				let output = tg::object::Metadata {
					count: row.output_count,
					depth: row.output_depth,
					weight: row.output_weight,
				};
				let children_outputs = tg::object::Metadata {
					count: row.children_outputs_count,
					depth: row.children_outputs_depth,
					weight: row.children_outputs_weight,
				};
				let metadata = tg::process::Metadata {
					children,
					children_commands,
					children_outputs,
					command,
					output,
				};
				(row.id, (complete, metadata))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(output)
	}

	pub(crate) async fn try_touch_process_and_get_complete_and_metadata_postgres(
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
			children_complete: bool,
			children_count: Option<u64>,
			children_commands_complete: bool,
			children_commands_count: Option<u64>,
			children_commands_depth: Option<u64>,
			children_commands_weight: Option<u64>,
			children_outputs_complete: bool,
			children_outputs_count: Option<u64>,
			children_outputs_depth: Option<u64>,
			children_outputs_weight: Option<u64>,
			command_complete: bool,
			command_count: Option<u64>,
			command_depth: Option<u64>,
			command_weight: Option<u64>,
			output_complete: bool,
			output_count: Option<u64>,
			output_depth: Option<u64>,
			output_weight: Option<u64>,
		}
		let statement = indoc!(
			"
				update processes
				set touched_at = greatest($1::int8, touched_at)
				where id = $2
				returning
					children_complete,
					children_count,
					children_commands_complete,
					children_commands_count,
					children_commands_depth,
					children_commands_weight,
					children_outputs_complete,
					children_outputs_count,
					children_outputs_depth,
					children_outputs_weight,
					command_complete,
					command_count,
					command_depth,
					command_weight,
					output_complete,
					output_count,
					output_depth,
					output_weight;
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
			let complete = Output {
				children: row.children_complete,
				children_commands: row.children_commands_complete,
				children_outputs: row.children_outputs_complete,
				command: row.command_complete,
				output: row.output_complete,
			};
			let children = tg::process::metadata::Children {
				count: row.children_count,
			};
			let command = tg::object::Metadata {
				count: row.command_count,
				depth: row.command_depth,
				weight: row.command_weight,
			};
			let children_commands = tg::object::Metadata {
				count: row.children_commands_count,
				depth: row.children_commands_depth,
				weight: row.children_commands_weight,
			};
			let output = tg::object::Metadata {
				count: row.output_count,
				depth: row.output_depth,
				weight: row.output_weight,
			};
			let children_outputs = tg::object::Metadata {
				count: row.children_outputs_count,
				depth: row.children_outputs_depth,
				weight: row.children_outputs_weight,
			};
			let metadata = tg::process::Metadata {
				children,
				children_commands,
				children_outputs,
				command,
				output,
			};
			(complete, metadata)
		});

		Ok(output)
	}

	pub(crate) async fn try_touch_process_and_get_complete_and_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		let options = tangram_futures::retry::Options::default();
		tangram_futures::retry(&options, || {
			self.try_touch_process_and_get_complete_and_metadata_batch_postgres_attempt(
				database, ids, touched_at,
			)
		})
		.await
	}

	async fn try_touch_process_and_get_complete_and_metadata_batch_postgres_attempt(
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
			children_complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_count: Option<u64>,
			children_commands_complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_commands_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_commands_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_commands_weight: Option<u64>,
			children_outputs_complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_outputs_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_outputs_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			children_outputs_weight: Option<u64>,
			command_complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			command_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			command_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			command_weight: Option<u64>,
			output_complete: bool,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			output_count: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			output_depth: Option<u64>,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<i64>>")]
			output_weight: Option<u64>,
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
					children_complete,
					children_count,
					children_commands_complete,
					children_commands_count,
					children_commands_depth,
					children_commands_weight,
					children_outputs_complete,
					children_outputs_count,
					children_outputs_depth,
					children_outputs_weight,
					command_complete,
					command_count,
					command_depth,
					command_weight,
					output_complete,
					output_count,
					output_depth,
					output_weight;
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
				let complete = Output {
					children: row.children_complete,
					children_commands: row.children_commands_complete,
					children_outputs: row.children_outputs_complete,
					command: row.command_complete,
					output: row.output_complete,
				};
				let children = tg::process::metadata::Children {
					count: row.children_count,
				};
				let command = tg::object::Metadata {
					count: row.command_count,
					depth: row.command_depth,
					weight: row.command_weight,
				};
				let children_commands = tg::object::Metadata {
					count: row.children_commands_count,
					depth: row.children_commands_depth,
					weight: row.children_commands_weight,
				};
				let output = tg::object::Metadata {
					count: row.output_count,
					depth: row.output_depth,
					weight: row.output_weight,
				};
				let children_outputs = tg::object::Metadata {
					count: row.children_outputs_count,
					depth: row.children_outputs_depth,
					weight: row.children_outputs_weight,
				};
				let metadata = tg::process::Metadata {
					children,
					children_commands,
					children_outputs,
					command,
					output,
				};
				(row.id, (complete, metadata))
			})
			.collect::<tg::Result<HashMap<_, _, tg::id::BuildHasher>>>()?;
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(ControlFlow::Break(output))
	}
}
