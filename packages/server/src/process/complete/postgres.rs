use {
	super::Output,
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
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
		#[derive(serde::Deserialize)]
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
			.query_optional_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0)
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
			.into_iter()
			.map(|row| {
				row.get::<_, Option<Vec<u8>>>(0)?;
				let children_complete = row.get::<_, bool>(1);
				let children_commands_complete = row.get::<_, bool>(2);
				let children_outputs_complete = row.get::<_, bool>(3);
				let command_complete = row.get::<_, bool>(4);
				let output_complete = row.get::<_, bool>(5);
				let output = Output {
					children: children_complete,
					children_commands: children_commands_complete,
					children_outputs: children_outputs_complete,
					command: command_complete,
					output: output_complete,
				};
				Some(output)
			})
			.collect();
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
			.into_iter()
			.map(|row| {
				let id = row.get::<_, Vec<u8>>(0);
				let id = tg::process::Id::from_slice(&id).unwrap();
				let children_complete = row.get::<_, bool>(1);
				let children_count = row.get::<_, Option<i64>>(2).map(|v| v.to_u64().unwrap());
				let children_commands_complete = row.get::<_, bool>(3);
				let children_commands_count =
					row.get::<_, Option<i64>>(4).map(|v| v.to_u64().unwrap());
				let children_commands_depth =
					row.get::<_, Option<i64>>(5).map(|v| v.to_u64().unwrap());
				let children_commands_weight =
					row.get::<_, Option<i64>>(6).map(|v| v.to_u64().unwrap());
				let children_outputs_complete = row.get::<_, bool>(7);
				let children_outputs_count =
					row.get::<_, Option<i64>>(8).map(|v| v.to_u64().unwrap());
				let children_outputs_depth =
					row.get::<_, Option<i64>>(9).map(|v| v.to_u64().unwrap());
				let children_outputs_weight =
					row.get::<_, Option<i64>>(10).map(|v| v.to_u64().unwrap());
				let command_complete = row.get::<_, bool>(11);
				let command_count = row.get::<_, Option<i64>>(12).map(|v| v.to_u64().unwrap());
				let command_depth = row.get::<_, Option<i64>>(13).map(|v| v.to_u64().unwrap());
				let command_weight = row.get::<_, Option<i64>>(14).map(|v| v.to_u64().unwrap());
				let output_complete = row.get::<_, bool>(15);
				let output_count = row.get::<_, Option<i64>>(16).map(|v| v.to_u64().unwrap());
				let output_depth = row.get::<_, Option<i64>>(17).map(|v| v.to_u64().unwrap());
				let output_weight = row.get::<_, Option<i64>>(18).map(|v| v.to_u64().unwrap());
				let complete = Output {
					children: children_complete,
					children_commands: children_commands_complete,
					children_outputs: children_outputs_complete,
					command: command_complete,
					output: output_complete,
				};
				let children = tg::process::metadata::Children {
					count: children_count,
				};
				let command = tg::object::Metadata {
					count: command_count,
					depth: command_depth,
					weight: command_weight,
				};
				let children_commands = tg::object::Metadata {
					count: children_commands_count,
					depth: children_commands_depth,
					weight: children_commands_weight,
				};
				let output = tg::object::Metadata {
					count: output_count,
					depth: output_depth,
					weight: output_weight,
				};
				let children_outputs = tg::object::Metadata {
					count: children_outputs_count,
					depth: children_outputs_depth,
					weight: children_outputs_weight,
				};
				let metadata = tg::process::Metadata {
					children,
					children_commands,
					children_outputs,
					command,
					output,
				};
				(id, (complete, metadata))
			})
			.collect::<HashMap<_, _, tg::id::BuildHasher>>();
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

		#[derive(serde::Deserialize)]
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
			.query_optional_into::<db::row::Serde<Row>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0);

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
			.into_iter()
			.map(|row| {
				let id = row.get::<_, Vec<u8>>(0);
				let id = tg::process::Id::from_slice(&id).unwrap();
				let children_complete = row.get::<_, bool>(1);
				let children_count = row.get::<_, Option<i64>>(2).map(|v| v.to_u64().unwrap());
				let children_commands_complete = row.get::<_, bool>(3);
				let children_commands_count =
					row.get::<_, Option<i64>>(4).map(|v| v.to_u64().unwrap());
				let children_commands_depth =
					row.get::<_, Option<i64>>(5).map(|v| v.to_u64().unwrap());
				let children_commands_weight =
					row.get::<_, Option<i64>>(6).map(|v| v.to_u64().unwrap());
				let children_outputs_complete = row.get::<_, bool>(7);
				let children_outputs_count =
					row.get::<_, Option<i64>>(8).map(|v| v.to_u64().unwrap());
				let children_outputs_depth =
					row.get::<_, Option<i64>>(9).map(|v| v.to_u64().unwrap());
				let children_outputs_weight =
					row.get::<_, Option<i64>>(10).map(|v| v.to_u64().unwrap());
				let command_complete = row.get::<_, bool>(11);
				let command_count = row.get::<_, Option<i64>>(12).map(|v| v.to_u64().unwrap());
				let command_depth = row.get::<_, Option<i64>>(13).map(|v| v.to_u64().unwrap());
				let command_weight = row.get::<_, Option<i64>>(14).map(|v| v.to_u64().unwrap());
				let output_complete = row.get::<_, bool>(15);
				let output_count = row.get::<_, Option<i64>>(16).map(|v| v.to_u64().unwrap());
				let output_depth = row.get::<_, Option<i64>>(17).map(|v| v.to_u64().unwrap());
				let output_weight = row.get::<_, Option<i64>>(18).map(|v| v.to_u64().unwrap());
				let complete = Output {
					children: children_complete,
					children_commands: children_commands_complete,
					children_outputs: children_outputs_complete,
					command: command_complete,
					output: output_complete,
				};
				let children = tg::process::metadata::Children {
					count: children_count,
				};
				let command = tg::object::Metadata {
					count: command_count,
					depth: command_depth,
					weight: command_weight,
				};
				let children_commands = tg::object::Metadata {
					count: children_commands_count,
					depth: children_commands_depth,
					weight: children_commands_weight,
				};
				let output = tg::object::Metadata {
					count: output_count,
					depth: output_depth,
					weight: output_weight,
				};
				let children_outputs = tg::object::Metadata {
					count: children_outputs_count,
					depth: children_outputs_depth,
					weight: children_outputs_weight,
				};
				let metadata = tg::process::Metadata {
					children,
					children_commands,
					children_outputs,
					command,
					output,
				};
				(id, (complete, metadata))
			})
			.collect::<HashMap<_, _, tg::id::BuildHasher>>();
		let output = ids.iter().map(|id| output.get(id).cloned()).collect();
		Ok(ControlFlow::Break(output))
	}
}
