use {
	super::Output,
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn try_get_process_complete_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.clone();
				move |connection| Self::try_get_process_complete_sqlite_sync(connection, &id)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_complete_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		#[derive(db::sqlite::row::Deserialize)]
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
				where id = ?1;
			",
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![id.to_bytes().to_vec()];
		let mut rows = statement
			.query(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
		let complete = Output {
			children: row.children_complete,
			children_commands: row.children_commands_complete,
			children_outputs: row.children_outputs_complete,
			command: row.command_complete,
			output: row.output_complete,
		};
		Ok(Some(complete))
	}

	pub(crate) async fn try_get_process_complete_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let ids = ids.to_owned();
				move |connection| Self::try_get_process_complete_batch_sqlite_sync(connection, &ids)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_complete_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		#[derive(db::sqlite::row::Deserialize)]
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
				where id = ?1;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut completes = Vec::new();
		for id in ids {
			let params = sqlite::params![id.to_bytes().to_vec()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				completes.push(None);
				continue;
			};
			let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
				.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
			let complete = Output {
				children: row.children_complete,
				children_commands: row.children_commands_complete,
				children_outputs: row.children_outputs_complete,
				command: row.command_complete,
				output: row.output_complete,
			};
			completes.push(Some(complete));
		}
		Ok(completes)
	}

	pub(crate) async fn try_get_process_complete_and_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let ids = ids.to_owned();
				move |connection| {
					Self::try_get_process_complete_and_metadata_batch_sqlite_sync(connection, &ids)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_complete_and_metadata_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			children_complete: u64,
			children_count: Option<u64>,
			children_commands_complete: u64,
			children_commands_count: Option<u64>,
			children_commands_depth: Option<u64>,
			children_commands_weight: Option<u64>,
			children_outputs_complete: u64,
			children_outputs_count: Option<u64>,
			children_outputs_depth: Option<u64>,
			children_outputs_weight: Option<u64>,
			command_complete: u64,
			command_count: Option<u64>,
			command_depth: Option<u64>,
			command_weight: Option<u64>,
			output_complete: u64,
			output_count: Option<u64>,
			output_depth: Option<u64>,
			output_weight: Option<u64>,
		}
		let statement = indoc!(
			"
				select
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
				from processes
				where id = ?1;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut outputs = Vec::new();
		for id in ids {
			let params = sqlite::params![id.to_bytes().to_vec()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				outputs.push(None);
				continue;
			};
			let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
				.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
			let complete = Output {
				children: row.children_complete != 0,
				children_commands: row.children_commands_complete != 0,
				children_outputs: row.children_outputs_complete != 0,
				command: row.command_complete != 0,
				output: row.output_complete != 0,
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
			outputs.push(Some((complete, metadata)));
		}
		Ok(outputs)
	}

	pub(crate) async fn try_touch_process_and_get_complete_and_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(Output, tg::process::Metadata)>> {
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| {
					Self::try_touch_process_and_get_complete_and_metadata_sqlite_sync(
						connection, &id, touched_at,
					)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_touch_process_and_get_complete_and_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::process::Id,
		touched_at: i64,
	) -> tg::Result<Option<(Output, tg::process::Metadata)>> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			children_complete: u64,
			children_count: Option<u64>,
			children_commands_complete: u64,
			children_commands_count: Option<u64>,
			children_commands_depth: Option<u64>,
			children_commands_weight: Option<u64>,
			children_outputs_complete: u64,
			children_outputs_count: Option<u64>,
			children_outputs_depth: Option<u64>,
			children_outputs_weight: Option<u64>,
			command_complete: u64,
			command_count: Option<u64>,
			command_depth: Option<u64>,
			command_weight: Option<u64>,
			output_complete: u64,
			output_count: Option<u64>,
			output_depth: Option<u64>,
			output_weight: Option<u64>,
		}
		let statement = indoc!(
			"
				update processes
				set touched_at = max(?1, touched_at)
				where id = ?2
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
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![touched_at, id.to_bytes().to_vec()];
		let mut rows = statement
			.query(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
		let complete = Output {
			children: row.children_complete != 0,
			children_commands: row.children_commands_complete != 0,
			children_outputs: row.children_outputs_complete != 0,
			command: row.command_complete != 0,
			output: row.output_complete != 0,
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
		Ok(Some((complete, metadata)))
	}

	pub(crate) async fn try_touch_process_and_get_complete_and_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let ids = ids.to_owned();
				move |connection| {
					Self::try_touch_process_and_get_complete_and_metadata_batch_sqlite_sync(
						connection, &ids, touched_at,
					)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_touch_process_and_get_complete_and_metadata_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			children_complete: u64,
			children_count: Option<u64>,
			children_commands_complete: u64,
			children_commands_count: Option<u64>,
			children_commands_depth: Option<u64>,
			children_commands_weight: Option<u64>,
			children_outputs_complete: u64,
			children_outputs_count: Option<u64>,
			children_outputs_depth: Option<u64>,
			children_outputs_weight: Option<u64>,
			command_complete: u64,
			command_count: Option<u64>,
			command_depth: Option<u64>,
			command_weight: Option<u64>,
			output_complete: u64,
			output_count: Option<u64>,
			output_depth: Option<u64>,
			output_weight: Option<u64>,
		}
		let statement = indoc!(
			"
				update processes
				set touched_at = max(?1, touched_at)
				where id = ?2
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
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut outputs = Vec::new();
		for id in ids {
			let params = sqlite::params![touched_at, id.to_bytes().to_vec()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				outputs.push(None);
				continue;
			};
			let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
				.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
			let complete = Output {
				children: row.children_complete != 0,
				children_commands: row.children_commands_complete != 0,
				children_outputs: row.children_outputs_complete != 0,
				command: row.command_complete != 0,
				output: row.output_complete != 0,
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
			outputs.push(Some((complete, metadata)));
		}
		Ok(outputs)
	}
}
