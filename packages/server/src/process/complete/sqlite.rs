use {
	super::Output,
	crate::Server,
	indoc::indoc,
	rusqlite as sqlite, tangram_client as tg,
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
		let children_complete = row.get_unwrap(0);
		let children_commands_complete = row.get_unwrap(1);
		let children_outputs_complete = row.get_unwrap(2);
		let command_complete = row.get_unwrap(3);
		let output_complete = row.get_unwrap(4);
		let complete = Output {
			children: children_complete,
			children_commands: children_commands_complete,
			children_outputs: children_outputs_complete,
			command: command_complete,
			output: output_complete,
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
			let children_complete = row.get_unwrap(0);
			let children_commands_complete = row.get_unwrap(1);
			let children_outputs_complete = row.get_unwrap(2);
			let command_complete = row.get_unwrap(3);
			let output_complete = row.get_unwrap(4);
			let complete = Output {
				children: children_complete,
				children_commands: children_commands_complete,
				children_outputs: children_outputs_complete,
				command: command_complete,
				output: output_complete,
			};
			completes.push(Some(complete));
		}
		Ok(completes)
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
		let children_complete = row
			.get::<_, u64>(0)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let children_count = row
			.get::<_, Option<u64>>(1)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_commands_complete = row
			.get::<_, u64>(2)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let children_commands_count = row
			.get::<_, Option<u64>>(3)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_commands_depth = row
			.get::<_, Option<u64>>(4)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_commands_weight = row
			.get::<_, Option<u64>>(5)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_outputs_complete = row
			.get::<_, u64>(6)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let children_outputs_count = row
			.get::<_, Option<u64>>(7)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_outputs_depth = row
			.get::<_, Option<u64>>(8)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let children_outputs_weight = row
			.get::<_, Option<u64>>(9)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let command_complete = row
			.get::<_, u64>(10)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let command_count = row
			.get::<_, Option<u64>>(11)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let command_depth = row
			.get::<_, Option<u64>>(12)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let command_weight = row
			.get::<_, Option<u64>>(13)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let output_complete = row
			.get::<_, u64>(14)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			!= 0;
		let output_count = row
			.get::<_, Option<u64>>(15)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let output_depth = row
			.get::<_, Option<u64>>(16)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let output_weight = row
			.get::<_, Option<u64>>(17)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
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
					Self::try_get_process_complete_and_metadata_batch_sqlite_sync(
						connection, &ids, touched_at,
					)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_complete_and_metadata_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<(Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
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
			let children_complete = row.get_unwrap::<_, u64>(0) != 0;
			let children_count = row.get_unwrap(1);
			let children_commands_complete = row.get_unwrap::<_, u64>(2) != 0;
			let children_commands_count = row.get_unwrap(3);
			let children_commands_depth = row.get_unwrap(4);
			let children_commands_weight = row.get_unwrap(5);
			let children_outputs_complete = row.get_unwrap::<_, u64>(6) != 0;
			let children_outputs_count = row.get_unwrap(7);
			let children_outputs_depth = row.get_unwrap(8);
			let children_outputs_weight = row.get_unwrap(9);
			let command_complete = row.get_unwrap::<_, u64>(10) != 0;
			let command_count = row.get_unwrap(11);
			let command_depth = row.get_unwrap(12);
			let command_weight = row.get_unwrap(13);
			let output_complete = row.get_unwrap::<_, u64>(14) != 0;
			let output_count = row.get_unwrap(15);
			let output_depth = row.get_unwrap(16);
			let output_weight = row.get_unwrap(17);
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
			outputs.push(Some((complete, metadata)));
		}
		Ok(outputs)
	}
}
