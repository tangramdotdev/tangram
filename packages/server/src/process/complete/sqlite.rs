use super::Output;
use crate::Server;
use indoc::indoc;
use rusqlite as sqlite;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub(crate) async fn try_get_process_complete_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the metadata.
		#[derive(serde::Deserialize)]
		struct Row {
			pub children_complete: bool,
			pub commands_complete: bool,
			pub outputs_complete: bool,
		}
		let statement = indoc!(
			"
				select children_complete, commands_complete, outputs_complete
				from processes
				where id = ?1;
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
				commands: row.commands_complete,
				outputs: row.outputs_complete,
			});

		// Drop the database connection.
		drop(connection);

		Ok(output)
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
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
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
					commands_complete,
					outputs_complete
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
			let commands_complete = row.get_unwrap(1);
			let outputs_complete = row.get_unwrap(2);
			let complete = Output {
				children: children_complete,
				commands: commands_complete,
				outputs: outputs_complete,
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
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
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
		index: &sqlite::Connection,
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
					commands_complete,
					commands_count,
					commands_depth,
					commands_weight,
					outputs_complete,
					outputs_count,
					outputs_depth,
					outputs_weight;
			"
		);
		let mut statement = index
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
			== 1;
		let children_count = row
			.get::<_, Option<u64>>(1)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let commands_complete = row
			.get::<_, u64>(2)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			== 1;
		let commands_count = row
			.get::<_, Option<u64>>(3)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let commands_depth = row
			.get::<_, Option<u64>>(4)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let commands_weight = row
			.get::<_, Option<u64>>(5)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let outputs_complete = row
			.get::<_, u64>(6)
			.map_err(|source| tg::error!(!source, "expected an integer"))?
			== 1;
		let outputs_count = row
			.get::<_, Option<u64>>(7)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let outputs_depth = row
			.get::<_, Option<u64>>(8)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let outputs_weight = row
			.get::<_, Option<u64>>(9)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let complete = Output {
			children: children_complete,
			commands: commands_complete,
			outputs: outputs_complete,
		};
		let children = tg::process::metadata::Children {
			count: children_count,
		};
		let commands = tg::object::Metadata {
			count: commands_count,
			depth: commands_depth,
			weight: commands_weight,
		};
		let outputs = tg::object::Metadata {
			count: outputs_count,
			depth: outputs_depth,
			weight: outputs_weight,
		};
		let metadata = tg::process::Metadata {
			children,
			commands,
			outputs,
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
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
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
					commands_complete,
					commands_count,
					commands_depth,
					commands_weight,
					outputs_complete,
					outputs_count,
					outputs_depth,
					outputs_weight;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut output = Vec::new();
		for id in ids {
			let params = sqlite::params![touched_at, id.to_bytes().to_vec()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				output.push(None);
				continue;
			};
			let children_complete = row.get_unwrap::<_, u64>(0) != 0;
			let children_count = row.get_unwrap(1);
			let commands_complete = row.get_unwrap::<_, u64>(2) != 0;
			let commands_count = row.get_unwrap(3);
			let commands_depth = row.get_unwrap(4);
			let commands_weight = row.get_unwrap(5);
			let outputs_complete = row.get_unwrap::<_, u64>(6) != 0;
			let outputs_count = row.get_unwrap(7);
			let outputs_depth = row.get_unwrap(8);
			let outputs_weight = row.get_unwrap(9);
			let complete = Output {
				children: children_complete,
				commands: commands_complete,
				outputs: outputs_complete,
			};
			let children = tg::process::metadata::Children {
				count: children_count,
			};
			let commands = tg::object::Metadata {
				count: commands_count,
				depth: commands_depth,
				weight: commands_weight,
			};
			let outputs = tg::object::Metadata {
				count: outputs_count,
				depth: outputs_depth,
				weight: outputs_weight,
			};
			let metadata = tg::process::Metadata {
				children,
				commands,
				outputs,
			};
			output.push(Some((complete, metadata)));
		}
		Ok(output)
	}
}
