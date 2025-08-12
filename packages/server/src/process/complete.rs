use crate::Server;
use indoc::{formatdoc, indoc};
use rusqlite as sqlite;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub children: bool,
	pub commands: bool,
	pub outputs: bool,
}

impl Server {
	pub(crate) async fn try_get_process_complete(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		match &self.index {
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_complete_sqlite(database, id).await
			},
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_complete_postgres(database, id).await
			},
		}
	}

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
		let p = connection.p();
		let statement = formatdoc!(
			"
				select children_complete, commands_complete, outputs_complete
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| Output {
				children: row.children_complete,
				commands: row.commands_complete,
				outputs: row.outputs_complete,
			});

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	#[cfg(feature = "postgres")]
	pub(crate) async fn try_get_process_complete_postgres(
		&self,
		database: &db::postgres::Database,
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
		let p = connection.p();
		let statement = formatdoc!(
			"
				select children_complete, commands_complete, outputs_complete
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| Output {
				children: row.children_complete,
				commands: row.commands_complete,
				outputs: row.outputs_complete,
			});

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_process_complete_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		match &self.index {
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_complete_batch_sqlite(database, ids)
					.await
			},
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_complete_batch_postgres(database, ids)
					.await
			},
		}
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
			let params = sqlite::params![id.to_string()];
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

	#[cfg(feature = "postgres")]
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
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let statement = indoc!(
			"
				select
					processes.id,
					children_complete,
					commands_complete,
					outputs_complete
				from unnest($1::text[]) as ids (id)
				left join processes on processes.id = ids.id;
			",
		);
		let outputs = connection
			.inner()
			.query(
				statement,
				&[&ids.iter().map(ToString::to_string).collect::<Vec<_>>()],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to query the database"))?
			.into_iter()
			.map(|row| {
				row.get::<_, Option<String>>(0)?;
				let children_complete = row.get::<_, i64>(1) != 0;
				let commands_complete = row.get::<_, i64>(2) != 0;
				let outputs_complete = row.get::<_, i64>(3) != 0;
				let output = Output {
					children: children_complete,
					commands: commands_complete,
					outputs: outputs_complete,
				};
				Some(output)
			})
			.collect();
		Ok(outputs)
	}

	#[allow(dead_code)]
	pub(crate) async fn try_get_process_complete_and_metadata(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<(super::complete::Output, tg::process::Metadata)>> {
		match &self.index {
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_complete_and_metadata_sqlite(database, id)
					.await
			},
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_complete_and_metadata_postgres(database, id)
					.await
			},
		}
	}

	pub(crate) async fn try_get_process_complete_and_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<(super::complete::Output, tg::process::Metadata)>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| {
					Self::try_get_process_complete_and_metadata_sqlite_sync(connection, &id)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_process_complete_and_metadata_sqlite_sync(
		index: &sqlite::Connection,
		id: &tg::process::Id,
	) -> tg::Result<Option<(super::complete::Output, tg::process::Metadata)>> {
		let statement = indoc!(
			"
				select
					children_complete,
					children_count,
					commands_complete,
					commands_count,
					commands_depth,
					commands_weight,
					outputs_complete,
					outputs_count,
					outputs_depth,
					outputs_weight
				from processes
				where id = ?1;
			"
		);
		let mut statement = index
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query([id.to_string()])
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
		let complete = super::complete::Output {
			children: children_complete,
			commands: commands_complete,
			outputs: outputs_complete,
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
			commands,
			count: children_count,
			outputs,
		};
		Ok(Some((complete, metadata)))
	}

	#[cfg(feature = "postgres")]
	pub(crate) async fn try_get_process_complete_and_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::process::Id,
	) -> tg::Result<Option<(super::complete::Output, tg::process::Metadata)>> {
		// Get a database connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		#[derive(serde::Deserialize)]
		struct Row {
			pub children_complete: bool,
			pub children_count: Option<u64>,
			pub commands_complete: bool,
			pub commands_count: Option<u64>,
			pub commands_depth: Option<u64>,
			pub commands_weight: Option<u64>,
			pub outputs_complete: bool,
			pub outputs_count: Option<u64>,
			pub outputs_depth: Option<u64>,
			pub outputs_weight: Option<u64>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select
					children_complete,
					children_count,
					commands_complete,
					commands_count,
					commands_depth,
					commands_weight,
					outputs_complete,
					outputs_count,
					outputs_depth,
					outputs_weight
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output: Option<Row> = connection
			.query_optional_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		let output = output.map(|output| {
			let commands = tg::object::Metadata {
				count: output.commands_count,
				depth: output.commands_depth,
				weight: output.commands_weight,
			};
			let outputs = tg::object::Metadata {
				count: output.outputs_count,
				depth: output.outputs_depth,
				weight: output.outputs_weight,
			};
			let metadata = tg::process::Metadata {
				commands,
				count: output.children_count,
				outputs,
			};
			let complete = super::complete::Output {
				children: output.children_complete,
				commands: output.commands_complete,
				outputs: output.outputs_complete,
			};
			(complete, metadata)
		});

		Ok(output)
	}

	#[allow(dead_code)]
	pub(crate) async fn try_get_process_complete_and_metadata_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(super::complete::Output, tg::process::Metadata)>>> {
		match &self.index {
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_complete_and_metadata_batch_sqlite(database, ids)
					.await
			},
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_complete_and_metadata_batch_postgres(database, ids)
					.await
			},
		}
	}

	pub(crate) async fn try_get_process_complete_and_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(super::complete::Output, tg::process::Metadata)>>> {
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
	) -> tg::Result<Vec<Option<(super::complete::Output, tg::process::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let statement = indoc!(
			"
				select
					children_complete,
					children_count,
					commands_complete,
					commands_count,
					commands_depth,
					commands_weight,
					outputs_complete,
					outputs_count,
					outputs_depth,
					outputs_weight
				from processes
				where id = ?1;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut process_metadata = Vec::new();
		for id in ids {
			let params = sqlite::params![id.to_string()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				process_metadata.push(None);
				continue;
			};
			let children_complete = row.get_unwrap::<_, u64>(0) == 1;
			let children_count: Option<u64> = row.get_unwrap(1);
			let commands_complete = row.get_unwrap::<_, u64>(2) == 1;
			let commands_count: Option<u64> = row.get_unwrap(3);
			let commands_depth: Option<u64> = row.get_unwrap(4);
			let commands_weight: Option<u64> = row.get_unwrap(5);
			let outputs_complete = row.get_unwrap::<_, u64>(6) == 1;
			let outputs_count: Option<u64> = row.get_unwrap(7);
			let outputs_depth: Option<u64> = row.get_unwrap(8);
			let outputs_weight: Option<u64> = row.get_unwrap(9);
			let complete = super::complete::Output {
				children: children_complete,
				commands: commands_complete,
				outputs: outputs_complete,
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
				commands,
				count: children_count,
				outputs,
			};
			process_metadata.push(Some((complete, metadata)));
		}
		Ok(process_metadata)
	}

	#[cfg(feature = "postgres")]
	pub(crate) async fn try_get_process_complete_and_metadata_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<(super::complete::Output, tg::process::Metadata)>>> {
		use num::ToPrimitive;

		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let statement = indoc!(
			"
				select
					processes.id,
					children_complete,
					children_count,
					commands_count,
					commands_depth,
					commands_weight,
					commands_complete,
					outputs_complete,
					outputs_count,
					outputs_depth,
					outputs_weight
				from unnest($1::text[]) as ids (id)
				left join processes on processes.id = ids.id;
			",
		);
		let outputs = connection
			.inner()
			.query(
				statement,
				&[&ids.iter().map(ToString::to_string).collect::<Vec<_>>()],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to query the database"))?
			.into_iter()
			.map(|row| {
				row.get::<_, Option<String>>(0)?;
				let children_complete = row.get::<_, i64>(1) != 0;
				let children_count = row.get::<_, Option<i64>>(2).map(|v| v.to_u64().unwrap());
				let commands_complete = row.get::<_, i64>(3) != 0;
				let commands_count = row.get::<_, Option<i64>>(4).map(|v| v.to_u64().unwrap());
				let commands_depth = row.get::<_, Option<i64>>(5).map(|v| v.to_u64().unwrap());
				let commands_weight = row.get::<_, Option<i64>>(6).map(|v| v.to_u64().unwrap());
				let outputs_complete = row.get::<_, i64>(7) != 0;
				let outputs_count = row.get::<_, Option<i64>>(8).map(|v| v.to_u64().unwrap());
				let outputs_depth = row.get::<_, Option<i64>>(9).map(|v| v.to_u64().unwrap());
				let outputs_weight = row.get::<_, Option<i64>>(10).map(|v| v.to_u64().unwrap());
				let complete = super::complete::Output {
					children: children_complete,
					commands: commands_complete,
					outputs: outputs_complete,
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
					commands,
					count: children_count,
					outputs,
				};
				Some((complete, metadata))
			})
			.collect();
		Ok(outputs)
	}
}
