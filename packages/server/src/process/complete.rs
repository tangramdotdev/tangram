use crate::{Server, database::Database};
use indoc::{formatdoc, indoc};
use rusqlite as sqlite;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub complete: bool,
	pub commands_complete: bool,
	pub outputs_complete: bool,
}

impl Server {
	pub(crate) async fn try_get_process_complete(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		// Get an index connection.
		let connection = self
			.index
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object metadata.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select complete, commands_complete, outputs_complete
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output = connection
			.query_optional_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_process_complete_batch(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		match &self.index {
			Database::Sqlite(index) => self.try_get_process_complete_batch_sqlite(index, ids).await,
			#[cfg(feature = "postgres")]
			Database::Postgres(index) => {
				self.try_get_process_complete_batch_postgres(index, ids)
					.await
			},
		}
	}

	async fn try_get_process_complete_batch_sqlite(
		&self,
		index: &db::sqlite::Database,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = index
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let completes = connection
			.with({
				let ids = ids.to_owned();
				move |connection| Self::try_get_process_complete_batch_sqlite_sync(connection, &ids)
			})
			.await?;
		Ok(completes)
	}

	fn try_get_process_complete_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Output>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let statement = indoc!(
			"
				select
					complete,
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
			let complete = row.get_unwrap(0);
			let commands_complete = row.get_unwrap(1);
			let outputs_complete = row.get_unwrap(2);
			let complete = Output {
				complete,
				commands_complete,
				outputs_complete,
			};
			completes.push(Some(complete));
		}
		Ok(completes)
	}

	#[cfg(feature = "postgres")]
	async fn try_get_process_complete_batch_postgres(
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
					complete,
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
				let complete = row.get::<_, i64>(1) != 0;
				let commands_complete = row.get::<_, i64>(2) != 0;
				let outputs_complete = row.get::<_, i64>(3) != 0;
				let output = Output {
					complete,
					commands_complete,
					outputs_complete,
				};
				Some(output)
			})
			.collect();
		Ok(outputs)
	}
}
