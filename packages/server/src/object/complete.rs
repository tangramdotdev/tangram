use crate::{Server, database::Database};
use indoc::{formatdoc, indoc};
use rusqlite::{self as sqlite, fallible_streaming_iterator::FallibleStreamingIterator as _};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub(crate) async fn try_get_object_complete(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<bool>> {
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
				select complete
				from objects
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output = connection
			.query_optional_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_object_complete_batch(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
		match &self.index {
			Database::Sqlite(index) => self.try_get_object_complete_batch_sqlite(index, ids).await,
			#[cfg(feature = "postgres")]
			Database::Postgres(index) => {
				self.try_get_object_complete_batch_postgres(index, ids)
					.await
			},
		}
	}

	async fn try_get_object_complete_batch_sqlite(
		&self,
		index: &db::sqlite::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
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
				move |connection| Self::try_get_object_complete_batch_sqlite_sync(connection, &ids)
			})
			.await?;
		Ok(completes)
	}

	fn try_get_object_complete_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let statement = indoc!(
			"
				select complete
				from objects
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
			rows.advance()
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let Some(row) = rows.get() else {
				completes.push(None);
				continue;
			};
			let complete = row.get_unwrap(0);
			completes.push(Some(complete));
		}
		Ok(completes)
	}

	#[cfg(feature = "postgres")]
	async fn try_get_object_complete_batch_postgres(
		&self,
		database: &db::postgres::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
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
					ids.id,
					complete
				from unnest($1::text[]) as ids (id)
				left join objects on objects.id = ids.id;
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
				Some(complete)
			})
			.collect();
		Ok(outputs)
	}
}
