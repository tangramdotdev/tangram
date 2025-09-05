use crate::Server;
use indoc::{formatdoc, indoc};
use rusqlite as sqlite;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub(crate) async fn try_get_object_complete_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<bool>> {
		// Get an index connection.
		let connection = database
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
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) async fn try_get_object_complete_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<bool>>> {
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
				move |connection| Self::try_get_object_complete_batch_sqlite_sync(connection, &ids)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_complete_batch_sqlite_sync(
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
		let mut output = Vec::new();
		for id in ids {
			let params = sqlite::params![id.to_bytes().to_vec()];
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
			let complete = row.get_unwrap(0);
			output.push(Some(complete));
		}
		Ok(output)
	}

	pub(crate) async fn try_get_object_complete_and_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<(bool, tg::object::Metadata)>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| {
					Self::try_get_object_complete_and_metadata_sqlite_sync(connection, &id)
				}
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_complete_and_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::object::Id,
	) -> tg::Result<Option<(bool, tg::object::Metadata)>> {
		let statement = indoc!(
			"
				select complete, count, depth, weight
				from objects
				where id = ?1;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let id = id.to_bytes().to_vec();
		let mut rows = statement
			.query([&id])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let complete = row.get_unwrap::<_, u64>(0) != 0;
		let count: Option<u64> = row.get_unwrap(1);
		let depth: Option<u64> = row.get_unwrap(2);
		let weight: Option<u64> = row.get_unwrap(3);
		let metadata = tg::object::Metadata {
			count,
			depth,
			weight,
		};
		Ok(Some((complete, metadata)))
	}

	pub(crate) async fn try_get_object_complete_and_metadata_batch_sqlite(
		&self,
		database: &db::sqlite::Database,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(bool, tg::object::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let metadata = connection
			.with({
				let ids = ids.to_owned();
				move |connection| {
					Self::try_get_object_complete_and_metadata_batch_sqlite_sync(connection, &ids)
				}
			})
			.await?;
		Ok(metadata)
	}

	pub(crate) fn try_get_object_complete_and_metadata_batch_sqlite_sync(
		connection: &sqlite::Connection,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<(bool, tg::object::Metadata)>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let statement = indoc!(
			"
				select
					complete,
					count,
					depth,
					weight
				from objects
				where id = ?1;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut object_metadata = Vec::new();
		for id in ids {
			let params = sqlite::params![id.to_bytes().to_vec()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				object_metadata.push(None);
				continue;
			};
			let complete = row.get_unwrap::<_, u64>(0) != 0;
			let count: Option<u64> = row.get_unwrap(1);
			let depth: Option<u64> = row.get_unwrap(2);
			let weight: Option<u64> = row.get_unwrap(3);
			let metadata = tg::object::Metadata {
				count,
				depth,
				weight,
			};
			object_metadata.push(Some((complete, metadata)));
		}
		Ok(object_metadata)
	}
}
