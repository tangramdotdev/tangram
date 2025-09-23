use {
	crate::Server,
	futures::{FutureExt as _, future},
	indoc::{formatdoc, indoc},
	rusqlite as sqlite,
	tangram_client::{self as tg, prelude::*},
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, response::builder::Ext as _},
};

impl Server {
	pub async fn try_get_object_metadata(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		if let Some(metadata) = self.try_get_object_metadata_local(id).await? {
			Ok(Some(metadata))
		} else if let Some(metadata) = self.try_get_object_metadata_remote(id).await? {
			Ok(Some(metadata))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn try_get_object_metadata_local(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_object_metadata_postgres(database, id).await
			},
			crate::index::Index::Sqlite(database) => {
				self.try_get_object_metadata_sqlite(database, id).await
			},
		}
	}

	#[cfg(feature = "postgres")]
	async fn try_get_object_metadata_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the object metadata.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count, depth, weight
				from objects
				where id = {p}1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<db::row::Serde<tg::object::Metadata>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0);

		// Drop the connection.
		drop(connection);

		Ok(output)
	}

	async fn try_get_object_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		// Get an index connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;

		// Get the object metadata.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count, depth, weight
				from objects
				where id = {p}1;
			",
		);
		let params = db::params![id.to_bytes()];
		let output = connection
			.query_optional_into::<db::row::Serde<tg::object::Metadata>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| row.0);

		// Drop the connection.
		drop(connection);

		Ok(output)
	}

	pub(crate) fn try_get_object_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let statement = indoc!(
			"
				select count, depth, weight
				from objects
				where id = ?1;
			"
		);
		let mut statement = connection
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let mut rows = statement
			.query([id.to_bytes().to_vec()])
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		let count = row
			.get::<_, Option<u64>>(0)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let depth = row
			.get::<_, Option<u64>>(1)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let weight = row
			.get::<_, Option<u64>>(2)
			.map_err(|source| tg::error!(!source, "expected an integer"))?;
		let metadata = tg::object::Metadata {
			count,
			depth,
			weight,
		};
		Ok(Some(metadata))
	}

	async fn try_get_object_metadata_remote(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let futures = self
			.get_remote_clients()
			.await?
			.into_values()
			.map(|client| async move { client.get_object_metadata(id).await }.boxed())
			.collect::<Vec<_>>();
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((metadata, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(Some(metadata))
	}

	pub(crate) async fn handle_get_object_metadata_request<H>(
		handle: &H,
		_request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(output) = handle.try_get_object_metadata(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
