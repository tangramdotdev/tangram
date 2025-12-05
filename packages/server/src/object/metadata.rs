#[cfg(feature = "postgres")]
use indoc::formatdoc;
use {
	crate::{Context, Server},
	futures::{FutureExt as _, future},
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn try_get_object_metadata_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		mut arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let output = remote.try_get_object_metadata(id, arg).await?;
			return Ok(output);
		}

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
		#[derive(db::row::Deserialize)]
		struct Row {
			count: Option<u64>,
			depth: Option<u64>,
			weight: Option<u64>,
		}
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
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|row| tg::object::Metadata {
				count: row.count,
				depth: row.depth,
				weight: row.weight,
			});

		// Drop the connection.
		drop(connection);

		Ok(output)
	}

	async fn try_get_object_metadata_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a connection"))?;
		let output = connection
			.with({
				let id = id.to_owned();
				move |connection| Self::try_get_object_metadata_sqlite_sync(connection, &id)
			})
			.await?;
		Ok(output)
	}

	pub(crate) fn try_get_object_metadata_sqlite_sync(
		connection: &sqlite::Connection,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			count: Option<u64>,
			depth: Option<u64>,
			weight: Option<u64>,
		}
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
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
		let metadata = tg::object::Metadata {
			count: row.count,
			depth: row.depth,
			weight: row.weight,
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

	pub(crate) async fn handle_get_object_metadata_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		let Some(output) = self
			.try_get_object_metadata_with_context(context, &id, arg)
			.await?
		else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
