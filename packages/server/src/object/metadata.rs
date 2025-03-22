use crate::Server;
use futures::{FutureExt as _, future};
use indoc::formatdoc;
use itertools::Itertools as _;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, response::builder::Ext as _};

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
				select count, depth, weight
				from objects
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

	async fn try_get_object_metadata_remote(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		// Attempt to get the object metadata from the remotes.
		let futures = self
			.get_remote_clients()
			.await?
			.into_values()
			.map(|client| async move { client.get_object_metadata(id).await }.boxed())
			.collect_vec();
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((metadata, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};

		Ok(Some(metadata))
	}
}

impl Server {
	pub(crate) async fn handle_get_object_metadata_request<H>(
		handle: &H,
		_request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(metadata) = handle.try_get_object_metadata(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(metadata).unwrap();
		Ok(response)
	}
}
