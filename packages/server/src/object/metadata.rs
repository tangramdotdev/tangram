use crate::Server;
use futures::{future, FutureExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

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

	async fn try_get_object_metadata_local(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		// Try to get the object metadata from the database.
		if let Some(output) = self.try_get_object_metadata_local_database(id).await? {
			return Ok(Some(output));
		};

		// If the object is an artifact, then try to store it.
		if let Ok(artifact) = tg::artifact::Id::try_from(id.clone()) {
			let stored = self
				.artifact_store_task_map
				.get_or_spawn(artifact.clone(), |_| {
					self.try_store_artifact_future(&artifact)
				})
				.wait()
				.await
				.map_err(|source| tg::error!(!source, "failed to wait for the task"))??;
			if stored {
				let output = self
					.try_get_object_metadata_local_database(id)
					.await?
					.ok_or_else(|| tg::error!("expected the object to exist"))?;
				return Ok(Some(output));
			}
		}

		// If the object is a blob, then try to store it.
		if let Ok(blob) = tg::blob::Id::try_from(id.clone()) {
			let server = self.clone();
			let stored = self
				.blob_store_task_map
				.get_or_spawn(blob.clone(), |_| async move {
					server.try_store_blob(&blob).await
				})
				.wait()
				.await
				.map_err(|source| tg::error!(!source, "failed to wait for the task"))??;
			if stored {
				let output = self
					.try_get_object_metadata_local_database(id)
					.await?
					.ok_or_else(|| tg::error!("expected the object to exist"))?;
				return Ok(Some(output));
			}
		}

		Ok(None)
	}

	async fn try_get_object_metadata_local_database(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<tg::object::Metadata>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object metadata.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select complete, count, depth, weight
				from objects
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output = connection
			.query_optional_into(statement, params)
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
			.remotes
			.iter()
			.map(|remote| async move { remote.get_object_metadata(id).await }.boxed())
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
	pub(crate) async fn handle_head_object_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let Some(metadata) = handle.try_get_object_metadata(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let mut response = http::Response::builder();
		response = response
			.header_json(tg::object::metadata::HEADER, metadata)
			.unwrap();
		let response = response.empty().unwrap();
		Ok(response)
	}
}
