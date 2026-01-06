use {
	crate::{Context, Server},
	futures::{FutureExt as _, future},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub async fn try_get_object_metadata_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		arg: tg::object::metadata::Arg,
	) -> tg::Result<Option<tg::object::Metadata>> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(metadata) = self
				.try_get_object_metadata_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the object metadata"))?
		{
			return Ok(Some(metadata));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(metadata) = self.try_get_object_metadata_remote(id, &remotes).await? {
			return Ok(Some(metadata));
		}

		Ok(None)
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
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => self.try_get_object_metadata_sqlite(database, id).await,
		}
	}

	pub(crate) async fn try_get_object_metadata_batch_local(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tg::object::Metadata>>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_object_metadata_batch_postgres(database, ids)
					.await
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => {
				self.try_get_object_metadata_batch_sqlite(database, ids)
					.await
			},
		}
	}

	async fn try_get_object_metadata_remote(
		&self,
		id: &tg::object::Id,
		remotes: &[String],
	) -> tg::Result<Option<tg::object::Metadata>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				client.get_object_metadata(id).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the object metadata"),
				)
			}
			.boxed()
		});
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
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the object id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();
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
