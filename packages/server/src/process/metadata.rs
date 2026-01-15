use {
	crate::{Context, Server},
	futures::{FutureExt as _, future},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub async fn try_get_process_metadata_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::metadata::Arg,
	) -> tg::Result<Option<tg::process::Metadata>> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(metadata) = self
				.try_get_process_metadata_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the process metadata"))?
		{
			return Ok(Some(metadata));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(metadata) = self.try_get_process_metadata_remote(id, &remotes).await? {
			return Ok(Some(metadata));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_metadata_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Metadata>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_metadata_postgres(database, id).await
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_metadata_sqlite(database, id).await
			},
		}
	}

	pub(crate) async fn try_get_process_metadata_batch_local(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<tg::process::Metadata>>> {
		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.try_get_process_metadata_batch_postgres(database, ids)
					.await
			},
			#[cfg(feature = "sqlite")]
			crate::index::Index::Sqlite(database) => {
				self.try_get_process_metadata_batch_sqlite(database, ids)
					.await
			},
		}
	}

	async fn try_get_process_metadata_remote(
		&self,
		id: &tg::process::Id,
		remotes: &[String],
	) -> tg::Result<Option<tg::process::Metadata>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				client.get_process_metadata(id).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the process metadata"),
				)
			}
			.boxed()
		});
		let Ok((metadata, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(Some(metadata))
	}

	pub(crate) async fn handle_get_process_metadata_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the process id.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the process metadata.
		let Some(output) = self
			.try_get_process_metadata_with_context(context, &id, arg)
			.await?
		else {
			return Ok(http::Response::builder()
				.status(http::StatusCode::NOT_FOUND)
				.body(Body::empty())
				.unwrap());
		};

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), Body::with_bytes(body))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};

		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();
		Ok(response)
	}
}
