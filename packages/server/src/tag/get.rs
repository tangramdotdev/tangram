use {
	crate::{Context, Database, Server},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_database::prelude::*,
	tangram_http::{Body, request::Ext as _},
	tracing::Instrument as _,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	#[tracing::instrument(level = "trace", name = "get_tag", skip_all, fields(tag = %tag))]
	pub(crate) async fn try_get_tag_with_context(
		&self,
		context: &Context,
		tag: &tg::Tag,
		arg: tg::tag::get::Arg,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Try to get the tag locally if requested.
		if Self::local(arg.local, arg.remotes.as_ref()) {
			let output = self
				.try_get_tag_local(tag)
				.instrument(tracing::trace_span!("local"))
				.await
				.map_err(|source| tg::error!(!source, "failed to get the local tag"))?;
			if output.is_some() {
				return Ok(output);
			}
		}

		// Resolve the TTL for cache lookups.
		let ttl = arg.ttl.unwrap_or(self.config.tag_cache_ttl.as_secs());

		// Get the list of remotes to check.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.instrument(tracing::trace_span!("get_remotes"))
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;

		// Try to get a fresh cached result for each remote.
		for remote in &remotes {
			let output = self
				.try_get_cached_tag(tag, remote, ttl)
				.instrument(tracing::trace_span!("cached", %remote))
				.await
				.map_err(|source| tg::error!(!source, %remote, "failed to get the cached tag"))?;
			if output.is_some() {
				return Ok(output);
			}
		}

		// No fresh cache hit. Fetch from remotes via HTTP.
		let remote_outputs = remotes
			.into_iter()
			.map(|remote| {
				let tag_arg = tg::tag::get::Arg {
					local: None,
					remotes: None,
					ttl: None,
				};
				let span = tracing::trace_span!("remote", %remote);
				async move {
					let client = self.get_remote_client(remote.clone()).await.map_err(
						|source| tg::error!(!source, %remote, "failed to get the remote client"),
					)?;
					let output = client.try_get_tag(tag, tag_arg).await.map_err(
						|source| tg::error!(!source, %remote, "failed to get the remote tag"),
					)?;
					let output = output.map(|mut output| {
						output.remote = Some(remote);
						output
					});
					Ok::<_, tg::Error>(output)
				}
				.instrument(span)
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		// Return the first remote result that was found, and cache it.
		let output = remote_outputs.into_iter().flatten().next();
		if let Some(output) = &output
			&& let Some(remote) = &output.remote
		{
			self.cache_remote_tag(remote, tag, output)
				.await
				.map_err(|source| tg::error!(!source, "failed to cache the remote tag result"))?;
		}
		Ok(output)
	}

	async fn try_get_tag_local(&self, tag: &tg::Tag) -> tg::Result<Option<tg::tag::get::Output>> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.try_get_tag_postgres(database, tag).await,
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => self.try_get_tag_sqlite(database, tag).await,
		}
	}

	async fn try_get_cached_tag(
		&self,
		tag: &tg::Tag,
		remote: &str,
		ttl: u64,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				self.try_get_cached_tag_postgres(database, tag, remote, ttl)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => {
				let tag = tag.clone();
				let remote = remote.to_owned();
				let connection = database
					.connection()
					.await
					.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
				connection
					.with(move |connection, cache| {
						Self::try_get_cached_tag_sqlite_sync(connection, cache, &tag, &remote, ttl)
					})
					.await
			},
		}
	}

	async fn cache_remote_tag(
		&self,
		remote: &str,
		tag: &tg::Tag,
		output: &tg::tag::get::Output,
	) -> tg::Result<()> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				self.cache_remote_tag_postgres(database, remote, tag, output)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => {
				let tag = tag.clone();
				let remote = remote.to_owned();
				let output = output.clone();
				let connection = database
					.write_connection()
					.await
					.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
				connection
					.with(move |connection, cache| {
						Self::cache_remote_tag_sqlite_sync(
							connection, cache, &remote, &tag, &output,
						)
					})
					.await
			},
		}
	}

	pub(crate) async fn handle_get_tag_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		path: &[&str],
	) -> tg::Result<http::Response<Body>> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Parse the tag.
		let tag: tg::Tag = path
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;

		// Get the tag.
		let Some(output) = self.try_get_tag_with_context(context, &tag, arg).await? else {
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
