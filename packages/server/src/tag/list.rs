use {
	crate::{Context, Database, Server},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	time::OffsetDateTime,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Serialize)]
pub(crate) struct RemoteTagListTaskKey {
	pub remote: String,
	pub arg: tg::tag::list::Arg,
}

impl Server {
	#[tracing::instrument(level = "trace", name = "list_tags", skip_all, fields(pattern = %arg.pattern))]
	pub(crate) async fn list_tags_with_context(
		&self,
		context: &Context,
		arg: tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let mut data = Vec::new();

		// Collect local results.
		if Self::local(arg.local, arg.remotes.as_ref()) {
			let local_arg = tg::tag::list::Arg {
				length: None,
				..arg.clone()
			};
			let local_output = self
				.list_tags_local(local_arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to list local tags"))?;
			data.extend(local_output.data);
		}

		// Collect remote results concurrently.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		let remote_results = remotes
			.into_iter()
			.map(|remote| {
				let arg = arg.clone();
				async move { self.list_tags_remote(&remote, &arg).await }
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;
		data.extend(remote_results.into_iter().flatten());

		// Sort by tag, then by remote, preferring local over remote.
		data.sort_by(|a, b| {
			let tag_cmp = if arg.reverse {
				b.tag.cmp(&a.tag)
			} else {
				a.tag.cmp(&b.tag)
			};
			tag_cmp.then_with(|| a.remote.cmp(&b.remote))
		});

		// Truncate to the requested length.
		if let Some(length) = arg.length {
			data.truncate(length.to_usize().unwrap());
		}

		Ok(tg::tag::list::Output { data })
	}

	async fn list_tags_remote(
		&self,
		remote: &str,
		arg: &tg::tag::list::Arg,
	) -> tg::Result<Vec<tg::tag::list::Entry>> {
		// Build the task key by clearing fields that do not affect the remote query.
		let key = RemoteTagListTaskKey {
			remote: remote.to_owned(),
			arg: Self::list_tags_remote_arg(arg),
		};
		let key_json = serde_json::to_string(&key).unwrap();

		// Check the cache unless ttl is Some(0).
		if arg.ttl != Some(0)
			&& let Some((cached_output, timestamp)) = self
				.list_tags_cache_get(&key_json)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the tag list cache"))?
		{
			let now = OffsetDateTime::now_utc().unix_timestamp();
			let ttl = arg.ttl.map_or(i64::MAX, u64::cast_signed);
			if now - timestamp < ttl {
				let mut entries: Vec<tg::tag::list::Entry> = serde_json::from_str(&cached_output)
					.map_err(|source| {
					tg::error!(!source, "failed to deserialize the cached tag list")
				})?;
				for entry in &mut entries {
					entry.remote = Some(remote.to_owned());
				}
				return Ok(entries);
			}
		}

		// If cached mode is enabled, do not fetch from the remote.
		if arg.cached {
			return Ok(Vec::new());
		}

		// Fetch from the remote with deduplication.
		let task = self
			.remote_list_tags_tasks
			.get_or_spawn_detached(key.clone(), {
				let server = self.clone();
				move |_stop| async move { server.list_tags_remote_task(key).await }
			});
		let entries = task
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "the remote tag list task panicked"))??;

		// Set the remote field on each entry.
		let entries = entries
			.into_iter()
			.map(|mut entry| {
				entry.remote = Some(remote.to_owned());
				entry
			})
			.collect();

		Ok(entries)
	}

	fn list_tags_remote_arg(arg: &tg::tag::list::Arg) -> tg::tag::list::Arg {
		tg::tag::list::Arg {
			cached: false,
			length: None,
			local: None,
			remotes: None,
			reverse: false,
			ttl: None,
			..arg.clone()
		}
	}

	async fn list_tags_remote_task(
		&self,
		key: RemoteTagListTaskKey,
	) -> tg::Result<Vec<tg::tag::list::Entry>> {
		let RemoteTagListTaskKey { arg, remote } = key;

		// Fetch from the remote.
		let client = self
			.get_remote_client(remote.clone())
			.await
			.map_err(|source| tg::error!(!source, %remote, "failed to get the remote client"))?;
		let output = client
			.list_tags(arg.clone())
			.await
			.map_err(|source| tg::error!(!source, %remote, "failed to list tags"))?;

		// Upsert the result into the cache.
		let key = serde_json::to_string(&RemoteTagListTaskKey { remote, arg }).unwrap();
		let output_json = serde_json::to_string(&output.data).unwrap();
		let now = OffsetDateTime::now_utc().unix_timestamp();
		self.list_tags_cache_put(&key, &output_json, now)
			.await
			.map_err(|source| tg::error!(!source, "failed to put the tag list cache"))?;

		Ok(output.data)
	}

	async fn list_tags_local(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.list_tags_postgres(database, arg).await,
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => self.list_tags_sqlite(database, arg).await,
		}
	}

	async fn list_tags_cache_get(&self, arg: &str) -> tg::Result<Option<(String, i64)>> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.list_tags_cache_get_postgres(database, arg).await,
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => self.list_tags_cache_get_sqlite(database, arg).await,
		}
	}

	async fn list_tags_cache_put(&self, arg: &str, output: &str, timestamp: i64) -> tg::Result<()> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				self.list_tags_cache_put_postgres(database, arg, output, timestamp)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => {
				self.list_tags_cache_put_sqlite(database, arg, output, timestamp)
					.await
			},
		}
	}

	pub(crate) async fn handle_list_tags_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
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

		// List the tags.
		let output = self.list_tags_with_context(context, arg).await?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(Some(content_type), BoxBody::with_bytes(body))
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
