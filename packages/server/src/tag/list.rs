use {
	crate::{Context, Database, Server},
	futures::{TryStreamExt as _, stream::FuturesUnordered},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub(crate) async fn list_tags_with_context(
		&self,
		context: &Context,
		arg: tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let mut output = tg::tag::list::Output { data: Vec::new() };

		// List the local tags if requested.
		if Self::local(arg.local, arg.remotes.as_ref()) {
			let local_output = self
				.list_tags_local(arg.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to list local tags"))?;
			output.data.extend(local_output.data);
		}

		// List the remote tags if requested.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		let remote_outputs = remotes
			.into_iter()
			.map(|remote| {
				let arg = tg::tag::list::Arg {
					local: None,
					remotes: None,
					..arg.clone()
				};
				async move {
					let client = self.get_remote_client(remote.clone()).await.map_err(
						|source| tg::error!(!source, %remote, "failed to get the remote client"),
					)?;
					let mut output = client.list_tags(arg).await.map_err(
						|source| tg::error!(!source, %remote, "failed to list remote tags"),
					)?;
					for output in &mut output.data {
						output.remote = Some(remote.clone());
					}
					Ok::<_, tg::Error>(output)
				}
			})
			.collect::<FuturesUnordered<_>>()
			.try_collect::<Vec<_>>()
			.await?;

		output
			.data
			.extend(remote_outputs.into_iter().flat_map(|output| output.data));

		// Sort by tag, then by remote (local first for ties).
		if arg.reverse {
			output.data.sort_by(|a, b| match b.tag.cmp(&a.tag) {
				std::cmp::Ordering::Equal => a.remote.cmp(&b.remote),
				other => other,
			});
		} else {
			output.data.sort_by(|a, b| match a.tag.cmp(&b.tag) {
				std::cmp::Ordering::Equal => a.remote.cmp(&b.remote),
				other => other,
			});
		}

		Ok(output)
	}

	async fn list_tags_local(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => self.list_tags_postgres(database, arg).await,
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => self.list_tags_sqlite(database, arg).await,
		}
	}

	pub(crate) async fn handle_list_tags_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
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
