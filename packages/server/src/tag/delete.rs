use {
	crate::{Context, Server, database::Database},
	tangram_client::prelude::*,
	tangram_http::request::Ext as _,
	tangram_index::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub(crate) async fn delete_tag_with_context(
		&self,
		context: &Context,
		arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
			let arg = tg::tag::delete::Arg {
				local: None,
				pattern: arg.pattern,
				recursive: arg.recursive,
				remotes: None,
			};
			let output = client
				.delete_tag(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to delete the tag on remote"))?;
			return Ok(output);
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Delete the tag from the database.
		let output = match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => Self::delete_tag_postgres(database, &arg.pattern, arg.recursive)
				.await
				.map_err(|source| tg::error!(!source, "failed to delete the tag"))?,
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => Self::delete_tag_sqlite(database, &arg.pattern, arg.recursive)
				.await
				.map_err(|source| tg::error!(!source, "failed to delete the tag"))?,
		};

		// Index the deleted tags.
		let tags = output
			.deleted
			.iter()
			.map(ToString::to_string)
			.collect::<Vec<_>>();
		if !tags.is_empty() {
			self.index
				.delete_tags(&tags)
				.await
				.map_err(|source| tg::error!(!source, "failed to index the deleted tags"))?;
		}

		Ok(output)
	}

	pub(crate) async fn handle_delete_tag_request(
		&self,
		request: tangram_http::Request,
		context: &Context,
		_tag: &[&str],
	) -> tg::Result<tangram_http::Response> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Delete the tag.
		let output = self.delete_tag_with_context(context, arg).await?;

		// Create the response.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let body = serde_json::to_vec(&output).unwrap();
				(
					Some(content_type),
					tangram_http::body::Boxed::with_bytes(body),
				)
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
