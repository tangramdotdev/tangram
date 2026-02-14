use {
	crate::{Context, Database, Server},
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

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

		// List the local tags.
		let mut output = self
			.list_tags_local(arg.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to list local tags"))?;

		// Sort by tag.
		if arg.reverse {
			output.data.sort_by(|a, b| b.tag.cmp(&a.tag));
		} else {
			output.data.sort_by(|a, b| a.tag.cmp(&b.tag));
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
