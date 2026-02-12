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
	pub(crate) async fn put_tag_with_context(
		&self,
		context: &Context,
		tag: &tg::Tag,
		arg: tg::tag::put::Arg,
	) -> tg::Result<()> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, %tag, "failed to get the remote client"))?;
			let arg = tg::tag::put::Arg {
				force: arg.force,
				item: arg.item,
				local: None,
				remotes: None,
			};
			client
				.put_tag(tag, arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to put the tag on remote"))?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Authorize.
		self.authorize(context)
			.await
			.map_err(|source| tg::error!(!source, "failed to authorize"))?;

		// Insert the tag into the database.
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				Self::put_tag_postgres(database, tag, &arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to put the tag"))?;
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => {
				Self::put_tag_sqlite(database, tag, &arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to put the tag"))?;
			},
		}

		// Index the tag.
		self.index
			.put_tags(&[tangram_index::PutTagArg {
				tag: tag.to_string(),
				item: arg.item,
			}])
			.await
			.map_err(|source| tg::error!(!source, "failed to index the tag"))?;

		Ok(())
	}

	pub(crate) async fn handle_put_tag_request(
		&self,
		request: tangram_http::Request,
		context: &Context,
		tag: &[&str],
	) -> tg::Result<tangram_http::Response> {
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the tag.
		let tag = tag
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;

		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Put the tag.
		self.put_tag_with_context(context, &tag, arg).await?;

		// Create the response.
		match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR)) => (),
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		}

		let response = http::Response::builder()
			.body(tangram_http::body::Boxed::empty())
			.unwrap();
		Ok(response)
	}
}
