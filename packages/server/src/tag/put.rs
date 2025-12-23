use {
	crate::{Context, Server, database::Database},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
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
			let client = self.get_remote_client(remote).await?;
			let arg = tg::tag::put::Arg {
				force: arg.force,
				item: arg.item,
				local: None,
				remotes: None,
			};
			client.put_tag(tag, arg).await?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Authorize.
		self.authorize(context).await?;

		// Insert the tag into the database.
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				Self::put_tag_postgres(database, tag, &arg).await?;
			},
			Database::Sqlite(database) => {
				Self::put_tag_sqlite(database, tag, &arg).await?;
			},
		}

		// Publish the put tag index message.
		let message = crate::index::Message::PutTag(crate::index::message::PutTagMessage {
			tag: tag.to_string(),
			item: arg.item,
		});
		self.messenger
			.stream_publish(
				"index".to_owned(),
				crate::index::message::Messages(vec![message]),
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	pub(crate) async fn handle_put_tag_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		tag: &[&str],
	) -> tg::Result<http::Response<Body>> {
		let tag = tag
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		let arg = request.json().await?;
		self.put_tag_with_context(context, &tag, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
