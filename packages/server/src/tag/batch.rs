use {
	crate::{Context, Database, Server},
	itertools::Itertools as _,
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::Messenger,
};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub(crate) async fn post_tag_batch_with_context(
		&self,
		context: &Context,
		mut arg: tg::tag::post::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.post_tag_batch(arg).await?;
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
				Self::post_tag_batch_postgres(database, &arg).await?;
			},
			Database::Sqlite(database) => {
				Self::post_tag_batch_sqlite(database, &arg).await?;
			},
		}

		// Publish the put tag index messages.
		let messages = arg
			.tags
			.into_iter()
			.map(|item| {
				crate::index::Message::PutTag(crate::index::message::PutTagMessage {
					tag: item.tag.to_string(),
					item: item.item,
				})
				.serialize()
			})
			.try_collect()?;
		self.messenger
			.stream_batch_publish("index".into(), messages)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?
			.await
			.map_err(|source| tg::error!(!source, "failed to ack the message"))?;
		Ok(())
	}

	pub(crate) async fn handle_post_tag_batch_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.json().await?;
		self.post_tag_batch_with_context(context, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
