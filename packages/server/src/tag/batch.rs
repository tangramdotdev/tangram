use {
	crate::{Context, Database, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::Messenger,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub(crate) async fn post_tag_batch_with_context(
		&self,
		context: &Context,
		arg: tg::tag::post::Arg,
	) -> tg::Result<()> {
		// Forward to remote if requested.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
			let arg = tg::tag::post::Arg {
				local: None,
				remotes: None,
				tags: arg.tags,
			};
			client.post_tag_batch(arg).await.map_err(|source| {
				tg::error!(!source, "failed to post the tag batch on the remote")
			})?;
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
				Self::post_tag_batch_postgres(database, &arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to post the tag batch"))?;
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(database) => {
				Self::post_tag_batch_sqlite(database, &arg)
					.await
					.map_err(|source| tg::error!(!source, "failed to post the tag batch"))?;
			},
		}

		// Publish the put tag index messages.
		let messages: Vec<crate::index::Message> = arg
			.tags
			.into_iter()
			.map(|item| {
				crate::index::Message::PutTag(crate::index::message::PutTagMessage {
					tag: item.tag.to_string(),
					item: item.item,
				})
			})
			.collect();
		let message = crate::index::message::Messages(messages);
		self.messenger
			.stream_publish("index".into(), message)
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
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		self.post_tag_batch_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to post the tag batch"))?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
