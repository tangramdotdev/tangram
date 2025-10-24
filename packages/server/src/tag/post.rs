use {
	crate::{Database, Server},
	itertools::Itertools as _,
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::Messenger,
};
mod sqlite;

#[cfg(feature = "postgres")]
mod postgres;

impl Server {
	pub(crate) async fn post_tags_batch(&self, mut arg: tg::tag::post::Arg) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.post_tags_batch(arg).await?;
			return Ok(());
		}

		// Insert the tag into the database.
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				Self::post_tags_batch_postgres(database, &arg).await?;
			},
			Database::Sqlite(database) => {
				Self::post_tags_batch_sqlite(database, &arg).await?;
			},
		}

		// Publish the put tag index messags.
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

	pub(crate) async fn handle_post_tags_batch_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		handle.post_tags_batch(arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
