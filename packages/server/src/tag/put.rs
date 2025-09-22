use crate::Server;
use crate::database::Database;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::prelude::*;

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub async fn put_tag(&self, tag: &tg::Tag, mut arg: tg::tag::put::Arg) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let arg = tg::tag::put::Arg {
				remote: None,
				..arg.clone()
			};
			remote.put_tag(tag, arg).await?;
		}

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
		let message = message.serialize()?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	pub(crate) async fn handle_put_tag_request<H>(
		handle: &H,
		request: http::Request<Body>,
		tag: &[&str],
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let tag = tag
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		let arg = request.json().await?;
		handle.put_tag(&tag, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
