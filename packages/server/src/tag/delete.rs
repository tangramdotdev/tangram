use {
	crate::{Server, database::Database},
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub async fn delete_tag(&self, tag: &tg::Tag, mut arg: tg::tag::delete::Arg) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.delete_tag(tag, arg).await?;
			return Ok(());
		}

		// Delete the tag from the database.
		match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => {
				Self::delete_tag_postgres(database, tag).await?;
			},
			Database::Sqlite(database) => {
				Self::delete_tag_sqlite(database, tag).await?;
			},
		}

		// Send the delete tag index message.
		let message = crate::index::Message::DeleteTag(crate::index::message::DeleteTag {
			tag: tag.to_string(),
		});
		let message = message.serialize()?;
		let _published = self
			.messenger
			.stream_publish("index".to_owned(), message)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	pub(crate) async fn handle_delete_tag_request<H>(
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
		handle.delete_tag(&tag, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
