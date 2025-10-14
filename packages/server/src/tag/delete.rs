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
	pub async fn delete_tag(
		&self,
		mut arg: tg::tag::delete::Arg,
	) -> tg::Result<tg::tag::delete::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let output = remote.delete_tag(arg).await?;
			return Ok(output);
		}

		// Delete the tag from the database.
		let output = match &self.database {
			#[cfg(feature = "postgres")]
			Database::Postgres(database) => Self::delete_tag_postgres(database, &arg.pattern).await?,
			Database::Sqlite(database) => Self::delete_tag_sqlite(database, &arg.pattern).await?,
		};

		// Send delete tag index messages.
		for tag in &output.deleted {
			let message = crate::index::Message::DeleteTag(crate::index::message::DeleteTag {
				tag: tag.to_string(),
			});
			let message = message.serialize()?;
			self.messenger
				.stream_publish("index".to_owned(), message)
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the message"))?
				.await
				.map_err(|source| tg::error!(!source, "failed to publish the message"))?;
		}

		Ok(output)
	}

	pub(crate) async fn handle_delete_tag_request<H>(
		handle: &H,
		request: http::Request<Body>,
		_tag: &[&str],
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.delete_tag(arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
