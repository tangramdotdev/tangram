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
			Database::Sqlite(database) => {
				Self::delete_tag_sqlite(database, &arg.pattern, arg.recursive)
					.await
					.map_err(|source| tg::error!(!source, "failed to delete the tag"))?
			},
		};

		// Send delete tag index messages.
		for tag in &output.deleted {
			let message = crate::index::Message::DeleteTag(crate::index::message::DeleteTag {
				tag: tag.to_string(),
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
		}

		Ok(output)
	}

	pub(crate) async fn handle_delete_tag_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		_tag: &[&str],
	) -> tg::Result<http::Response<Body>> {
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		let output = self.delete_tag_with_context(context, arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
