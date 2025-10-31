use {
	crate::{Context, Server, database::Database, handle::ServerOrProxy, http::ContextExt},
	indoc::formatdoc,
	tangram_client as tg,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

#[cfg(feature = "postgres")]
mod postgres;
mod sqlite;

impl Server {
	pub(crate) async fn put_tag(
		&self,
		context: Context,
		tag: &tg::Tag,
		mut arg: tg::tag::put::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.put_tag(tag, arg).await?;
			return Ok(());
		}
		self.ensure_put_tag_authorized(&context, tag);

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
		self.messenger
			.stream_publish("index".to_owned(), message)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	pub(crate) async fn ensure_put_tag_authorized(
		&self,
		context: &Context,
		tag: &tg::Tag,
	) -> tg::Result<()> {
		// Check the token.
		let user = match (&context.token, self.config().serve.put_tag_requires_auth) {
			(Some(token), _) => self
				.get_user(token)
				.await
				.map_err(|source| tg::error!(!source, "failed to get user"))?
				.ok_or_else(|| tg::error!("not found"))?,
			(None, true) => return Err(tg::error!("unauthorized")),
			(_, false) => return Ok(()),
		};

		// If the tag has more than one component, get its parent.
		let mut components = tag.components().collect::<Vec<_>>();
		if components.len() > 1 {
			components.pop();
		}
		let tag = components.join("/");

		// Get a transaction.
		let mut connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a transaction"))?;

		// Check if the user is allowed to access the parent.
		let p = transaction.p();
		let statement =
			formatdoc!("select count(*) from tag_users where tag = {p}1 and user = {p}2;");
		let count = transaction
			.query_one_into::<db::row::Serde<u64>>(statement.into(), db::params![tag, user.id])
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?
			.0;

		if count == 0 {
			return Err(tg::error!(%tag, %user = user.id, "unauthorized"));
		}

		Ok(())
	}

	pub(crate) async fn handle_put_tag_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
		tag: &[&str],
	) -> tg::Result<http::Response<Body>> {
		let tag = tag
			.join("/")
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the tag"))?;
		let context = request.context();
		let arg = request.json().await?;
		handle.put_tag(context, &tag, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
