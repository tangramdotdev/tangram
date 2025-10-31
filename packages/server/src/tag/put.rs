use {
	crate::{Context, Server, database::Database, handle::ServerOrProxy, http::ContextExt},
	indoc::formatdoc,
	tangram_client::{self as tg, prelude::*},
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

		// Check the token.
		match (&context.token, self.config().serve.put_tag_requires_auth) {
			(Some(token), _) => {
				let user = self
					.get_user(token)
					.await
					.map_err(|source| tg::error!(!source, "failed to get user"))?
					.ok_or_else(|| tg::error!("not found"))?;
				if !self.is_user_authorized_to_put_tag(&user.id, tag).await {
					return Err(tg::error!("unauthorized"));
				}
			},
			(None, true) => return Err(tg::error!("unauthorized")),
			(_, false) => (),
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
		self.messenger
			.stream_publish("index".to_owned(), message)
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?
			.await
			.map_err(|source| tg::error!(!source, "failed to publish the message"))?;

		Ok(())
	}

	pub(crate) async fn is_user_authorized_to_put_tag(
		&self,
		token: &str,
		tag: &tg::Tag,
	) -> tg::Result<bool> {
		let mut connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a transaction"))?;

		// First, get the corresponding tag id(s) for a token.
		let p = transaction.p();
		let statement = formatdoc!("select id from tag_tokens where token = {p}1;");
		#[derive(serde::Deserialize)]
		struct Row {
			id: u64,
		}
		let rows = transaction
			.query_all_into::<db::row::Serde<Row>>(statement.into(), db::params![token])
			.await
			.map_err(|source| tg::error!(!source, %token, "failed to perform the query"))?;

		// Next, check if any of those tags are an ancestor of the desired tag.
		for row in rows {
			let statement = formatdoc!("select id from tags where parent = {p}1;");
		}

		todo!()
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
