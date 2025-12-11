use {
	crate::{Context, Server},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn touch_object_with_context(
		&self,
		_context: &Context,
		id: &tg::object::Id,
		arg: tg::object::touch::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self.get_remote_client(remote).await?;
			let arg = tg::object::touch::Arg {
				local: None,
				remotes: None,
			};
			client.touch_object(id, arg).await?;
			return Ok(());
		}

		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.touch_object_postgres(database, id).await?;
			},
			crate::index::Index::Sqlite(database) => {
				self.touch_object_sqlite(database, id).await?;
			},
		}

		Ok(())
	}

	#[cfg(feature = "postgres")]
	async fn touch_object_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Get a connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let p = connection.p();
		let statement = formatdoc!(
			"
				update objects
				set touched_at = greatest(touched_at, {p}1)
				where id = {p}2;
			",
		);
		let params = db::params![touched_at, id.to_bytes()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n == 0 {
			return Err(tg::error!("failed to find the object"));
		}

		// Drop the connection.
		drop(connection);

		Ok(())
	}

	async fn touch_object_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::object::Id,
	) -> tg::Result<()> {
		let touched_at = time::OffsetDateTime::now_utc().unix_timestamp();

		// Get a connection.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let p = connection.p();
		let statement = formatdoc!(
			"
				update objects
				set touched_at = max(touched_at, {p}1)
				where id = {p}2;
			",
		);
		let params = db::params![touched_at, id.to_bytes()];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		if n == 0 {
			return Err(tg::error!("failed to find the object"));
		}

		// Drop the connection.
		drop(connection);

		Ok(())
	}

	pub(crate) async fn handle_touch_object_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.json().await?;
		self.touch_object_with_context(context, &id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
