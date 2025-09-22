use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn touch_object(
		&self,
		id: &tg::object::Id,
		mut arg: tg::object::touch::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::object::touch::Arg { remote: None };
			remote.touch_object(id, arg).await?;
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

	pub(crate) async fn handle_touch_object_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.touch_object(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
