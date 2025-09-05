use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn touch_process(
		&self,
		id: &tg::process::Id,
		mut arg: tg::process::touch::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			let arg = tg::process::touch::Arg { remote: None };
			remote.touch_process(id, arg).await?;
			return Ok(());
		}

		match &self.index {
			#[cfg(feature = "postgres")]
			crate::index::Index::Postgres(database) => {
				self.touch_process_postgres(database, id).await?;
			},
			crate::index::Index::Sqlite(database) => {
				self.touch_process_sqlite(database, id).await?;
			},
		}

		Ok(())
	}

	async fn touch_process_sqlite(
		&self,
		database: &db::sqlite::Database,
		id: &tg::process::Id,
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
				update processes
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
			return Err(tg::error!("failed to find the process"));
		}

		// Drop the connection.
		drop(connection);

		Ok(())
	}

	#[cfg(feature = "postgres")]
	async fn touch_process_postgres(
		&self,
		database: &db::postgres::Database,
		id: &tg::process::Id,
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
				update processes
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
			return Err(tg::error!("failed to find the process"));
		}

		// Drop the connection.
		drop(connection);

		Ok(())
	}

	pub(crate) async fn handle_touch_process_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.touch_process(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
