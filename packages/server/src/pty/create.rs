use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database, Query, params};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn create_pty(
		&self,
		mut arg: tg::pty::create::Arg,
	) -> tg::Result<tg::pty::create::Output> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			return remote.create_pty(arg).await;
		}

		// Create the pty data.
		let id = tg::pty::Id::new();

		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into ptys (id, created_at, window_size)
				values ({p}1, {p}2, {p}3);
			"
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = params![id.to_string(), now, db::value::Json(arg.window_size),];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		for subject in ["master", "slave"] {
			self.messenger
				.create_stream(format!("{id}_{subject}"))
				.await
				.map_err(|source| tg::error!(!source, "failed to create the stream"))?;
		}

		let output = tg::pty::create::Output { id };
		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_create_pty_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.create_pty(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
