use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, params, Database, Query};
use tangram_http::{request::Ext as _, response::builder::Ext as _, Body};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn open_pipe(&self, arg: tg::pipe::open::Arg) -> tg::Result<tg::pipe::open::Output> {
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			return remote.open_pipe(tg::pipe::open::Arg::default()).await;
		}
		// Create the pipe data.
		let reader = tg::pipe::Id::new();
		let writer = tg::pipe::Id::new();

		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			r#"
				insert into pipes (
					reader,
					writer,
					reader_count,
					writer_count,
					touched_at,
					window_size
				)
				values (
					{p}1,
					{p}2,
					1,
					1,
					{p}3,
					{p}4
				);
			"#
		);
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		let params = params![
			reader.to_string(),
			writer.to_string(),
			now,
			arg.window_size.map(db::value::Json)
		];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let output = tg::pipe::open::Output { writer, reader };
		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_open_pipe_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.open_pipe(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
