use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{Database, Query, params};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::{self as messenger, prelude::*};

impl Server {
	pub async fn create_pipe(
		&self,
		mut arg: tg::pipe::create::Arg,
	) -> tg::Result<tg::pipe::create::Output> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			return remote.create_pipe(arg).await;
		}

		// Create the pipe ID.
		let id = tg::pipe::Id::new();

		// Create the stream.
		let config = messenger::StreamConfig {
			discard: messenger::DiscardPolicy::New,
			max_bytes: Some(65_536),
			max_messages: Some(256),
			retention: messenger::RetentionPolicy::Limits,
		};
		self.messenger
			.get_or_create_stream(id.to_string(), config)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the stream"))?;

		// Insert the pipe into the database.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into pipes (id, created_at)
				values ({p}1, {p}2);
			"
		);
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		let params = params![id.to_string(), now];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		let output = tg::pipe::create::Output { id };

		Ok(output)
	}

	pub(crate) async fn handle_create_pipe_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.create_pipe(arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
