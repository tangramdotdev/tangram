use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database, Query, params};
use tangram_either::Either;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger as messenger;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn open_pipe(
		&self,
		mut arg: tg::pipe::open::Arg,
	) -> tg::Result<tg::pipe::open::Output> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			return remote.open_pipe(arg).await;
		}

		// Create the pipe data.
		let reader = tg::pipe::Id::new();
		let writer = tg::pipe::Id::new();

		eprintln!("insert {reader},{writer} {arg:#?}");

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

		match &self.messenger {
			Either::Left(messenger) => {
				self.create_pipe_in_memory(messenger, &reader, &writer)
					.await
					.map_err(|source| tg::error!(!source, "failed to create pipe"))?;
			},
			Either::Right(messenger) => {
				self.create_pipe_nats(messenger, &reader, &writer)
					.await
					.map_err(|source| tg::error!(!source, "failed to create pipe"))?;
			},
		}
		let output = tg::pipe::open::Output { writer, reader };
		Ok(output)
	}

	async fn create_pipe_in_memory(
		&self,
		messenger: &messenger::memory::Messenger,
		reader: &tg::pipe::Id,
		writer: &tg::pipe::Id,
	) -> tg::Result<()> {
		messenger
			.streams()
			.create_stream(reader.to_string())
			.await
			.map_err(|source| tg::error!(!source, "failed to create pipe"))?;
		messenger
			.streams()
			.create_stream(writer.to_string())
			.await
			.map_err(|source| tg::error!(!source, "failed to create pipe"))?;
		Ok(())
	}

	async fn create_pipe_nats(
		&self,
		messenger: &messenger::nats::Messenger,
		reader: &tg::pipe::Id,
		writer: &tg::pipe::Id,
	) -> tg::Result<()> {
		for pipe in [reader, writer] {
			let stream_name = pipe.to_string();
			let stream_config = async_nats::jetstream::stream::Config {
				name: stream_name.clone(),
				max_messages: 256,
				..Default::default()
			};
			messenger
				.jetstream
				.create_stream(stream_config)
				.await
				.map_err(|source| {
					tg::error!(!source, ?stream_name, "failed to get the pipe stream")
				})?;
		}
		Ok(())
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
