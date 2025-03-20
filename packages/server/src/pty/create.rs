use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database, Query, params};
use tangram_either::Either;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger as messenger;
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

		match &self.messenger {
			Either::Left(messenger) => {
				self.create_pty_in_memory(messenger, &id)
					.await
					.map_err(|source| tg::error!(!source, "failed to create pty"))?;
			},
			Either::Right(messenger) => {
				self.create_pty_nats(messenger, &id)
					.await
					.map_err(|source| tg::error!(!source, "failed to create pty"))?;
			},
		}
		let output = tg::pty::create::Output { id };
		Ok(output)
	}

	async fn create_pty_in_memory(
		&self,
		messenger: &messenger::memory::Messenger,
		id: &tg::pty::Id,
	) -> tg::Result<()> {
		let subject = format!("{id}.master");
		messenger
			.streams()
			.create_stream(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to create pty"))?;
		let subject = format!("{id}.slave");
		messenger
			.streams()
			.create_stream(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to create pty"))?;
		Ok(())
	}

	async fn create_pty_nats(
		&self,
		messenger: &messenger::nats::Messenger,
		id: &tg::pty::Id,
	) -> tg::Result<()> {
		let stream_name = format!("{id}_master");
		let stream_config = async_nats::jetstream::stream::Config {
			name: stream_name.clone(),
			max_messages: 256,
			..Default::default()
		};
		messenger
			.jetstream
			.create_stream(stream_config)
			.await
			.map_err(|source| tg::error!(!source, ?stream_name, "failed to get the pty stream"))?;
		let stream_name = format!("{id}_slave");
		let stream_config = async_nats::jetstream::stream::Config {
			name: stream_name.clone(),
			max_messages: 256,
			..Default::default()
		};
		messenger
			.jetstream
			.create_stream(stream_config)
			.await
			.map_err(|source| tg::error!(!source, ?stream_name, "failed to get the pty stream"))?;

		Ok(())
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
