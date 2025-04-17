use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database as _, Query as _};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn delete_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> tg::Result<()> {
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			let arg = tg::pipe::delete::Arg::default();
			remote.delete_pipe(id, arg).await?;
			return Ok(());
		}

		// Send the end message and wait for acknowledgement.
		let sequence_number = self
			.write_pipe_event(id, tg::pipe::Event::End)
			.await?
			.sequence;

		// Poll the stream until the end message has been acknowledged by all consumers.
		let timeout = std::time::Duration::from_secs(10);
		let duration = std::time::Duration::from_millis(50);
		tokio::time::timeout(timeout, async {
			loop {
				let Ok(info) = self
					.messenger
					.stream_info(id.to_string())
					.await
					.inspect_err(|error| tracing::error!(?error, "failed to get stream info"))
				else {
					break;
				};
				if info.last_sequence >= sequence_number {
					break;
				}
				tokio::time::sleep(duration).await;
			}
		})
		.await
		.ok();

		// Delete the pipe from the database.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from pipes
				where id = {p}1;
			"
		);
		let params = db::params![id];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to delete the pipe"))?;
		drop(connection);

		// Delete the stream.
		let name = id.to_string();
		self.messenger
			.delete_stream(name)
			.await
			.map_err(|source| tg::error!(!source, "failed to delete the stream"))?;

		Ok(())
	}

	pub(crate) async fn handle_delete_pipe_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		handle.delete_pipe(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
