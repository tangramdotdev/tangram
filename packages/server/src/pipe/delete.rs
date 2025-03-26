use crate::Server;
use bytes::Bytes;
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
			return remote
				.delete_pipe(id, tg::pipe::delete::Arg::default())
				.await;
		}

		// Send a notification that this pipe is going to be deleted.
		self.messenger
			.publish(format!("pipes.{id}.deleted"), Bytes::new())
			.await
			.map_err(|source| tg::error!(!source, "failed to send pipe delete notification"))?;

		// Remove the pipe from the database.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to acquire a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from pipes
				where id = {p};
			"
		);
		let params = db::params![id];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to delete the pipe"))?;
		drop(connection);

		// Delete the pipe.
		self.messenger
			.destroy_stream(id.to_string())
			.await
			.map_err(|source| tg::error!(!source, "failed to destroy the stream"))?;

		Ok(())
	}
}

impl Server {
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
