use crate::Server;
use bytes::Bytes;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, Database as _, Query as _};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;

impl Server {
	pub async fn delete_pty(&self, id: &tg::pty::Id, arg: tg::pty::delete::Arg) -> tg::Result<()> {
		// Forward to a remote if requested.
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			return remote.delete_pty(id, tg::pty::delete::Arg::default()).await;
		}

		// Notify listeners that this pty is deleted.
		self.messenger
			.publish(format!("ptys.{id}.deleted"), Bytes::new())
			.await
			.ok();

		// Remove the pty from the database.
		let connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to acquire a database connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				delete from ptys
				where id = {p};
			"
		);
		let params = db::params![id];
		connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to delete the pipe"))?;
		drop(connection);

		for subject in ["master", "slave"] {
			self.messenger
				.destroy_stream(format!("{id}_{subject}"))
				.await
				.map_err(|source| tg::error!(!source, "failed to destroy stream"))?;
		}

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_delete_pty_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		handle.delete_pty(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
