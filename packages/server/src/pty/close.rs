use {
	crate::Server,
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn close_pty(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::close::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.close_pty(id, arg).await?;
			return Ok(());
		}

		// Remove the PTY, which will drop the spawn_tx and session_task.
		let pty = self.ptys.remove(id);

		// If we had a PTY, abort the session task.
		if let Some((_, pty)) = pty {
			// Drop spawn_tx to signal session task to exit.
			drop(pty.spawn_tx);

			// Abort the session task.
			pty.session_task.abort();
		}

		Ok(())
	}

	pub(crate) async fn handle_close_pty_request<H>(
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

		handle.close_pty(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
