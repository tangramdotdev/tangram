use {
	crate::Server,
	tangram_client as tg,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tokio::io::AsyncWriteExt,
};

impl Server {
	pub async fn close_pipe(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::close::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.close_pipe(id, arg).await?;
			return Ok(());
		}

		let mut pipe = self
			.pipes
			.get_mut(id)
			.ok_or_else(|| tg::error!("failed to find the pipe"))?;
		let mut sender = pipe
			.sender
			.take()
			.ok_or_else(|| tg::error!("the pipe is already closed"))?;
		sender
			.flush()
			.await
			.map_err(|source| tg::error!(!source, "failed to flush the pipe"))?;

		Ok(())
	}

	pub(crate) async fn handle_close_pipe_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		handle.close_pipe(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
