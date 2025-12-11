use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tokio::io::AsyncWriteExt,
};

impl Server {
	pub(crate) async fn close_pipe_with_context(
		&self,
		context: &Context,
		id: &tg::pipe::Id,
		arg: tg::pipe::close::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self.get_remote_client(remote).await?;
			let arg = tg::pipe::close::Arg {
				local: None,
				remotes: None,
			};
			client.close_pipe(id, arg).await?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
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

	pub(crate) async fn handle_close_pipe_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		self.close_pipe_with_context(context, &id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
