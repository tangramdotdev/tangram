use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn delete_pipe_with_context(
		&self,
		context: &Context,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> tg::Result<()> {
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self.get_remote_client(remote).await?;
			let arg = tg::pipe::delete::Arg {
				local: None,
				remotes: None,
			};
			client.delete_pipe(id, arg).await?;
			return Ok(());
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		self.pipes.remove(id);
		Ok(())
	}

	pub(crate) async fn handle_delete_pipe_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.json_or_default().await?;
		self.delete_pipe_with_context(context, &id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
