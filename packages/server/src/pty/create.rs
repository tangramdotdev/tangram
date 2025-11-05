use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn create_pty_with_context(
		&self,
		context: &Context,
		mut arg: tg::pty::create::Arg,
	) -> tg::Result<tg::pty::create::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			return remote.create_pty(arg).await;
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Create the pty.
		let id = tg::pty::Id::new();
		let pty = super::Pty::new(self, arg.size).await?;
		self.ptys.insert(id.clone(), pty);

		// Create the output.
		let output = tg::pty::create::Output { id };

		Ok(output)
	}

	pub(crate) async fn handle_create_pty_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.json().await?;
		let output = self.create_pty_with_context(context, arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
