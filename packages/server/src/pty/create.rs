use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn create_pty_with_context(
		&self,
		context: &Context,
		arg: tg::pty::create::Arg,
	) -> tg::Result<tg::pty::create::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
			let arg = tg::pty::create::Arg {
				local: None,
				remotes: None,
				size: arg.size,
			};
			return client
				.create_pty(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the pty on the remote"));
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Create the pty.
		let id = tg::pty::Id::new();
		let pty = super::Pty::new(self, arg.size)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the pty"))?;
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
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		let output = self
			.create_pty_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the pty"))?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
