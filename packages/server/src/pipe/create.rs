use {
	crate::{Context, Server},
	tangram_client::prelude::*,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub(crate) async fn create_pipe_with_context(
		&self,
		context: &Context,
		arg: tg::pipe::create::Arg,
	) -> tg::Result<tg::pipe::create::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = Self::remote(arg.local, arg.remotes.as_ref())? {
			let client = self
				.get_remote_client(remote)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the remote client"))?;
			let arg = tg::pipe::create::Arg {
				local: None,
				remotes: None,
			};
			return client
				.create_pipe(arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to create the pipe on the remote"));
		}

		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		// Create the pipe.
		let id = tg::pipe::Id::new();
		let pipe = super::Pipe::new()
			.await
			.map_err(|source| tg::error!(!source, "failed to create the pipe"))?;
		self.pipes.insert(id.clone(), pipe);

		// Create the output.
		let output = tg::pipe::create::Output { id };

		Ok(output)
	}

	pub(crate) async fn handle_create_pipe_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		let output = self
			.create_pipe_with_context(context, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the pipe"))?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
