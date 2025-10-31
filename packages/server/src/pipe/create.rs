use {
	crate::{Server, handle::ServerOrProxy},
	tangram_client::{self as tg, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn create_pipe(
		&self,
		mut arg: tg::pipe::create::Arg,
	) -> tg::Result<tg::pipe::create::Output> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			return remote.create_pipe(arg).await;
		}

		// Create the pipe.
		let id = tg::pipe::Id::new();
		let pipe = super::Pipe::new().await?;
		self.pipes.insert(id.clone(), pipe);

		// Create the output.
		let output = tg::pipe::create::Output { id };

		Ok(output)
	}

	pub(crate) async fn handle_create_pipe_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>> {
		let arg = request.json().await?;
		let output = handle.create_pipe(arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
