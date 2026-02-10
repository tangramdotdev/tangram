use {
	crate::{Context, Server},
	futures::{FutureExt as _, TryFutureExt as _, future},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_futures::task::Stop,
	tangram_http::{Body, response::builder::Ext as _},
	tokio::io::{AsyncBufRead, AsyncWrite},
};

impl Server {
	pub(crate) async fn lsp_with_context(
		&self,
		context: &Context,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		let compiler = self.create_compiler();
		compiler
			.serve(input, output)
			.await
			.map_err(|source| tg::error!(!source, "failed to serve the lsp"))?;
		compiler.stop();
		compiler
			.wait()
			.await
			.map_err(|source| tg::error!(!source, "failed to wait for the compiler"))?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_lsp_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		// Ensure the connection header is set correctly.
		if request
			.headers()
			.get(http::header::CONNECTION)
			.is_none_or(|value| value != "upgrade")
		{
			return Err(tg::error!(
				"expected the connection header to be set to upgrade"
			));
		}

		// Ensure the upgrade header is set correctly.
		if request
			.headers()
			.get(http::header::UPGRADE)
			.is_none_or(|value| value != "lsp")
		{
			return Err(tg::error!("expected the upgrade header to be set to lsp"));
		}

		// Spawn the LSP.
		let handle = self.clone();
		let context = context.clone();
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		tokio::spawn(
			async move {
				let io = hyper::upgrade::on(request)
					.await
					.map_err(|source| tg::error!(!source, "failed to perform the upgrade"))?;
				let io = hyper_util::rt::TokioIo::new(io);
				let (input, output) = tokio::io::split(io);
				let input = tokio::io::BufReader::new(input);
				let task = handle.lsp_with_context(&context, input, output);
				future::select(pin!(task), pin!(stop.wait()))
					.map(|output| match output {
						future::Either::Left((Err(error), _)) => Err(error),
						_ => Ok(()),
					})
					.await?;
				Ok::<_, tg::Error>(())
			}
			.inspect_err(|error| tracing::error!(error = %error.trace())),
		);

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::SWITCHING_PROTOCOLS)
			.header(http::header::CONNECTION, "upgrade")
			.header(http::header::UPGRADE, "lsp")
			.empty()
			.unwrap();

		Ok(response)
	}
}
