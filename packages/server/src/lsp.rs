use {
	crate::{Context, Server},
	futures::{TryFutureExt as _, future},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
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
		let result = match context.stopper.clone() {
			Some(stopper) => {
				let serve = compiler.serve(input, output);
				match future::select(pin!(serve), pin!(stopper.wait())).await {
					future::Either::Left((result, _)) => result,
					future::Either::Right(_) => Ok(()),
				}
			},
			None => compiler.serve(input, output).await,
		};
		result.map_err(|source| tg::error!(!source, "failed to serve the lsp"))?;
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
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
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
		tokio::spawn(
			async move {
				let io = hyper::upgrade::on(request)
					.await
					.map_err(|source| tg::error!(!source, "failed to perform the upgrade"))?;
				let io = hyper_util::rt::TokioIo::new(io);
				let (input, output) = tokio::io::split(io);
				let input = tokio::io::BufReader::new(input);
				handle.lsp_with_context(&context, input, output).await?;
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
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
