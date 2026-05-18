use {
	crate::{Session, context::Authentication},
	futures::{TryFutureExt as _, future},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
	tokio::io::{AsyncBufRead, AsyncWrite},
};

impl Session {
	pub(crate) fn create_compiler(&self) -> tangram_compiler::Shared {
		let handle = tg::handle::dynamic::Handle::new(self.clone());
		let artifacts_path = self.server.artifacts_path();
		let tags_path = self.server.tags_path();
		let library_path = self.server.library_path();
		let main_runtime_handle = tokio::runtime::Handle::current();
		let version = self.server.version.clone();
		tangram_compiler::Compiler::start(
			handle,
			artifacts_path,
			tags_path,
			library_path,
			main_runtime_handle,
			version,
		)
	}

	pub(crate) async fn lsp(
		&self,
		input: impl AsyncBufRead + Send + Unpin + 'static,
		output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		if self
			.context
			.authentication
			.as_ref()
			.is_some_and(Authentication::is_process)
		{
			return Err(tg::error!("unauthorized"));
		}
		let compiler = self.create_compiler();
		let result = match self.context.stopper.clone() {
			Some(stopper) => {
				let serve = compiler.serve(input, output);
				match future::select(pin!(serve), pin!(stopper.wait())).await {
					future::Either::Left((result, _)) => result,
					future::Either::Right(_) => Ok(()),
				}
			},
			None => compiler.serve(input, output).await,
		};
		result.map_err(|error| tg::error!(!error, "failed to serve the lsp"))?;
		compiler.stop();
		compiler
			.wait()
			.await
			.map_err(|error| tg::error!(!error, "failed to wait for the compiler"))?;
		Ok(())
	}

	pub(crate) async fn lsp_request(
		&self,
		request: http::Request<BoxBody>,
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
		let session = self.clone();
		tokio::spawn(
			async move {
				let io = hyper::upgrade::on(request)
					.await
					.map_err(|error| tg::error!(!error, "failed to perform the upgrade"))?;
				let io = hyper_util::rt::TokioIo::new(io);
				let (input, output) = tokio::io::split(io);
				let input = tokio::io::BufReader::new(input);
				session.lsp(input, output).await?;
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
