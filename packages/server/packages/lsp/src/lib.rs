use {
	futures::{FutureExt as _, TryFutureExt as _, future, future::BoxFuture},
	std::pin::pin,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
	tangram_server_core::Session,
	tokio::io::{AsyncBufRead, AsyncWrite},
};

/// The component.
#[derive(Clone, Default)]
pub struct Component {}

impl Component {
	#[must_use]
	pub fn new() -> Self {
		Self::default()
	}

	pub(crate) async fn request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let method = request.method().clone();
		let path = request.uri().path().to_owned();
		let path_components = path.split('/').skip(1).collect::<Vec<_>>();
		match (method, path_components.as_slice()) {
			(http::Method::POST, ["lsp"]) => self.lsp_request(session, request).await,
			(_, _) => Ok(tangram_server_core::http::not_found()),
		}
	}

	pub(crate) async fn lsp(
		&self,
		session: &Session,
		input: Box<dyn AsyncBufRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> tg::Result<()> {
		if !session.server.checkouts_enabled() {
			return Err(tg::error!("checkouts are disabled"));
		}
		session.verify_request_from_host()?;
		let compiler = session.create_compiler();
		let result = match session.context.stopper.clone() {
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
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Ensure the connection header is set correctly.
		if request
			.headers()
			.get(http::header::CONNECTION)
			.is_none_or(|value| value != "upgrade")
		{
			return Err(tg::error!(
				argument,
				"expected the connection header to be set to upgrade"
			));
		}

		// Ensure the upgrade header is set correctly.
		if request
			.headers()
			.get(http::header::UPGRADE)
			.is_none_or(|value| value != "lsp")
		{
			return Err(tg::error!(
				argument,
				"expected the upgrade header to be set to lsp"
			));
		}

		// Spawn the LSP.
		let component = self.clone();
		let session = session.clone();
		tokio::spawn(
			async move {
				let io = hyper::upgrade::on(request)
					.await
					.map_err(|error| tg::error!(!error, "failed to perform the upgrade"))?;
				let io = hyper_util::rt::TokioIo::new(io);
				let (input, output) = tokio::io::split(io);
				let input = tokio::io::BufReader::new(input);
				component
					.lsp(&session, Box::new(input), Box::new(output))
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
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}

impl tangram_server_core::component::Lifecycle for Component {}

impl tangram_server_core::lsp::Component for Component {
	fn lsp<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		input: Box<dyn AsyncBufRead + Send + Unpin + 'static>,
		output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.lsp(session, input, output).boxed()
	}

	fn request<'a>(
		&'a self,
		session: &'a tangram_server_core::Session,
		request: http::Request<BoxBody>,
	) -> BoxFuture<'a, tg::Result<http::Response<BoxBody>>> {
		self.request(session, request).boxed()
	}
}
