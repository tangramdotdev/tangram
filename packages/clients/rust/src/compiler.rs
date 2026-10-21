use {
	crate::prelude::*,
	futures::{TryFutureExt as _, future},
	std::pin::pin,
	tangram_http::{request::Ext as _, request::builder::Ext as _, response::Ext as _},
	tokio::io::{AsyncBufRead, AsyncWrite, AsyncWriteExt as _},
};

impl tg::Session {
	pub async fn lsp(
		&self,
		mut input: impl AsyncBufRead + Send + Unpin + 'static,
		mut output: impl AsyncWrite + Send + Unpin + 'static,
	) -> tg::Result<()> {
		let mut sender =
			tg::Client::connect_h1(self.client().url(), self.client().http().connect_timeout)
				.await?;
		let method = http::Method::POST;
		let uri = "/lsp";
		let mut request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::CONNECTION, "upgrade")
			.header(http::header::UPGRADE, "lsp")
			.empty()
			.unwrap();
		self.apply_context_headers(&mut request);
		let response = sender
			.send_request(request.boxed_body())
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() != http::StatusCode::SWITCHING_PROTOCOLS {
			let bytes = response
				.bytes()
				.await
				.map_err(|error| tg::error!(!error, "failed to collect the response body"))?;
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("failed to deserialize the error"));
			return Err(error);
		}
		let io = hyper::upgrade::on(response)
			.await
			.map_err(|error| tg::error!(!error, "failed to perform the upgrade"))?;
		let io = hyper_util::rt::TokioIo::new(io);
		let (mut o, mut i) = tokio::io::split(io);

		// Copy the input until it ends, then shut down the write half so the server sees the end of the input.
		let input = async move {
			tokio::io::copy(&mut input, &mut i)
				.await
				.map_err(|error| tg::error!(!error, "failed to copy the input"))?;
			i.shutdown()
				.await
				.map_err(|error| tg::error!(!error, "failed to shut down the input"))?;
			Ok::<_, tg::Error>(())
		};

		// Copy the output until the server closes the connection.
		let output = tokio::io::copy(&mut o, &mut output)
			.map_err(|error| tg::error!(!error, "failed to copy the output"));

		// Finish when the server closes the connection, because the server stops reading after the exit notification and the input may never end.
		match future::select(pin!(input), pin!(output)).await {
			future::Either::Left((result, output)) => {
				result?;
				output.await?;
			},
			future::Either::Right((result, _)) => {
				result?;
			},
		}

		Ok(())
	}
}
