use crate::Client;
use futures::{future, TryFutureExt};
use http_body_util::BodyExt;
use tangram_error::{error, Result, Wrap, WrapErr};
use tangram_util::http::empty;
use tokio::io::{AsyncRead, AsyncWrite};

impl Client {
	pub async fn lsp(
		&self,
		mut input: Box<dyn AsyncRead + Send + Unpin + 'static>,
		mut output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> Result<()> {
		let mut sender = self.connect_h1().await?;
		let method = http::Method::POST;
		let uri = "/lsp";
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::UPGRADE, "lsp".to_string())
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = sender
			.send_request(request)
			.await
			.wrap_err("Failed to send the request.")?;
		if response.status() != http::StatusCode::SWITCHING_PROTOCOLS {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("Failed to deserialize the error."));
			return Err(error);
		}
		let io = hyper::upgrade::on(response)
			.await
			.wrap_err("Failed to perform the upgrade.")?;
		let io = hyper_util::rt::TokioIo::new(io);
		let (mut o, mut i) = tokio::io::split(io);
		let input = tokio::io::copy(&mut input, &mut i)
			.map_err(|error| error.wrap("Failed to copy the input."));
		let output = tokio::io::copy(&mut o, &mut output)
			.map_err(|error| error.wrap("Failed to copy the output."));
		future::try_join(input, output).await?;
		Ok(())
	}
}
