use crate::{
	self as tg,
	util::http::{empty, full},
};
use futures::{future, TryFutureExt as _};
use http_body_util::BodyExt as _;
use tokio::io::{AsyncBufRead, AsyncWrite};

impl tg::Client {
	pub async fn format(&self, text: String) -> tg::Result<String> {
		let method = http::Method::POST;
		let uri = "/format";
		let request = http::request::Builder::default().method(method).uri(uri);
		let body = full(text);
		let request = request
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("failed to deserialize the error"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let text = String::from_utf8(bytes.to_vec())
			.map_err(|source| tg::error!(!source, "failed to deserialize the response body"))?;
		Ok(text)
	}

	pub async fn lsp(
		&self,
		mut input: Box<dyn AsyncBufRead + Send + Unpin + 'static>,
		mut output: Box<dyn AsyncWrite + Send + Unpin + 'static>,
	) -> tg::Result<()> {
		let mut sender = self.connect_h1().await?;
		let method = http::Method::POST;
		let uri = "/lsp";
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::CONNECTION, "upgrade")
			.header(http::header::UPGRADE, "lsp")
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = sender
			.send_request(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if response.status() != http::StatusCode::SWITCHING_PROTOCOLS {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("failed to deserialize the error"));
			return Err(error);
		}
		let io = hyper::upgrade::on(response)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the upgrade"))?;
		let io = hyper_util::rt::TokioIo::new(io);
		let (mut o, mut i) = tokio::io::split(io);
		let input = tokio::io::copy(&mut input, &mut i)
			.map_err(|source| tg::error!(!source, "failed to copy the input"));
		let output = tokio::io::copy(&mut o, &mut output)
			.map_err(|source| tg::error!(!source, "failed to copy the output"));
		future::try_join(input, output).await?;
		Ok(())
	}
}
