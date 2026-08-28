use {
	super::sign::Request,
	super::{Client, REQUEST_TIMEOUT, Response},
	bytes::Bytes,
	tangram_client::prelude::*,
	tangram_http::body::Ext as _,
};

impl Client {
	pub async fn send(
		&self,
		method: http::Method,
		path: &str,
		headers: http::HeaderMap,
		body: Bytes,
	) -> tg::Result<Response> {
		let (credentials, session_token) = self.signing_credentials().await?;
		let arg = Request {
			body,
			credentials: &credentials,
			headers,
			method,
			path,
			query: None,
			session_token: session_token.as_deref(),
		};
		let request = self.request(arg)?;
		self.send_request(request).await
	}

	pub(super) async fn send_request(
		&self,
		request: http::Request<tangram_http::body::Boxed>,
	) -> tg::Result<Response> {
		let mut connection = loop {
			let connection = self
				.pool
				.get_exclusive(tangram_pool::Priority::default())
				.await
				.map_err(|error| tg::error!(!error, "failed to get an S3 connection"))?;
			if connection.is_closed() {
				connection.discard();
				continue;
			}
			break connection;
		};
		let response = match tokio::time::timeout(REQUEST_TIMEOUT, connection.send(request)).await {
			Ok(Ok(response)) => response,
			Ok(Err(error)) => {
				connection.discard();
				return Err(error);
			},
			Err(_) => {
				connection.discard();
				return Err(tg::error!("the S3 request timed out"));
			},
		};
		let (parts, body) = response.into_parts();
		let bytes = match tokio::time::timeout(REQUEST_TIMEOUT, body.collect()).await {
			Ok(Ok(body)) => body.to_bytes(),
			Ok(Err(error)) => {
				connection.discard();
				return Err(tg::error!(!error, "failed to read the S3 response body"));
			},
			Err(_) => {
				connection.discard();
				return Err(tg::error!("the S3 response body timed out"));
			},
		};
		let output = Response {
			bytes,
			headers: parts.headers,
			status: parts.status,
		};

		Ok(output)
	}
}
