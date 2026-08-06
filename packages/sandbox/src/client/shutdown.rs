use {
	super::Client,
	tangram_client::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

impl Client {
	pub async fn shutdown(&self) -> tg::Result<()> {
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri("/shutdown")
			.empty()
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response
				.json()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			return Err(error);
		}
		response
			.bytes()
			.await
			.map_err(|error| tg::error!(!error, "failed to read the response"))?;
		self.sender.lock().await.take();
		if let Some(task) = self.connection.lock().await.take() {
			task.abort();
			match task.await {
				Err(error) if error.is_cancelled() => {},
				Err(error) => return Err(tg::error!(!error, "the connection task panicked")),
				Ok(()) => {},
			}
		}

		Ok(())
	}
}
