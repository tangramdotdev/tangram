use crate::Client;
use http_body_util::BodyExt;
use tangram_error::{error, Result, WrapErr};
use tangram_util::http::empty;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Health {
	pub version: String,
}

impl Client {
	pub async fn health(&self) -> Result<Health> {
		let method = http::Method::GET;
		let uri = "/health";
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.wrap_err("Failed to collect the response body.")?
			.to_bytes();
		let health = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;
		Ok(health)
	}

	pub async fn clean(&self) -> Result<()> {
		let method = http::Method::POST;
		let uri = "/clean";
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.wrap_err("Failed to create the request.")?;
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.wrap_err("Failed to collect the response body.")?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| error!("The request did not succeed."));
			return Err(error);
		}
		Ok(())
	}

	pub async fn stop(&self) -> Result<()> {
		let method = http::Method::POST;
		let uri = "/stop";
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.wrap_err("Failed to create the request.")?;
		self.send(request).await.ok();
		Ok(())
	}
}
