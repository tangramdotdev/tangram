use crate as tg;
use crate::Client;
use http_body_util::BodyExt;
use tangram_error::{error, Result, WrapErr};
use tangram_util::http::full;

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct DequeueArg {
	#[serde(default)]
	pub hosts: Option<Vec<tg::System>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct DequeueOutput {
	pub build: tg::build::Id,
	pub options: tg::build::Options,
}

impl Client {
	pub async fn try_dequeue_build(
		&self,
		user: Option<&tg::User>,
		arg: tg::build::queue::DequeueArg,
	) -> Result<Option<tg::build::queue::DequeueOutput>> {
		let method = http::Method::POST;
		let uri = "/builds/dequeue";
		let mut request = http::request::Builder::default().method(method).uri(uri);
		let user = user.or(self.inner.user.as_ref());
		if let Some(token) = user.and_then(|user| user.token.as_ref()) {
			request = request.header(http::header::AUTHORIZATION, format!("Bearer {token}"));
		}
		let body = serde_json::to_vec(&arg).wrap_err("Failed to serialize the body.")?;
		let body = full(body);
		let request = request
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
		let item =
			serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the response body.")?;
		Ok(item)
	}
}
