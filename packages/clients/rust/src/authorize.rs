use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub permissions: tg::grant::permission::Set,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub principal: Option<tg::Principal>,

	pub resource: tg::grant::Resource,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub permissions: Option<tg::grant::permission::Set>,
}

impl tg::Session {
	pub async fn authorize(&self, arg: Arg) -> tg::Result<Output> {
		let method = http::Method::POST;
		let uri = "/authorize";
		let arg = serde_json::to_string(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?;
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.bytes(arg)
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(output)
	}
}
