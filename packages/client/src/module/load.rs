use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub module: tg::module::Data,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub text: String,
}

impl tg::Client {
	pub async fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> tg::Result<tg::module::load::Output> {
		let response = self
			.send(|| {
				http::request::Builder::default()
					.method(http::Method::POST)
					.uri("/modules/load")
					.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
					.header(
						http::header::CONTENT_TYPE,
						mime::APPLICATION_JSON.to_string(),
					)
					.json(arg.clone())
					.unwrap()
					.unwrap()
			})
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response.json().await.map_err(|source| {
				tg::error!(!source, "failed to deserialize the error response")
			})?;
			return Err(error);
		}
		let output = response
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the response"))?;
		Ok(output)
	}
}
