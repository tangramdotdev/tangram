use {
	crate::prelude::*,
	std::path::PathBuf,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub items: Vec<PathBuf>,
	pub path: PathBuf,
}

impl tg::Session {
	pub async fn touch_watch(&self, arg: tg::watch::touch::Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/watches/touch";
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.json(arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response
				.json()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			return Err(error);
		}
		Ok(())
	}
}
