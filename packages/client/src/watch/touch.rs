use {crate::prelude::*, std::path::PathBuf, tangram_http::response::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub items: Vec<PathBuf>,
	pub path: PathBuf,
}

impl tg::Client {
	pub async fn touch_watch(&self, arg: tg::watch::touch::Arg) -> tg::Result<()> {
		let method = http::Method::POST;
		let uri = "/watches/touch";
		let body = serde_json::to_vec(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let response = self
			.send(|| {
				http::request::Builder::default()
					.method(method.clone())
					.uri(uri)
					.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
					.body(tangram_http::Body::with_bytes(body.clone()))
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
		Ok(())
	}
}
