use {
	crate::prelude::*,
	std::path::PathBuf,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Output {
	pub data: Vec<Item>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Item {
	pub path: PathBuf,
}

impl tg::Client {
	pub async fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> tg::Result<tg::watch::list::Output> {
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/watches?{query}");
		let response = self
			.send(|| {
				http::request::Builder::default()
					.method(http::Method::GET)
					.uri(uri.clone())
					.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
					.empty()
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
