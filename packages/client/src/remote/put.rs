use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub url: Uri,
}

impl tg::Client {
	pub async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		let uri = format!("/remotes/{name}");
		let response = self
			.send(|| {
				http::request::Builder::default()
					.method(http::Method::PUT)
					.uri(uri.clone())
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
		Ok(())
	}
}
