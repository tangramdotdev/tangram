use {
	crate::prelude::*,
	std::path::PathBuf,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub path: PathBuf,
}

impl tg::Client {
	pub async fn delete_watch(&self, arg: tg::watch::delete::Arg) -> tg::Result<()> {
		let response = self
			.send(|| {
				http::request::Builder::default()
					.method(http::Method::DELETE)
					.uri("/watches")
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
