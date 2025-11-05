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
		let method = http::Method::DELETE;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/watches?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		Ok(())
	}
}
