use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

impl tg::Client {
	pub async fn delete_remote(&self, name: &str) -> tg::Result<()> {
		let method = http::Method::DELETE;
		let name = urlencoding::encode(name);
		let uri = format!("/remotes/{name}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if !response.status().is_success() {
			let error = response
				.json()
				.await
				.map_err(|source| tg::error!(!source, "failed to deserialize the error response"))?;
			return Err(error);
		}
		Ok(())
	}
}
