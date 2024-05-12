use crate as tg;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

pub type Output = Vec<String>;

impl tg::Client {
	pub async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<Option<tg::package::versions::Output>> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/versions");
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
		let output = response.json().await?;
		Ok(Some(output))
	}
}
