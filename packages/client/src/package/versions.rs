use crate as tg;
use std::collections::BTreeMap;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Debug, Clone, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "std::ops::Not::not")]
	pub yanked: bool,
}

pub type Output = BTreeMap<String, tg::artifact::Id>;

impl tg::Client {
	pub async fn try_get_package_versions(
		&self,
		dependency: &tg::Dependency,
		arg: Arg,
	) -> tg::Result<Option<tg::package::versions::Output>> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let query = serde_urlencoded::to_string(arg).unwrap();
		let uri = format!("/packages/{dependency}/versions?{query}");
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
