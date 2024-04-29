use crate as tg;
use serde_with::{serde_as, DisplayFromStr};
use std::collections::BTreeMap;
use tangram_http::{incoming::ResponseExt as _, Outgoing};

#[serde_as]
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(flatten)]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub info: Option<Info>,

	#[serde_as(as = "BTreeMap<DisplayFromStr, _>")]
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub dependencies: BTreeMap<tg::Dependency, Self>,
}

#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct Info {
	pub current: String,
	pub compatible: String,
	pub latest: String,
	pub yanked: bool,
}

impl tg::Client {
	pub async fn get_package_outdated(
		&self,
		dependency: &tg::Dependency,
	) -> tg::Result<tg::package::outdated::Output> {
		let method = http::Method::POST;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let uri = format!("/packages/{dependency}/outdated");
		let body = Outgoing::empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.unwrap();
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Err(tg::error!(%dependency, "could not find package"));
		}
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
