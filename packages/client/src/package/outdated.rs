use crate as tg;
use serde_with::{serde_as, DisplayFromStr};
use std::collections::BTreeMap;
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub locked: bool,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(flatten)]
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub info: Option<Info>,
	#[serde_as(as = "BTreeMap<DisplayFromStr, _>")]
	#[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
	pub dependencies: BTreeMap<tg::Dependency, Self>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
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
		arg: Arg,
	) -> tg::Result<tg::package::outdated::Output> {
		let method = http::Method::GET;
		let dependency = dependency.to_string();
		let dependency = urlencoding::encode(&dependency);
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/packages/{dependency}/outdated?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.empty()
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
