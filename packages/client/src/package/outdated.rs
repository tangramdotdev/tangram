use crate::{self as tg, util::http::empty};
use http_body_util::BodyExt as _;
use serde_with::{serde_as, DisplayFromStr};
use std::collections::BTreeMap;

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
		let body = empty();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.body(body)
			.map_err(|source| tg::error!(!source, "failed to create the request"))?;
		let response = self.send(request).await?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Err(tg::error!(%dependency, "could not find package"));
		}
		if !response.status().is_success() {
			let bytes = response
				.collect()
				.await
				.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
				.to_bytes();
			let error = serde_json::from_slice(&bytes)
				.unwrap_or_else(|_| tg::error!("failed to deserialize the error"));
			return Err(error);
		}
		let bytes = response
			.collect()
			.await
			.map_err(|source| tg::error!(!source, "failed to collect the response body"))?
			.to_bytes();
		let output = serde_json::from_slice(&bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the response body"))?;
		Ok(output)
	}
}
