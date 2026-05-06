use {
	crate::prelude::*,
	serde_with::{DurationSecondsWithFrac, serde_as},
	std::time::Duration,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Output {
	pub data: Vec<Item>,
}

#[serde_as]
#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Item {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cpu: Option<u64>,

	pub id: tg::sandbox::Id,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub hostname: Option<String>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub memory: Option<u64>,

	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	pub mounts: Vec<tg::sandbox::Mount>,

	#[serde(default = "default_network")]
	pub network: tg::Either<bool, tg::sandbox::Network>,

	pub status: tg::sandbox::Status,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<DurationSecondsWithFrac>")]
	pub ttl: Option<Duration>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub user: Option<String>,
}

impl tg::Client {
	pub async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		let method = http::Method::GET;
		let uri = Uri::builder()
			.path("/sandboxes")
			.query_params(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.build()
			.unwrap();
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
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

fn default_network() -> tg::Either<bool, tg::sandbox::Network> {
	tg::Either::Left(false)
}
