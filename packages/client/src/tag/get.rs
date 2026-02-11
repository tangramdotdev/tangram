use {
	crate::prelude::*,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::CommaSeparatedString,
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub cached: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub ttl: Option<u64>,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Child {
	pub component: String,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct Output {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub children: Option<Vec<Child>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub item: Option<tg::Either<tg::object::Id, tg::process::Id>>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	pub tag: tg::Tag,
}

impl tg::Client {
	pub async fn try_get_tag(
		&self,
		tag: &tg::Tag,
		arg: tg::tag::get::Arg,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?;
		let uri = format!("/tags/{tag}?{query}");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
			.unwrap();
		let response = self
			.send(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
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
		Ok(Some(output))
	}
}
