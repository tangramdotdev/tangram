use crate::{self as tg, util::serde::is_false};
use tangram_http::{incoming::response::Ext as _, outgoing::request::Ext as _};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub length: Option<u64>,

	#[serde(default, skip_serializing_if = "tg::tag::Pattern::is_empty")]
	pub pattern: tg::tag::Pattern,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub remote: Option<String>,

	#[serde(default, skip_serializing_if = "is_false")]
	pub reverse: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Output {
	pub data: Vec<tg::tag::get::Output>,
}

impl tg::Client {
	pub async fn list_tags(&self, arg: tg::tag::list::Arg) -> tg::Result<tg::tag::list::Output> {
		let method = http::Method::GET;
		let query = serde_urlencoded::to_string(&arg).unwrap();
		let uri = format!("/tags?{query}");
		let request = http::request::Builder::default().method(method).uri(uri);
		let request = request.empty().unwrap();
		let response = self.send(request).await?;
		if !response.status().is_success() {
			let error = response.json().await?;
			return Err(error);
		}
		let output = response.json().await?;
		Ok(output)
	}
}
