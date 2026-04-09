use {
	crate::prelude::*,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
	tangram_util::serde::CommaSeparatedString,
};

#[serde_as]
#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Output {
	pub data: Vec<tg::process::get::Output>,
}

impl tg::Client {
	pub async fn list_processes(
		&self,
		arg: tg::process::list::Arg,
	) -> tg::Result<tg::process::list::Output> {
		let method = http::Method::GET;
		let uri = Uri::builder()
			.path("/processes")
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
