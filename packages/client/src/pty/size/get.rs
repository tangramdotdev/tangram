use {
	crate::prelude::*,
	serde_with::serde_as,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_util::serde::CommaSeparatedString,
};

#[serde_as]
#[derive(Default, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub local: Option<bool>,

	pub master: bool,

	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[serde_as(as = "Option<CommaSeparatedString>")]
	pub remotes: Option<Vec<String>>,
}

impl tg::Client {
	pub async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		arg: Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		let method = http::Method::GET;
		let uri = format!("/ptys/{id}/size");
		let request = http::request::Builder::default()
			.method(method)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|source| tg::error!(!source, "failed to serialize the arg"))?
			.unwrap();
		self.send_with_retry(request)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the response"))?
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the body"))
	}
}
