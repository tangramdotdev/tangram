use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub member: tg::group::Member,
}

impl tg::Session {
	pub async fn add_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<()> {
		let arg = tg::group::members::add::Arg {
			member: member.to_owned(),
		};
		let uri = format!("/groups/{}/members", group.to_string().replace('/', ":"));
		let request = http::request::Builder::default()
			.method(http::Method::POST)
			.uri(uri)
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.json(arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		Ok(())
	}
}
