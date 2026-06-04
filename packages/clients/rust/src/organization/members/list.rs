use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(transparent)]
pub struct Output {
	pub data: Vec<tg::organization::Member>,
}

impl tg::Session {
	pub async fn list_organization_members(
		&self,
		organization: &tg::organization::Selector,
	) -> tg::Result<tg::organization::members::list::Output> {
		let path = format!(
			"/organizations/{}/members",
			organization.to_string().replace('/', ":")
		);
		let request = http::request::Builder::default()
			.method(http::Method::GET)
			.uri(path)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
			.empty()
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
			return Err(tg::error!(!error, status = %status, "the request failed"));
		}
		response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))
	}
}
