use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

impl tg::Session {
	pub async fn remove_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<Option<()>> {
		let uri = format!(
			"/groups/{}/members/{}",
			group.to_string().replace('/', ":"),
			member.to_string().replace('/', ":"),
		);
		let request = http::request::Builder::default()
			.method(http::Method::DELETE)
			.uri(uri)
			.empty()
			.unwrap();
		let response = self
			.send_with_retry(request)
			.await
			.map_err(|error| tg::error!(!error, "failed to send the request"))?;
		if response.status() == http::StatusCode::NOT_FOUND {
			return Ok(None);
		}
		if !response.status().is_success() {
			let status = response.status();
			let error = response
				.json::<tg::Error>()
				.await
				.map_err(|error| tg::error!(!error, "failed to deserialize the error response"))?;
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		Ok(Some(()))
	}
}
