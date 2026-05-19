use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub group: String,
}

impl tg::Session {
	pub async fn try_delete_group(&self, group: &str) -> tg::Result<Option<()>> {
		let arg = tg::group::delete::Arg {
			group: group.to_owned(),
		};
		let uri = Uri::builder()
			.path("/groups")
			.query_params(&arg)
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
			.build()
			.unwrap();
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

impl tg::Client {
	pub async fn try_delete_group(&self, group: &str) -> tg::Result<Option<()>> {
		self.session(self.context()).try_delete_group(group).await
	}

	pub async fn delete_group(&self, group: &str) -> tg::Result<()> {
		self.try_delete_group(group)
			.await?
			.ok_or_else(|| tg::error!("failed to find the group"))
	}
}
