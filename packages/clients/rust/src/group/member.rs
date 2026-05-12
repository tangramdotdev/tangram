use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

pub mod list;

impl tg::Session {
	pub async fn add_group_member(&self, group: &str, user: &str) -> tg::Result<()> {
		let path = format!("/groups/{group}/members/{user}");
		let uri = Uri::builder().path(&path).build().unwrap();
		let request = http::request::Builder::default()
			.method(http::Method::PUT)
			.uri(uri)
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
			let error = tg::error!(!error, status = %status, "the request failed");
			return Err(error);
		}
		Ok(())
	}

	pub async fn remove_group_member(&self, group: &str, user: &str) -> tg::Result<Option<()>> {
		let path = format!("/groups/{group}/members/{user}");
		let uri = Uri::builder().path(&path).build().unwrap();
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
	pub async fn add_group_member(&self, group: &str, user: &str) -> tg::Result<()> {
		self.session(self.context())
			.add_group_member(group, user)
			.await
	}

	pub async fn remove_group_member(&self, group: &str, user: &str) -> tg::Result<Option<()>> {
		self.session(self.context())
			.remove_group_member(group, user)
			.await
	}
}
