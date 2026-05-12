use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
	tangram_uri::Uri,
};

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	pub namespace: tg::Namespace,

	pub permission: tg::Permission,
}

impl tg::Session {
	pub async fn grant_group_namespace_permission(
		&self,
		group: &str,
		arg: tg::group::grant::Arg,
	) -> tg::Result<tg::Grant> {
		let path = format!("/groups/{group}/grants");
		let uri = Uri::builder().path(&path).build().unwrap();
		let request = http::request::Builder::default()
			.method(http::Method::PUT)
			.uri(uri)
			.header(http::header::ACCEPT, mime::APPLICATION_JSON.to_string())
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
		let output = response
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the response"))?;
		Ok(output)
	}
}

impl tg::Client {
	pub async fn grant_group_namespace_permission(
		&self,
		group: &str,
		arg: tg::group::grant::Arg,
	) -> tg::Result<tg::Grant> {
		self.session(self.context())
			.grant_group_namespace_permission(group, arg)
			.await
	}
}
