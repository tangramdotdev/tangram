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
	pub async fn revoke_user_namespace_permission(
		&self,
		user: &str,
		arg: tg::user::revoke::Arg,
	) -> tg::Result<Option<()>> {
		let path = format!("/users/{user}/grants");
		let uri = Uri::builder()
			.path(&path)
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
	pub async fn revoke_user_namespace_permission(
		&self,
		user: &str,
		arg: tg::user::revoke::Arg,
	) -> tg::Result<Option<()>> {
		self.session(self.context())
			.revoke_user_namespace_permission(user, arg)
			.await
	}
}
