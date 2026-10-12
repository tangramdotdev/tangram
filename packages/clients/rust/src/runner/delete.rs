use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

#[derive(Clone, Debug, Default, serde::Deserialize, serde::Serialize)]
pub struct Arg {
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub location: Option<tg::location::Arg>,
}

impl tg::Session {
	pub async fn try_delete_runner(
		&self,
		runner: &tg::runner::Id,
		arg: tg::runner::delete::Arg,
	) -> tg::Result<Option<()>> {
		let request = http::request::Builder::default()
			.method(http::Method::DELETE)
			.uri(format!("/runners/{runner}"))
			.arg(&arg, tangram_http::body::Empty::new())
			.map_err(|error| tg::error!(!error, "failed to serialize the arg"))?
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
			return Err(tg::error!(!error, %status, "the request failed"));
		}

		Ok(Some(()))
	}
}
