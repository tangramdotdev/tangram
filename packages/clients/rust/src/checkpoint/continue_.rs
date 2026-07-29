use {
	crate::prelude::*,
	tangram_http::{request::builder::Ext as _, response::Ext as _},
};

impl tg::Session {
	pub async fn try_continue_checkpoint_hit(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> tg::Result<Option<()>> {
		super::validate(checkpoint)?;
		let method = http::Method::POST;
		let uri = format!("/checkpoints/{checkpoint}/watches/{watch}/hits/{hit}/continue");
		let request = http::request::Builder::default()
			.method(method)
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
