use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_http::{body::Boxed as BoxBody, response::Ext as _, response::builder::Ext as _},
};

impl Session {
	pub(crate) async fn try_get_clock(&self) -> tg::Result<Option<tg::clock::get::Output>> {
		let Some(clock) = self.virtual_clock()? else {
			return Ok(None);
		};
		let now = clock.now();
		let output = tg::clock::get::Output { now };
		Ok(Some(output))
	}

	pub(crate) async fn get_clock_request(
		&self,
		_request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let Some(output) = self
			.try_get_clock()
			.await
			.map_err(|error| tg::error!(!error, "failed to get the clock"))?
		else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
		let body = serde_json::to_vec(&output).unwrap();
		let response = http::Response::builder()
			.header(
				http::header::CONTENT_TYPE,
				mime::APPLICATION_JSON.to_string(),
			)
			.body(BoxBody::with_bytes(body))
			.unwrap();
		Ok(response)
	}
}
