use {
	crate::Session,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Session {
	pub(crate) async fn try_set_clock(
		&self,
		arg: tg::clock::set::Arg,
	) -> tg::Result<Option<tg::clock::get::Output>> {
		let Some(clock) = self.virtual_clock()? else {
			return Ok(None);
		};
		let now = clock.set(arg.now)?;
		let output = tg::clock::get::Output { now };
		Ok(Some(output))
	}

	pub(crate) async fn set_clock_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg: tg::clock::set::Arg = arg;
		let Some(output) = self
			.try_set_clock(arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to set the clock"))?
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
