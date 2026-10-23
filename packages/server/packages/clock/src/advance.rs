use {
	crate::Component,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn try_advance_clock(
		&self,
		session: &Session,
		arg: tg::clock::advance::Arg,
	) -> tg::Result<Option<tg::clock::get::Output>> {
		let Some(clock) = Self::virtual_clock(session)? else {
			return Ok(None);
		};
		let now = clock.advance(arg.duration)?;
		let output = tg::clock::get::Output { now };
		Ok(Some(output))
	}

	pub(crate) async fn advance_clock_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let arg = request
			.json()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg: tg::clock::advance::Arg = arg;
		let Some(output) = self
			.try_advance_clock(session, arg)
			.await
			.map_err(|error| tg::error!(!error, "failed to advance the clock"))?
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
