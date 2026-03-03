use {
	crate::server::Server,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext, builder::Ext as _},
	},
};

impl Server {
	pub async fn wait(
		&self,
		arg: crate::client::wait::Arg,
	) -> tg::Result<crate::client::wait::Output> {
		todo!()
	}

	pub(crate) async fn handle_wait(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;

		// Wait.
		let output = self
			.wait(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to wait"))?;

		let response = http::Response::builder()
			.json(output)
			.unwrap()
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
