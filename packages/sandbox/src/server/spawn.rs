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
	pub async fn spawn(
		&self,
		arg: crate::client::spawn::Arg,
	) -> tg::Result<crate::client::spawn::Output> {
		todo!()
	}

	pub(crate) async fn handle_spawn(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;

		// Spawn.
		let output = self
			.spawn(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to spawn"))?;

		let response = http::Response::builder()
			.json(output)
			.unwrap()
			.unwrap()
			.boxed_body();

		Ok(response)
	}
}
