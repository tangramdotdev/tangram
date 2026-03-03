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
	pub async fn set_pty_size(&self, arg: crate::client::pty::SizeArg) -> tg::Result<()> {
		todo!()
	}

	pub(crate) async fn handle_set_pty_size(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		// Get the arg.
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;

		// Set the pty size.
		self.set_pty_size(arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to set the pty size"))?;

		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
