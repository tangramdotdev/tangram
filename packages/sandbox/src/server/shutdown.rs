use {
	crate::server::Server,
	tangram_client::prelude::*,
	tangram_http::{
		body::Boxed as BoxBody,
		response::{Ext as _, builder::Ext as _},
	},
};

impl Server {
	pub(super) async fn shutdown_request(&self) -> tg::Result<http::Response<BoxBody>> {
		self.stopper.stop();
		let response = http::Response::builder().empty().unwrap().boxed_body();

		Ok(response)
	}
}
