use crate::Server;
use tangram_client as tg;
use tangram_http::{Incoming, Outgoing};

impl Server {
	pub async fn wait_process(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<tg::process::wait::Output> {
		todo!()
	}
}

impl Server {
	pub(crate) async fn handle_post_process_wait_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		todo!()
	}
}
