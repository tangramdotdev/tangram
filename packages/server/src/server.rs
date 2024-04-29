use crate::Server;
use tangram_client as tg;
use tangram_http::{outgoing::ResponseExt as _, Incoming, Outgoing};

impl Server {
	pub async fn health(&self) -> tg::Result<tg::server::Health> {
		Ok(tg::server::Health {
			version: self.options.version.clone(),
		})
	}
}

impl Server {
	pub(crate) async fn handle_server_clean_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		handle.clean().await?;
		Ok(http::Response::builder()
			.status(http::StatusCode::OK)
			.body(Outgoing::empty())
			.unwrap())
	}

	pub(crate) async fn handle_server_health_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let health = handle.health().await?;
		let body = serde_json::to_vec(&health).unwrap();
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(Outgoing::bytes(body))
			.unwrap();
		Ok(response)
	}

	#[allow(clippy::unnecessary_wraps)]
	pub(crate) async fn handle_server_stop_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		handle.stop().await?;
		Ok(http::Response::ok())
	}
}
