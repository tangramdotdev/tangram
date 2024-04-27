use crate::{
	util::http::{empty, full, ok, Incoming, Outgoing},
	Server,
};
use tangram_client as tg;

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
			.body(empty())
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
			.body(full(body))
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
		Ok(ok())
	}
}
