use crate::{
	util::http::{empty, full, ok, Incoming, Outgoing},
	Http, Server,
};
use tangram_client as tg;

impl Server {
	pub async fn health(&self) -> tg::Result<tg::server::Health> {
		Ok(tg::server::Health {
			version: self.options.version.clone(),
		})
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_server_clean_request(
		&self,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		self.handle.clean().await?;
		Ok(http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap())
	}

	pub async fn handle_server_health_request(
		&self,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		let health = self.handle.health().await?;
		let body = serde_json::to_vec(&health).unwrap();
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();
		Ok(response)
	}

	#[allow(clippy::unnecessary_wraps)]
	pub async fn handle_server_stop_request(
		&self,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		self.stop();
		Ok(ok())
	}
}
