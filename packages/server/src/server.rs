use crate::Server;
use tangram_error::Result;
use tangram_util::http::{empty, full, ok, Incoming, Outgoing};

impl Server {
	pub async fn handle_health_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		let health = self.health().await?;
		let body = serde_json::to_vec(&health).unwrap();
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap();
		Ok(response)
	}

	pub async fn handle_clean_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		self.clean().await?;
		Ok(http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap())
	}

	#[allow(clippy::unnecessary_wraps, clippy::unused_async)]
	pub async fn handle_stop_request(
		&self,
		_request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		self.stop();
		Ok(ok())
	}
}
