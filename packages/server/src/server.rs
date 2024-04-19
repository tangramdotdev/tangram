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

	pub async fn path(&self) -> tg::Result<Option<tg::Path>> {
		Ok(Some(self.path.clone().try_into()?))
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_health_request(
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

	pub async fn handle_path_request(
		&self,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		let path = self.handle.path().await?;
		let body = serde_json::to_string(&path).unwrap();
		Ok(http::Response::builder()
			.status(http::StatusCode::OK)
			.body(full(body))
			.unwrap())
	}

	pub async fn handle_clean_request(
		&self,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		self.handle.clean().await?;
		Ok(http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap())
	}

	#[allow(clippy::unnecessary_wraps)]
	pub async fn handle_stop_request(
		&self,
		_request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		self.stop();
		Ok(ok())
	}
}
