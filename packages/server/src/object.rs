use super::Server;
use crate::Http;
use async_recursion::async_recursion;
use tangram_client as tg;
use tangram_error::{error, Result};
use tangram_util::http::{bad_request, ok, Incoming, Outgoing};

mod get;
mod put;

impl Server {
	pub async fn push_object(&self, id: &tg::object::Id) -> Result<()> {
		let remote = self
			.inner
			.remote
			.as_ref()
			.ok_or_else(|| error!("the server does not have a remote"))?;
		tg::object::Handle::with_id(id.clone())
			.push(self, remote.as_ref())
			.await
			.map_err(|error| error!(source = error, "failed to push the object"))?;
		Ok(())
	}

	#[async_recursion]
	pub async fn pull_object(&self, id: &tg::object::Id) -> Result<()> {
		let remote = self
			.inner
			.remote
			.as_ref()
			.ok_or_else(|| error!("the server does not have a remote"))?;
		tg::object::Handle::with_id(id.clone())
			.pull(self, remote.as_ref())
			.await
			.map_err(|error| error!(source = error, "failed to pull the object"))?;
		Ok(())
	}
}

impl Http {
	pub async fn handle_push_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id, "push"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Push the object.
		self.inner.tg.push_object(&id).await?;

		Ok(ok())
	}

	pub async fn handle_pull_object_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id, "pull"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Pull the object.
		self.inner.tg.pull_object(&id).await?;

		Ok(ok())
	}
}
