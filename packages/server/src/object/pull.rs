use crate::{Http, Server};
use tangram_client as tg;
use tangram_error::{error, Result};
use tangram_http::{bad_request, ok, Incoming, Outgoing};

impl Server {
	pub async fn pull_object(&self, id: &tg::object::Id) -> Result<()> {
		let remote = self
			.inner
			.remotes
			.first()
			.ok_or_else(|| error!("the server does not have a remote"))?;
		tg::object::Handle::with_id(id.clone())
			.pull(self, remote.as_ref())
			.await
			.map_err(|source| error!(!source, "failed to pull the object"))?;
		Ok(())
	}
}

impl Http {
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
