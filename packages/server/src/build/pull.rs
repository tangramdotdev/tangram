use crate::{Http, Server};
use tangram_client as tg;
use tangram_error::{error, Result};
use tangram_http::{bad_request, ok, Incoming, Outgoing};

impl Server {
	pub async fn pull_build(&self, id: &tg::build::Id) -> Result<()> {
		let remote = self
			.inner
			.remotes
			.first()
			.ok_or_else(|| error!("the server does not have a remote"))?;
		tg::Build::with_id(id.clone())
			.pull(self, remote.as_ref())
			.await
			.map_err(|source| error!(!source, "failed to pull the build"))?;
		Ok(())
	}
}

impl Http {
	pub async fn handle_pull_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["build", id, "pull"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Pull the build.
		self.inner.tg.pull_build(&id).await?;

		Ok(ok())
	}
}
