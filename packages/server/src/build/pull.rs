use crate::{
	util::http::{bad_request, ok, Incoming, Outgoing},
	Http, Server,
};
use tangram_client as tg;

impl Server {
	pub async fn pull_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		let remote = self
			
			.remotes
			.first()
			.ok_or_else(|| tg::error!("the server does not have a remote"))?;
		tg::Build::with_id(id.clone())
			.pull(self, remote)
			.await
			.map_err(|source| tg::error!(!source, "failed to pull the build"))?;
		Ok(())
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_pull_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["build", id, "pull"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Pull the build.
		self.tg.pull_build(&id).await?;

		Ok(ok())
	}
}
