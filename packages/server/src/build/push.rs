use crate::{
	util::http::{bad_request, ok, Incoming, Outgoing},
	Http, Server,
};
use tangram_client as tg;

impl Server {
	pub async fn push_build(&self, id: &tg::build::Id) -> tg::Result<()> {
		let remote = self
			.remotes
			.first()
			.ok_or_else(|| tg::error!("the server does not have a remote"))?;
		tg::Build::with_id(id.clone())
			.push(self, remote)
			.await
			.map_err(|source| tg::error!(!source, "failed to push the build"))?;
		Ok(())
	}
}

impl<H> Http<H>
where
	H: tg::Handle,
{
	pub async fn handle_push_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "push"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Push the build.
		self.handle.push_build(&id).await?;

		Ok(ok())
	}
}
