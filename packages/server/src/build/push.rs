use crate::{
	util::http::{bad_request, ok, Incoming, Outgoing},
	Http, Server,
};
use tangram_client as tg;
use tangram_error::{error, Result};

impl Server {
	pub async fn push_build(&self, user: Option<&tg::User>, id: &tg::build::Id) -> Result<()> {
		let remote = self
			.inner
			.remotes
			.first()
			.ok_or_else(|| error!("the server does not have a remote"))?;
		tg::Build::with_id(id.clone())
			.push(user, self, remote.as_ref())
			.await
			.map_err(|source| error!(!source, "failed to push the build"))?;
		Ok(())
	}
}

impl Http {
	pub async fn handle_push_build_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<http::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "push"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Push the build.
		self.inner.tg.push_build(user.as_ref(), &id).await?;

		Ok(ok())
	}
}
