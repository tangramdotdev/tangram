use crate::{
	util::http::{bad_request, ok, Incoming, Outgoing},
	Server,
};
use tangram_client as tg;

impl Server {
	pub async fn pull_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		let remote = self
			.remotes
			.first()
			.ok_or_else(|| tg::error!("the server does not have a remote"))?;
		tg::object::Handle::with_id(id.clone())
			.pull(self, remote, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to pull the object"))?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_pull_object_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["objects", id, "pull"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(tg::error!(%path, "unexpected path"));
		};
		let Ok(id) = id.parse() else {
			return Ok(bad_request());
		};

		// Pull the object.
		handle.pull_object(&id).await?;

		Ok(ok())
	}
}
