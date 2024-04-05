use crate::{
	util::http::{bad_request, ok, Incoming, Outgoing},
	Http, Server,
};
use tangram_client as tg;
use tangram_error::{error, Result};

impl Server {
	pub async fn push_object(&self, id: &tg::object::Id) -> Result<()> {
		let remote = self
			.inner
			.remotes
			.first()
			.ok_or_else(|| error!("the server does not have a remote"))?;
		tg::object::Handle::with_id(id.clone())
			.push(self, remote)
			.await
			.map_err(|source| error!(!source, "failed to push the object"))?;
		Ok(())
	}
}

impl<H> Http<H> where H: tg::Handle {
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
}
