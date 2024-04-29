use crate::Server;
use tangram_client as tg;
use tangram_http::{outgoing::ResponseExt as _, Incoming, Outgoing};

impl Server {
	pub async fn push_object(&self, id: &tg::object::Id) -> tg::Result<()> {
		let remote = self
			.remotes
			.first()
			.ok_or_else(|| tg::error!("the server does not have a remote"))?;
		tg::object::Handle::with_id(id.clone())
			.push(self, remote, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to push the object"))?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_push_object_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		handle.push_object(&id).await?;
		let response = http::Response::ok();
		Ok(response)
	}
}
