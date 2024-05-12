use crate::Server;
use tangram_client as tg;
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

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
		_request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		handle.pull_object(&id).await?;
		let response = http::Response::builder().ok().empty().unwrap();
		Ok(response)
	}
}
