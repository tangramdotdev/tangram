use crate::Server;
use tangram_client as tg;
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn remove_remote(&self, name: &str) -> tg::Result<()> {
		self.remotes
			.remove(name)
			.ok_or_else(|| tg::error!("failed to find the remote"))?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_delete_remote_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		name: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		handle.delete_remote(name).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
