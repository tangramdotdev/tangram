use crate::Server;
use tangram_client as tg;
use tangram_http::{outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		let name = name.to_owned();
		let Some(client) = self.remotes.get(&name) else {
			return Ok(None);
		};
		let url = client.url().clone();
		let output = tg::remote::get::Output { name, url };
		Ok(Some(output))
	}
}

impl Server {
	pub(crate) async fn handle_get_remote_request<H>(
		handle: &H,
		_request: http::Request<Incoming>,
		name: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let Some(output) = handle.try_get_remote(name).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
