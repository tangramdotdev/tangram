use crate::Server;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		let remote = tg::Client::new(arg.url);
		self.remotes.insert(name.to_owned(), remote);
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_put_remote_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		name: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		handle.put_remote(name, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
