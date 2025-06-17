use crate::Server;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn create_pty(
		&self,
		mut arg: tg::pty::create::Arg,
	) -> tg::Result<tg::pty::create::Output> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			return remote.create_pty(arg).await;
		}
		let id = tg::pty::Id::new();

		// Create the pty.
		let pty = super::Pty::open(arg.size).await?;
		self.ptys.insert(id.clone(), pty);

		// Create the output.
		let output = tg::pty::create::Output { id };

		Ok(output)
	}

	pub(crate) async fn handle_create_pty_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let arg = request.json().await?;
		let output = handle.create_pty(arg).await?;
		let response = http::Response::builder()
			.json(output)
			.map_err(|source| tg::error!(!source, "failed to serialize the output"))?
			.unwrap();
		Ok(response)
	}
}
