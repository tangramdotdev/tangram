use {
	crate::{Server, handle::ServerOrProxy},
	tangram_client::{self as tg, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn delete_pty(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::delete::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.delete_pty(id, arg).await?;
			return Ok(());
		}

		self.ptys.remove(id);

		Ok(())
	}

	pub(crate) async fn handle_delete_pty_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.delete_pty(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
