use {
	crate::{Server, handle::ServerOrProxy},
	tangram_client::{self as tg, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn close_pty(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::close::Arg,
	) -> tg::Result<()> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.close_pty(id, arg).await?;
			return Ok(());
		}

		let mut pty = self
			.ptys
			.get_mut(id)
			.ok_or_else(|| tg::error!("failed to get the pty"))?;
		if arg.master {
			pty.master.take();
		} else {
			pty.session.take();
			pty.slave.take();
		}
		drop(pty);

		Ok(())
	}

	pub(crate) async fn handle_close_pty_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		handle.close_pty(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
