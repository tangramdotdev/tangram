use crate::Server;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn close_pty(&self, id: &tg::pty::Id, arg: tg::pty::close::Arg) -> tg::Result<()> {
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			let arg = tg::pty::close::Arg::default();
			remote.close_pty(id, arg).await?;
			return Ok(());
		}

		let Some((_, pty)) = self.ptys.remove(id) else {
			return Ok(());
		};
		unsafe {
			libc::close(pty.host);
			libc::close(pty.guest);
		}

		Ok(())
	}

	pub(crate) async fn handle_close_pty_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		handle.close_pty(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
