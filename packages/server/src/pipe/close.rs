use crate::Server;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn close_pipe(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::close::Arg,
	) -> tg::Result<()> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.close_pipe(id, arg).await?;
			return Ok(());
		}
		self.pipes.remove(id);
		Ok(())
	}

	pub(crate) async fn handle_close_pipe_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		handle.close_pipe(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
