use crate::Server;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn close_pipe(&self, id: &tg::pipe::Id, arg: tg::pipe::close::Arg) -> tg::Result<()> {
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			return remote.close_pipe(id, tg::pipe::close::Arg::default()).await;
		}
		self.try_release_pipe(id)
			.await
			.map_err(|source| tg::error!(!source, "failed to release the pipe"))
			.ok();
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_close_pipe_request<H>(
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

		handle.close_pipe(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
