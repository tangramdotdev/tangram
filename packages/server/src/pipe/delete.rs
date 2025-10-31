use {
	crate::{Server, handle::ServerOrProxy},
	tangram_client::{self as tg, prelude::*},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn delete_pipe(
		&self,
		id: &tg::pipe::Id,
		mut arg: tg::pipe::delete::Arg,
	) -> tg::Result<()> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			remote.delete_pipe(id, arg).await?;
			return Ok(());
		}
		self.pipes.remove(id);
		Ok(())
	}

	pub(crate) async fn handle_delete_pipe_request(
		handle: &ServerOrProxy,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		let id = id.parse()?;
		let arg = request.json().await?;
		handle.delete_pipe(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
