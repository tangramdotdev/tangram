use crate::Server;
use tangram_client as tg;
use tangram_http::{Body, response::builder::Ext as _};

impl Server {
	pub async fn close_pipe(&self, id: &tg::pipe::Id) -> tg::Result<()> {
		let (_, pipe) = self
			.pipes
			.remove(id)
			.ok_or_else(|| tg::error!("failed to find the pipe"))?;
		pipe.sender.send(tg::pipe::Event::End).await.ok();
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_close_pipe_request<H>(
		handle: &H,
		_request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		handle.close_pipe(&id).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
