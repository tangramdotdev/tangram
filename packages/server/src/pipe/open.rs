use crate::{Pipe, Server};
use tangram_client as tg;
use tangram_http::{Body, response::builder::Ext as _};

impl Server {
	pub async fn open_pipe(&self) -> tg::Result<tg::pipe::open::Output> {
		let id = tg::pipe::Id::new();
		let (sender, receiver) = tokio::sync::mpsc::channel(8);
		let pipe = Pipe {
			sender,
			receiver: Some(receiver),
		};
		self.pipes.insert(id.clone(), pipe);
		let output = tg::pipe::open::Output { id };
		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_open_pipe_request<H>(
		handle: &H,
		_request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let output = handle.open_pipe().await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
