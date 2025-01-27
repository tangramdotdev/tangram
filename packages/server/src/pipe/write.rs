use crate::Server;
use bytes::Bytes;
use tangram_client as tg;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};

impl Server {
	pub async fn write_pipe(&self, id: &tg::pipe::Id, bytes: Bytes) -> tg::Result<()> {
		let sender = self
			.pipes
			.get(id)
			.ok_or_else(|| tg::error!("failed to find the pipe"))?
			.value()
			.sender
			.clone();
		sender
			.send(tg::pipe::read::Event::Chunk(bytes))
			.await
			.map_err(|source| tg::error!(!source, "failed to write to the pipe"))?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_write_pipe_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let bytes = request.bytes().await?;
		handle.write_pipe(&id, bytes).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
