use crate::{Server, messenger::Messenger};
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger as messenger;

impl Server {
	pub async fn delete_pipe(
		&self,
		id: &tg::pipe::Id,
		arg: tg::pipe::delete::Arg,
	) -> tg::Result<()> {
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			return remote
				.delete_pipe(id, tg::pipe::delete::Arg::default())
				.await;
		}
		// self.send_pipe_event(id, tg::pipe::Event::End).await.ok();
		match &self.messenger {
			Messenger::Left(m) => self.delete_pipe_memory(m, id).await?,
			Messenger::Right(m) => self.delete_pipe_nats(m, id).await?,
		}
		Ok(())
	}

	async fn delete_pipe_memory(
		&self,
		messenger: &messenger::memory::Messenger,
		id: &tg::pipe::Id,
	) -> tg::Result<()> {
		let payload = serde_json::to_vec(&tg::pipe::Event::End).unwrap().into();
		messenger
			.streams()
			.close_stream(id.to_string(), Some(payload))
			.await
			.map_err(|source| tg::error!(!source, "failed to close the pipe"))?;
		Ok(())
	}

	async fn delete_pipe_nats(
		&self,
		messenger: &messenger::nats::Messenger,
		id: &tg::pipe::Id,
	) -> tg::Result<()> {
		messenger
			.jetstream
			.delete_stream(id.to_string())
			.await
			.map_err(|source| tg::error!(!source, "failed to close the pipe"))?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_delete_pipe_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let arg = request.query_params().transpose()?.unwrap_or_default();
		handle.delete_pipe(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
