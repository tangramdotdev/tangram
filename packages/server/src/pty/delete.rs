use crate::{Server, messenger::Messenger};
use futures::future;
use tangram_client as tg;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger as messenger;

impl Server {
	pub async fn delete_pty(&self, id: &tg::pty::Id, arg: tg::pty::delete::Arg) -> tg::Result<()> {
		eprintln!("DELETE /ptys/{id}");
		if let Some(remote) = arg.remote {
			let remote = self.get_remote_client(remote).await?;
			return remote.delete_pty(id, tg::pty::delete::Arg::default()).await;
		}
		self.send_pty_event(id, tg::pty::Event::End, false).await.ok();
		match &self.messenger {
			Messenger::Left(m) => self.delete_pty_in_memory(m, id).await?,
			Messenger::Right(m) => self.delete_pty_nats(m, id).await?,
		}
		Ok(())
	}

	async fn delete_pty_in_memory(
		&self,
		messenger: &messenger::memory::Messenger,
		id: &tg::pty::Id,
	) -> tg::Result<()> {
		messenger
			.streams()
			.close_stream(format!("{id}.master"))
			.await
			.map_err(|source| tg::error!(!source, "failed to close the pipe"))?;
		messenger
			.streams()
			.close_stream(format!("{id}.slave"))
			.await
			.map_err(|source| tg::error!(!source, "failed to close pipe"))?;
		Ok(())
	}

	async fn delete_pty_nats(
		&self,
		messenger: &messenger::nats::Messenger,
		id: &tg::pty::Id,
	) -> tg::Result<()> {
		messenger
			.jetstream
			.delete_stream(format!("{id}_master"))
			.await
			.map_err(|source| tg::error!(!source, "failed to close the pipe"))?;
		messenger
			.jetstream
			.delete_stream(format!("{id}_slave"))
			.await
			.map_err(|source| tg::error!(!source, "failed to close the pipe"))?;
		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_delete_pty_request<H>(
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

		handle.delete_pty(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
