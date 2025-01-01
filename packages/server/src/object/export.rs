use crate::Server;
use std::pin::Pin;
use tangram_client::{self as tg, Handle as _};
use tangram_futures::read::Ext as _;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tokio::io::AsyncRead;
use tokio_util::task::AbortOnDropHandle;

impl Server {
	pub(crate) async fn export_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::export::Arg,
	) -> tg::Result<impl AsyncRead + Send + 'static> {
		if let Some(remote) = arg.remote.clone() {
			let reader = self.export_object_remote(id, arg, remote).await?;
			let reader = Box::pin(reader) as Pin<Box<dyn AsyncRead + Send + 'static>>;
			Ok(reader)
		} else {
			let reader = self.export_object_local(id, arg).await?;
			let reader = Box::pin(reader) as Pin<Box<dyn AsyncRead + Send + 'static>>;
			Ok(reader)
		}
	}

	async fn export_object_remote(
		&self,
		id: &tg::object::Id,
		arg: tg::object::export::Arg,
		remote: String,
	) -> tg::Result<impl AsyncRead + Send + 'static> {
		let remote = self
			.remotes
			.get(&remote)
			.ok_or_else(|| tg::error!(%remote, "the remote does not exist"))?;
		let reader = remote.export_object(id, arg).await?;
		let reader = Box::pin(reader) as Pin<Box<dyn AsyncRead + Send + 'static>>;
		Ok(reader)
	}

	async fn export_object_local(
		&self,
		id: &tg::object::Id,
		_arg: tg::object::export::Arg,
	) -> tg::Result<impl AsyncRead + Send + 'static> {
		let (writer, reader) = tokio::io::duplex(8192);
		let task = tokio::spawn({
			let id = id.clone();
			let server = self.clone();
			async move {
				server
					.archive_object(&id, writer)
					.await
					.inspect_err(|error| tracing::error!(?error, "an error occured while archving"))
					.ok();
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);
		Ok(reader.attach(abort_handle))
	}
}

impl Server {
	pub(crate) async fn handle_object_export_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the arg.
		let arg = request.json().await?;

		// Get the reader.
		let reader = handle.export_object(&id, arg).await?;

		// Create the response.
		let response = http::Response::builder().reader(reader).unwrap();

		Ok(response)
	}
}
