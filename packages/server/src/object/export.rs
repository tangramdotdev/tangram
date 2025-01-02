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
			let remote = self.get_remote_client(remote).await?;
			let arg = tg::object::export::Arg { remote: None };
			let reader = remote.export_object(id, arg).await?;
			let reader = Box::pin(reader) as Pin<Box<dyn AsyncRead + Send + 'static>>;
			return Ok(reader);
		}

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

		let reader = reader.attach(abort_handle).boxed();

		Ok(reader)
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
