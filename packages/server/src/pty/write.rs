use crate::Server;
use futures::{Stream, StreamExt as _, future, stream::TryStreamExt as _};
use std::pin::pin;
use tangram_client as tg;
use tangram_futures::{stream::Ext as _, task::Stop};
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};

impl Server {
	pub async fn write_pty(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::write::Arg,
		stream: impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static,
	) -> tg::Result<()> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote.clone()).await?;
			return remote.write_pty(id, arg, stream.boxed()).await;
		}

		let mut stream = pin!(stream);
		while let Some(event) = stream.try_next().await? {
			self.write_pty_event(id, event.clone(), arg.master).await?;
			if matches!(event, tg::pty::Event::End) {
				break;
			}
		}

		Ok(())
	}

	pub(crate) async fn handle_write_pty_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move {
			stop.wait().await;
		};

		// Create the stream.
		let stream = request
			.sse()
			.map(|event| match event {
				Ok(e) => e.try_into(),
				Err(source) => Err(tg::error!(!source, "sse error")),
			})
			.take_while_inclusive(|event| future::ready(!matches!(event, Ok(tg::pty::Event::End))))
			.take_until(stop)
			.boxed();

		handle.write_pty(&id, arg, stream).await?;

		// Create the response.
		let response = http::Response::builder().empty().unwrap();

		Ok(response)
	}
}
