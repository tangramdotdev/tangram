use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, future},
	tangram_client::prelude::*,
	tangram_futures::task::Stop,
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
};

impl Server {
	pub(crate) async fn try_get_process_signal_stream_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + use<>>,
	> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(stream) = self.try_get_process_signal_stream_local(id).await?
		{
			return Ok(Some(stream.left_stream()));
		}

		// Try remotes.
		let remotes = self.remotes(arg.remotes.clone()).await?;
		if let Some(stream) = self
			.try_get_process_signal_stream_remote(id, arg.clone(), &remotes)
			.await?
		{
			return Ok(Some(stream.right_stream()));
		}

		Ok(None)
	}

	async fn try_get_process_signal_stream_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<
		Option<
			impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + 'static + use<>,
		>,
	> {
		// Check if the process exists locally.
		if self.try_get_process_local(id).await?.is_none() {
			return Ok(None);
		}

		let stream = self
			.messenger
			.subscribe(format!("processes.{id}.signal"), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?
			.map(|message| {
				serde_json::from_slice::<tg::process::signal::get::Event>(&message.payload)
					.map_err(|_| tg::error!("failed to deserialize the message"))
			})
			.boxed();

		Ok(Some(stream))
	}

	async fn try_get_process_signal_stream_remote(
		&self,
		id: &tg::process::Id,
		_arg: tg::process::signal::get::Arg,
		remotes: &[String],
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::signal::get::Event>> + Send + use<>>,
	> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::signal::get::Arg {
			local: None,
			remotes: None,
		};
		let futures = remotes.iter().map(|remote| {
			let arg = arg.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await?;
				client
					.try_get_process_signal_stream(id, arg)
					.await?
					.ok_or_else(|| tg::error!("not found"))
					.map(futures::StreamExt::boxed)
			}
			.boxed()
		});
		let Ok((stream, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(Some(stream))
	}

	pub(crate) async fn handle_get_process_signal_request(
		&self,
		request: http::Request<Body>,
		_context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Parse the ID.
		let id = id.parse()?;

		// Get the args.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Get the stream.
		let Some(stream) = self.try_get_process_signal_stream(&id, arg).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		let response = response.body(body).unwrap();

		Ok(response)
	}
}
