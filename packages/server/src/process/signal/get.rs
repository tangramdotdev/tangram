use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, future},
	tangram_client::prelude::*,
	tangram_futures::task::Stop,
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
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
			&& let Some(stream) = self
				.try_get_process_signal_stream_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the process signal stream"))?
		{
			return Ok(Some(stream.left_stream()));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
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
		if self
			.try_get_process_local(id, false)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the process"))?
			.is_none()
		{
			return Ok(None);
		}

		let stream = self
			.messenger
			.subscribe::<tangram_messenger::payload::Json<tg::process::signal::get::Event>>(
				format!("processes.{id}.signal"),
				None,
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?
			.map(|result| {
				let message =
					result.map_err(|source| tg::error!(!source, "failed to get message"))?;
				Ok(message.payload.0)
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
			let remote = remote.clone();
			let arg = arg.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				client
					.try_get_process_signal_stream(id, arg)
					.await
					.map_err(
						|source| tg::error!(!source, %remote, "failed to get the process signal stream"),
					)?
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
		request: http::Request<BoxBody>,
		_context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the args.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the stream.
		let Some(stream) = self.try_get_process_signal_stream(&id, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
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
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},

			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
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
