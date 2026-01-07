use {
	crate::{Context, Server},
	futures::{FutureExt as _, StreamExt as _, future, stream},
	tangram_client::prelude::*,
	tangram_futures::{stream::TryExt as _, task::Stop},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
};

impl Server {
	pub async fn try_wait_process_future_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static + use<>,
		>,
	> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(future) = self
				.try_wait_process_local(id)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to wait for the process"))?
		{
			return Ok(Some(future.left_future()));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(future) = self.try_wait_process_remote(id, &remotes).await.map_err(
			|source| tg::error!(!source, %id, "failed to wait for the process on the remote"),
		)? {
			return Ok(Some(future.right_future()));
		}

		Ok(None)
	}

	async fn try_wait_process_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static + use<>,
		>,
	> {
		let server = self.clone();
		let id = id.clone();
		let Some(stream) = server
			.try_get_process_status_stream_local(&id)
			.await
			.map_err(|source| tg::error!(!source, %id, "failed to get the process status stream"))?
			.map(futures::StreamExt::boxed)
		else {
			return Ok(None);
		};
		let future = async move {
			let stream = stream
				.take_while(|event| {
					future::ready(!matches!(event, Ok(tg::process::status::Event::End)))
				})
				.map(|event| match event {
					Ok(tg::process::status::Event::Status(status)) => Ok(status),
					Err(error) => Err(error),
					_ => unreachable!(),
				});
			let status = stream
				.try_last()
				.await?
				.ok_or_else(|| tg::error!("failed to get the status"))?;
			if !status.is_finished() {
				return Err(tg::error!("expected the process to be finished"));
			}
			let output = server
				.try_get_process_local(&id, false)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?
				.ok_or_else(|| tg::error!(%id, "failed to get the process"))?;
			let exit = output
				.data
				.exit
				.ok_or_else(|| tg::error!("expected the exit to be set"))?;
			let output = tg::process::wait::Output {
				error: output.data.error,
				exit,
				output: output.data.output,
			};
			Ok(Some(output))
		};
		Ok(Some(future))
	}

	async fn try_wait_process_remote(
		&self,
		id: &tg::process::Id,
		remotes: &[String],
	) -> tg::Result<
		Option<
			impl Future<Output = tg::Result<Option<tg::process::wait::Output>>> + Send + 'static + use<>,
		>,
	> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let id = id.clone();
		let arg = tg::process::wait::Arg {
			local: None,
			remotes: None,
		};
		let futures = remotes.iter().map(|remote| {
			let id = id.clone();
			let arg = arg.clone();
			let remote = remote.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				let output = client.wait_process(&id, arg).await.map_err(
					|source| tg::error!(!source, %id, %remote, "failed to wait for the process"),
				)?;
				Ok::<_, tg::Error>(output)
			}
			.boxed()
		});
		let Ok((output, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		Ok(Some(async move { Ok(Some(output)) }))
	}

	pub(crate) async fn handle_post_process_wait_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Parse the ID.
		let id = id
			.parse::<tg::process::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Parse the arg.
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

		// Get the future.
		let Some(future) = self
			.try_wait_process_future_with_context(context, &id, arg)
			.await?
		else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Create the stream.
		let stream = stream::once(future).filter_map(|result| async move {
			match result {
				Ok(Some(value)) => Some(Ok(tg::process::wait::Event::Output(value))),
				Ok(None) => None,
				Err(error) => Some(Err(error)),
			}
		});

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
