use {
	crate::{Context, Server},
	futures::{
		FutureExt as _, StreamExt as _,
		future::{self, BoxFuture},
		stream,
	},
	std::sync::{
		Arc,
		atomic::{AtomicBool, Ordering},
	},
	tangram_client::prelude::*,
	tangram_futures::{future::Ext as _, stream::TryExt as _, task::Stopper},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
};

impl Server {
	pub async fn try_wait_process_future_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(future) = self
				.try_wait_process_local(id)
				.await
				.map_err(|source| tg::error!(!source, %id, "failed to wait for the process"))?
		{
			return self.attach_wait_process_cancel_guard(id, arg, future).await;
		}

		// Try peers.
		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if let Some(future) = self.try_wait_process_peer(id, &peers).await.map_err(
			|source| tg::error!(!source, %id, "failed to wait for the process on the peer"),
		)? {
			return self.attach_wait_process_cancel_guard(id, arg, future).await;
		}

		// Try remotes.
		let remote_names = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		let Some(future) = self
			.try_wait_process_remote(id, &remote_names)
			.await
			.map_err(
				|source| tg::error!(!source, %id, "failed to wait for the process on the remote"),
			)?
		else {
			return Ok(None);
		};

		self.attach_wait_process_cancel_guard(id, arg, future).await
	}

	async fn attach_wait_process_cancel_guard(
		&self,
		id: &tg::process::Id,
		arg: tg::process::wait::Arg,
		future: BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		// If a token is provided, attach a cancellation guard.
		let future = if let Some(token) = arg.token.clone() {
			let cancel = Arc::new(AtomicBool::new(true));

			// Map the future to defuse on success.
			let future = {
				let cancel = cancel.clone();
				future.map(
					move |result: tg::Result<Option<tg::process::wait::Output>>| {
						if result.is_ok() {
							cancel.store(false, Ordering::SeqCst);
						}
						result
					},
				)
			}
			.boxed();

			// Create guard that cancels if not defused.
			let guard = {
				let server = self.clone();
				let id = id.clone();
				let arg = arg.clone();
				scopeguard::guard((), move |()| {
					if cancel.load(Ordering::SeqCst) {
						let arg = tg::process::cancel::Arg {
							local: arg.local,
							remotes: arg.remotes,
							token: token.clone(),
						};
						tokio::spawn(async move {
							server.cancel_process(&id, arg).await.ok();
						});
					}
				})
			};

			future.attach(guard).boxed()
		} else {
			future
		};

		Ok(Some(future))
	}

	async fn try_wait_process_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
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
		Ok(Some(future.boxed()))
	}

	async fn try_wait_process_peer(
		&self,
		id: &tg::process::Id,
		peers: &[String],
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		if peers.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::wait::Arg {
			local: None,
			remotes: None,
			token: None,
		};
		for peer in peers {
			let client = self.get_peer_client(peer.clone()).await.map_err(
				|source| tg::error!(!source, peer = %peer, "failed to get the peer client"),
			)?;
			let future = client
				.try_wait_process_future(id, arg.clone())
				.await
				.map_err(
					|source| tg::error!(!source, %id, peer = %peer, "failed to wait for the process"),
				)?
				.map(futures::FutureExt::boxed);
			if let Some(future) = future {
				return Ok(Some(future));
			}
		}
		Ok(None)
	}

	async fn try_wait_process_remote(
		&self,
		id: &tg::process::Id,
		remotes: &[String],
	) -> tg::Result<Option<BoxFuture<'static, tg::Result<Option<tg::process::wait::Output>>>>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::wait::Arg {
			local: None,
			remotes: None,
			token: None,
		};
		for remote in remotes {
			let client = self.get_remote_client(remote.clone()).await.map_err(
				|source| tg::error!(!source, remote = %remote, "failed to get the remote client"),
			)?;
			let future = client
				.try_wait_process_future(id, arg.clone())
				.await
				.map_err(
					|source| tg::error!(!source, %id, remote = %remote, "failed to wait for the process"),
				)?
				.map(futures::FutureExt::boxed);
			if let Some(future) = future {
				return Ok(Some(future));
			}
		}
		Ok(None)
	}

	pub(crate) async fn handle_post_process_wait_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
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
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
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
		let stopper = request.extensions().get::<Stopper>().cloned().unwrap();
		let stopper = async move { stopper.wait().await };
		let stream = stream.take_until(stopper);

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
