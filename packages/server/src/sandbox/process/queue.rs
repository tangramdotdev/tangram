use {
	crate::{Context, Server, database::Database},
	futures::{StreamExt as _, future, stream},
	std::{pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::task::Stopper,
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub(crate) async fn try_dequeue_sandbox_process_with_context(
		&self,
		context: &Context,
		sandbox: &tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		if Self::local(arg.local, arg.remotes.as_ref())
			&& self
				.get_sandbox_exists_local(sandbox)
				.await
				.map_err(|source| tg::error!(!source, %sandbox, "failed to get the sandbox"))?
		{
			return self.try_dequeue_sandbox_process_local(sandbox).await;
		}

		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if let Some(output) = self
			.try_dequeue_sandbox_process_peer(sandbox, &peers)
			.await?
		{
			return Ok(Some(output));
		}

		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(output) = self
			.try_dequeue_sandbox_process_remote(sandbox, &remotes)
			.await?
		{
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_dequeue_sandbox_process_local(
		&self,
		sandbox: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		let subject = format!("sandboxes.{sandbox}.processes.created");
		let created = self
			.messenger
			.subscribe::<()>(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());
		let interval = Duration::from_secs(1);
		let interval = IntervalStream::new(tokio::time::interval(interval)).map(|_| ());
		let stream = stream::select(created, interval);
		let mut stream = pin!(stream);
		while let Some(()) = stream.next().await {
			let output = match &self.register {
				#[cfg(feature = "postgres")]
				Database::Postgres(register) => {
					self.try_dequeue_sandbox_process_postgres(register, sandbox)
						.await?
				},
				#[cfg(feature = "sqlite")]
				Database::Sqlite(register) => {
					self.try_dequeue_sandbox_process_sqlite(register, sandbox)
						.await?
				},
			};
			if let Some(output) = output {
				self.messenger
					.publish(format!("processes.{}.status", output.process), ())
					.await
					.ok();
				return Ok(Some(output));
			}
		}
		Ok(None)
	}

	async fn try_dequeue_sandbox_process_peer(
		&self,
		sandbox: &tg::sandbox::Id,
		peers: &[String],
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		for peer in peers {
			let client = self.get_peer_client(peer.clone()).await.map_err(
				|source| tg::error!(!source, %sandbox, peer = %peer, "failed to get the peer client"),
			)?;
			let output = client
				.try_get_sandbox(sandbox, tg::sandbox::get::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %sandbox, peer = %peer, "failed to get the sandbox"),
				)?;
			if output.is_none() {
				continue;
			}
			let output = client
				.try_dequeue_sandbox_process(sandbox, tg::sandbox::process::queue::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %sandbox, peer = %peer, "failed to dequeue the process"),
				)?;
			if output.is_some() {
				return Ok(output);
			}
		}
		Ok(None)
	}

	async fn try_dequeue_sandbox_process_remote(
		&self,
		sandbox: &tg::sandbox::Id,
		remotes: &[String],
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		for remote in remotes {
			let client = self.get_remote_client(remote.clone()).await.map_err(
				|source| tg::error!(!source, %sandbox, remote = %remote, "failed to get the remote client"),
			)?;
			let output = client
				.try_get_sandbox(sandbox, tg::sandbox::get::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %sandbox, remote = %remote, "failed to get the sandbox"),
				)?;
			if output.is_none() {
				continue;
			}
			let output = client
				.try_dequeue_sandbox_process(sandbox, tg::sandbox::process::queue::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, %sandbox, remote = %remote, "failed to dequeue the process"),
				)?;
			if output.is_some() {
				return Ok(output);
			}
		}
		Ok(None)
	}

	pub(crate) fn spawn_publish_sandbox_processes_created_message_task(
		&self,
		sandbox: &tg::sandbox::Id,
	) {
		let subject = format!("sandboxes.{sandbox}.processes.created");
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish(subject, ())
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to publish the sandbox process created message");
					})
					.ok();
			}
		});
	}

	pub(crate) async fn handle_dequeue_sandbox_process_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		sandbox: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let stopper = request.extensions().get::<Stopper>().cloned().unwrap();
		let sandbox = sandbox
			.parse::<tg::sandbox::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the arg.
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Get the stream.
		let handle = self.clone();
		let context = context.clone();
		let future = async move {
			handle
				.try_dequeue_sandbox_process_with_context(&context, &sandbox, arg)
				.await
		};
		let stream = stream::once(future).filter_map(|option| future::ready(option.transpose()));

		// Stop the stream when the server stops.
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
