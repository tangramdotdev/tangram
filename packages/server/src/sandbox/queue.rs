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
	pub(crate) async fn try_dequeue_sandbox_with_context(
		&self,
		context: &Context,
		arg: tg::sandbox::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		if Self::local(arg.local, arg.remotes.as_ref()) {
			return self.try_dequeue_sandbox_local().await;
		}

		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if let Some(output) = self.try_dequeue_sandbox_peer(&peers).await? {
			return Ok(Some(output));
		}

		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(output) = self.try_dequeue_sandbox_remote(&remotes).await? {
			return Ok(Some(output));
		}

		Ok(None)
	}

	async fn try_dequeue_sandbox_local(&self) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		let created = self
			.messenger
			.subscribe::<()>("sandboxes.created".into(), None)
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
				Database::Postgres(register) => self.try_dequeue_sandbox_postgres(register).await?,
				#[cfg(feature = "sqlite")]
				Database::Sqlite(register) => self.try_dequeue_sandbox_sqlite(register).await?,
			};
			if let Some(output) = output {
				self.publish_sandbox_status(&output.sandbox);
				if let Some(process) = &output.process {
					self.messenger
						.publish(format!("processes.{process}.status"), ())
						.await
						.ok();
				}
				return Ok(Some(output));
			}
		}
		Ok(None)
	}

	async fn try_dequeue_sandbox_peer(
		&self,
		peers: &[String],
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		for peer in peers {
			let client = self.get_peer_client(peer.clone()).await.map_err(
				|source| tg::error!(!source, peer = %peer, "failed to get the peer client"),
			)?;
			let output = client
				.try_dequeue_sandbox(tg::sandbox::queue::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, peer = %peer, "failed to dequeue the sandbox"),
				)?;
			if output.is_some() {
				return Ok(output);
			}
		}
		Ok(None)
	}

	async fn try_dequeue_sandbox_remote(
		&self,
		remotes: &[String],
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		for remote in remotes {
			let client = self.get_remote_client(remote.clone()).await.map_err(
				|source| tg::error!(!source, remote = %remote, "failed to get the remote client"),
			)?;
			let output = client
				.try_dequeue_sandbox(tg::sandbox::queue::Arg::default())
				.await
				.map_err(
					|source| tg::error!(!source, remote = %remote, "failed to dequeue the sandbox"),
				)?;
			if output.is_some() {
				return Ok(output);
			}
		}
		Ok(None)
	}

	pub(crate) fn spawn_publish_sandboxes_created_message_task(&self) {
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish("sandboxes.created".into(), ())
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to publish the sandbox created message");
					})
					.ok();
			}
		});
	}

	pub(crate) async fn handle_dequeue_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
	) -> tg::Result<http::Response<BoxBody>> {
		let stopper = request.extensions().get::<Stopper>().cloned().unwrap();
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;
		let handle = self.clone();
		let context = context.clone();
		let future = async move { handle.try_dequeue_sandbox_with_context(&context, arg).await };
		let stream = stream::once(future).filter_map(|option| future::ready(option.transpose()));
		let stopper = async move { stopper.wait().await };
		let stream = stream.take_until(stopper);
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
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap())
	}
}
