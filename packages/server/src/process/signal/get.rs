use {
	crate::{Context, Server, database::Database},
	futures::{
		StreamExt as _,
		stream::{self, BoxStream},
	},
	std::{pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_futures::{
		stream::Ext as _,
		task::{Stopper, Task},
	},
	tangram_http::{
		body::Boxed as BoxBody,
		request::Ext as _,
		response::{Ext as _, builder::Ext as _},
	},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

impl Server {
	pub(crate) async fn try_get_process_signal_stream_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(stream) = self
				.try_get_process_signal_stream_local(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the process signal stream"))?
		{
			return Ok(Some(stream));
		}

		// Try peers.
		let peers = self
			.peers(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the peers"))?;
		if let Some(stream) = self
			.try_get_process_signal_stream_peer(id, arg.clone(), &peers)
			.await?
		{
			return Ok(Some(stream));
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
			return Ok(Some(stream));
		}

		Ok(None)
	}

	async fn try_get_process_signal_stream_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		// Verify the process is local.
		if !self
			.get_process_exists_local(id)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the process exists"))?
		{
			return Ok(None);
		}

		// Create the channel.
		let (sender, receiver) = async_channel::unbounded();

		// Spawn the task.
		let server = self.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = server
				.try_get_process_signal_stream_local_task(&id, sender.clone())
				.await;
			if let Err(error) = result {
				sender.try_send(Err(error)).ok();
			}
		});

		let stream = receiver.attach(task).boxed();

		Ok(Some(stream))
	}

	async fn try_get_process_signal_stream_local_task(
		&self,
		id: &tg::process::Id,
		sender: async_channel::Sender<tg::Result<tg::process::signal::get::Event>>,
	) -> tg::Result<()> {
		let subject = format!("processes.{id}.signal");
		let group = "processes.signal.dequeue";
		let wakeup = self
			.messenger
			.subscribe::<()>(subject, Some(group.into()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());
		let interval =
			IntervalStream::new(tokio::time::interval(Duration::from_secs(1))).map(|_| ());
		let stream = stream::select(wakeup, interval);
		let mut stream = pin!(stream);
		while let Some(()) = stream.next().await {
			loop {
				let signal = match self.try_dequeue_process_signal(id).await {
					Ok(Some(signal)) => signal,
					Ok(None) => break,
					Err(error) => {
						tracing::error!(
							error = %error.trace(),
							%id,
							"failed to dequeue the process signal"
						);
						tokio::time::sleep(Duration::from_secs(1)).await;
						break;
					},
				};
				let result = sender.try_send(Ok(tg::process::signal::get::Event::Signal(signal)));
				if result.is_err() {
					return Ok(());
				}
			}
		}
		Ok(())
	}

	async fn try_dequeue_process_signal(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Signal>> {
		match &self.sandbox_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(sandbox_store) => {
				self.try_dequeue_process_signal_postgres(sandbox_store, id)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(sandbox_store) => {
				self.try_dequeue_process_signal_sqlite(sandbox_store, id)
					.await
			},
		}
	}

	async fn try_get_process_signal_stream_peer(
		&self,
		id: &tg::process::Id,
		_arg: tg::process::signal::get::Arg,
		peers: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		if peers.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::signal::get::Arg {
			local: None,
			remotes: None,
		};
		let mut error = None;
		let mut stream = None;
		for peer in peers {
			let client = match self.get_peer_client(peer.clone()).await {
				Ok(client) => client,
				Err(source) => {
					error.replace(tg::error!(
						!source,
						peer = %peer,
						"failed to get the peer client"
					));
					continue;
				},
			};
			match client.try_get_process_signal_stream(id, arg.clone()).await {
				Ok(Some(peer_stream)) => {
					stream.replace(peer_stream.boxed());
					break;
				},
				Ok(None) => (),
				Err(source) => {
					error.replace(tg::error!(
						!source,
						peer = %peer,
						"failed to get the process signal stream"
					));
				},
			}
		}
		if let Some(stream) = stream {
			return Ok(Some(stream));
		}
		if let Some(error) = error {
			return Err(error);
		}
		Ok(None)
	}

	async fn try_get_process_signal_stream_remote(
		&self,
		id: &tg::process::Id,
		_arg: tg::process::signal::get::Arg,
		remotes: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::signal::get::Arg {
			local: None,
			remotes: None,
		};
		let mut error = None;
		let mut stream = None;
		for remote in remotes {
			let client = match self.get_remote_client(remote.clone()).await {
				Ok(client) => client,
				Err(source) => {
					error.replace(tg::error!(
						!source,
						remote = %remote,
						"failed to get the remote client"
					));
					continue;
				},
			};
			match client.try_get_process_signal_stream(id, arg.clone()).await {
				Ok(Some(remote_stream)) => {
					stream.replace(remote_stream.boxed());
					break;
				},
				Ok(None) => (),
				Err(source) => {
					error.replace(tg::error!(
						!source,
						remote = %remote,
						"failed to get the process signal stream"
					));
				},
			}
		}
		if let Some(stream) = stream {
			return Ok(Some(stream));
		}
		if let Some(error) = error {
			return Err(error);
		}
		Ok(None)
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
