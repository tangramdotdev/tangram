use {
	crate::{Context, Server, database::Database},
	futures::{
		StreamExt as _,
		stream::{self, BoxStream, FuturesUnordered},
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
		let locations = self
			.locations_with_regions(arg.locations)
			.await
			.map_err(|source| tg::error!(!source, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(stream) =
					self.try_get_process_signal_stream_local(id)
						.await
						.map_err(|source| {
							tg::error!(!source, "failed to get the process signal stream")
						})? {
				return Ok(Some(stream));
			}

			if let Some(stream) = self
				.try_get_process_signal_stream_from_regions(id, &local.regions)
				.await
				.map_err(|source| {
					tg::error!(
						!source,
						"failed to get the process signal stream from another region"
					)
				})? {
				return Ok(Some(stream));
			}
		}

		if let Some(stream) = self
			.try_get_process_signal_stream_from_remotes(id, &locations.remotes)
			.await
			.map_err(|source| {
				tg::error!(
					!source,
					"failed to get the process signal stream from a remote"
				)
			})? {
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
		let stream = self
			.messenger
			.subscribe::<()>(subject, Some(group.into()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());
		let interval =
			IntervalStream::new(tokio::time::interval(Duration::from_secs(1))).map(|_| ());
		let stream = stream::select(stream, interval);
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

	async fn try_get_process_signal_stream_from_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_signal_stream_from_region(id, region))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(stream)) => {
					result = Ok(Some(stream));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(stream) = result? else {
			return Ok(None);
		};
		Ok(Some(stream))
	}

	async fn try_get_process_signal_stream_from_region(
		&self,
		id: &tg::process::Id,
		region: &str,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		let client = self.get_region_client(region.to_owned()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let arg = tg::process::signal::get::Arg {
			locations: tg::location::Locations {
				local: Some(tg::Either::Right(tg::location::Local {
					regions: Some(vec![region.to_owned()]),
				})),
				remotes: Some(tg::Either::Left(false)),
			},
		};
		let Some(stream) = client
			.try_get_process_signal_stream(id, arg)
			.await
			.map_err(
				|source| tg::error!(!source, region = %region, "failed to get the process signal stream"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	async fn try_get_process_signal_stream_from_remotes(
		&self,
		id: &tg::process::Id,
		remotes: &[tg::location::Remote],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_signal_stream_from_remote(id, remote))
			.collect::<FuturesUnordered<_>>();
		let mut result = Ok(None);
		while let Some(next) = futures.next().await {
			match next {
				Ok(Some(stream)) => {
					result = Ok(Some(stream));
					break;
				},
				Ok(None) => (),
				Err(source) => {
					result = Err(source);
				},
			}
		}
		let Some(stream) = result? else {
			return Ok(None);
		};
		Ok(Some(stream))
	}

	async fn try_get_process_signal_stream_from_remote(
		&self,
		id: &tg::process::Id,
		remote: &tg::location::Remote,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		let client = self
			.get_remote_client(remote.remote.clone())
			.await
			.map_err(
				|source| tg::error!(!source, remote = %remote.remote, "failed to get the remote client"),
			)?;
		let arg = tg::process::signal::get::Arg {
			locations: tg::location::Locations {
				local: match &remote.regions {
					Some(regions) => Some(tg::Either::Right(tg::location::Local {
						regions: Some(regions.clone()),
					})),
					None => Some(tg::Either::Left(true)),
				},
				remotes: Some(tg::Either::Left(false)),
			},
		};
		let Some(stream) = client.try_get_process_signal_stream(id, arg).await.map_err(
			|source| tg::error!(!source, remote = %remote.remote, "failed to get the process signal stream"),
		)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
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
