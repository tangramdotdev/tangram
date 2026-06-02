use {
	crate::{Session, context::Authentication, database::Database},
	futures::{
		StreamExt as _,
		stream::{self, BoxStream, FuturesUnordered},
	},
	std::time::Duration,
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
#[cfg(feature = "turso")]
mod turso;

impl Session {
	pub(crate) async fn try_get_process_signal_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::signal::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(stream) = self
					.try_get_process_signal_stream_local(id, arg.timeout)
					.await
					.map_err(|error| {
						tg::error!(!error, "failed to get the process signal stream")
					})? {
				return Ok(Some(stream));
			}

			if let Some(stream) = self
				.try_get_process_signal_stream_regions(id, &local.regions, arg.timeout)
				.await
				.map_err(|error| {
					tg::error!(
						!error,
						"failed to get the process signal stream from another region"
					)
				})? {
				return Ok(Some(stream));
			}
		}

		if let Some(stream) = self
			.try_get_process_signal_stream_remotes(id, &locations.remotes, arg.timeout)
			.await
			.map_err(|error| {
				tg::error!(
					!error,
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
		timeout: Option<Duration>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		if !matches!(
			self.context.authentication.as_ref(),
			Some(Authentication::Process(process)) if process.id == *id
		) {
			return Err(tg::error!("unauthorized"));
		}

		// Verify the process is local.
		let Some(_output) = self
			.try_get_process_local(id, false)
			.await
			.map_err(|error| tg::error!(!error, "failed to get the process"))?
		else {
			return Ok(None);
		};

		// Create the channel.
		let (sender, receiver) = async_channel::unbounded();

		// Spawn the task.
		let session = self.clone();
		let id = id.clone();
		let stopper = self.context.stopper.clone();
		let task = Task::spawn(|_| async move {
			let result = session
				.try_get_process_signal_stream_local_task(&id, sender.clone(), stopper, timeout)
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
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> tg::Result<()> {
		let mut wakeups = if timeout == Some(Duration::ZERO) {
			None
		} else {
			let subject = format!("processes.{id}.signal");
			let wakeups = self
				.server
				.messenger
				.subscribe_with_delivery::<()>(subject, Delivery::One)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ());
			let interval = IntervalStream::new(tokio::time::interval(Duration::from_secs(1)))
				.skip(1)
				.map(|_| ());
			let wakeups = stream::select(wakeups, interval);
			let wakeups = match timeout {
				Some(timeout) => wakeups.take_until(tokio::time::sleep(timeout)).boxed(),
				None => wakeups.boxed(),
			};
			Some(wakeups.with_stopper(stopper))
		};
		loop {
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
			let Some(wakeups) = &mut wakeups else {
				sender
					.send(Ok(tg::process::signal::get::Event::End))
					.await
					.ok();
				break;
			};
			if wakeups.next().await.is_none() {
				break;
			}
		}
		Ok(())
	}

	async fn try_dequeue_process_signal(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Signal>> {
		match &self.server.process_store {
			#[cfg(feature = "postgres")]
			Database::Postgres(process_store) => {
				self.try_dequeue_process_signal_postgres(process_store, id)
					.await
			},
			#[cfg(feature = "sqlite")]
			Database::Sqlite(process_store) => {
				self.try_dequeue_process_signal_sqlite(process_store, id)
					.await
			},
			#[cfg(feature = "turso")]
			Database::Turso(process_store) => {
				self.try_dequeue_process_signal_turso(process_store, id)
					.await
			},
		}
	}

	async fn try_get_process_signal_stream_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
		timeout: Option<Duration>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_signal_stream_region(id, region, timeout))
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

	async fn try_get_process_signal_stream_region(
		&self,
		id: &tg::process::Id,
		region: &str,
		timeout: Option<Duration>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::signal::get::Arg {
			location: Some(location.into()),
			timeout,
		};
		let Some(stream) = client
			.try_get_process_signal_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the process signal stream"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	async fn try_get_process_signal_stream_remotes(
		&self,
		id: &tg::process::Id,
		remotes: &[crate::location::Remote],
		timeout: Option<Duration>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_signal_stream_remote(id, remote, timeout))
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

	async fn try_get_process_signal_stream_remote(
		&self,
		id: &tg::process::Id,
		remote: &crate::location::Remote,
		timeout: Option<Duration>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::signal::get::Event>>>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::process::signal::get::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			timeout,
		};
		let Some(stream) = client
			.try_get_process_signal_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the process signal stream"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	pub(crate) async fn try_get_process_signal_stream_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

		// Get the args.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the stream.
		let Some(stream) = self.try_get_process_signal_stream(&id, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};

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
