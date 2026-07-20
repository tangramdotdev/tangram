use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _, future,
		stream::{BoxStream, FuturesUnordered},
	},
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_futures::{
		stream::Ext as _,
		task::{Stopper, Task},
	},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

impl Session {
	pub async fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<
		Option<
			impl futures::Stream<Item = tg::Result<tg::sandbox::status::Event>> + Send + 'static + use<>,
		>,
	> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			let stopper = self.context.stopper.clone();
			if local.current {
				let check_future = async {
					self.try_get_sandbox_local(id)
						.await
						.map(|output| output.is_some())
				}
				.boxed();
				let create_future = self
					.create_sandbox_status_stream_local(id, stopper, arg.timeout)
					.boxed();
				let stream = match future::select(check_future, create_future).await {
					future::Either::Left((checked, create_future)) => {
						if checked? {
							Some(create_future.await)
						} else {
							None
						}
					},
					future::Either::Right((stream, check_future)) => {
						if check_future.await? {
							Some(stream)
						} else {
							None
						}
					},
				};
				if let Some(stream) = stream {
					let stream = stream.map_err(
						|error| tg::error!(!error, %id, "failed to get the sandbox status stream"),
					)?;
					return Ok(Some(stream));
				}
			}

			if let Some(status) = self
				.try_get_sandbox_status_stream_regions(id, &local.regions, arg.timeout)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the sandbox status from another region"),
				)? {
				return Ok(Some(status));
			}
		}

		if let Some(status) = self
			.try_get_sandbox_status_stream_remotes(id, &locations.remotes, arg.timeout)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to get the sandbox status from a remote"),
			)? {
			return Ok(Some(status));
		}

		Ok(None)
	}

	pub(crate) async fn create_sandbox_status_stream_local(
		&self,
		id: &tg::sandbox::Id,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>> {
		// Create the wakeups stream.
		let wakeups = if timeout == Some(Duration::ZERO) {
			None
		} else {
			let subject = format!("sandboxes.{id}.status");
			let wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ());
			let wakeups = match timeout {
				Some(timeout) => wakeups.take_until(tokio::time::sleep(timeout)).boxed(),
				None => wakeups.boxed(),
			};
			Some(wakeups.with_stopper(stopper))
		};

		// Create the channel.
		let (sender, receiver) = tokio::sync::mpsc::channel(1);

		// Spawn the task.
		let session = self.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = session
				.try_get_sandbox_status_stream_local_task(&id, sender.clone(), wakeups)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});

		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(stream)
	}

	async fn try_get_sandbox_status_stream_local_task(
		&self,
		id: &tg::sandbox::Id,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sandbox::status::Event>>,
		mut wakeups: Option<BoxStream<'static, ()>>,
	) -> tg::Result<()> {
		let mut previous: Option<tg::sandbox::Status> = None;
		loop {
			let status = self
				.try_get_sandbox_status_local(id)
				.await?
				.unwrap_or(tg::sandbox::Status::Destroyed);
			if previous != Some(status) {
				previous.replace(status);
				let event = tg::sandbox::status::Event::Status(status);
				if sender.send(Ok(event)).await.is_err() {
					return Ok(());
				}
			}
			if status.is_destroyed() {
				sender.send(Ok(tg::sandbox::status::Event::End)).await.ok();
				return Ok(());
			}
			let Some(wakeups) = &mut wakeups else {
				sender.send(Ok(tg::sandbox::status::Event::End)).await.ok();
				return Ok(());
			};
			if wakeups.next().await.is_none() {
				return Ok(());
			}
		}
	}

	pub(crate) async fn try_get_sandbox_status_local(
		&self,
		id: &tg::sandbox::Id,
	) -> tg::Result<Option<tg::sandbox::Status>> {
		let output = self.try_get_sandbox_local_inner(id).await?;
		Ok(output.map(|output| output.status))
	}

	async fn try_get_sandbox_status_stream_regions(
		&self,
		id: &tg::sandbox::Id,
		regions: &[String],
		timeout: Option<Duration>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_sandbox_status_stream_region(id, region, timeout))
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

	async fn try_get_sandbox_status_stream_region(
		&self,
		id: &tg::sandbox::Id,
		region: &str,
		timeout: Option<Duration>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::status::Arg {
			location: Some(location.into()),
			timeout,
		};
		let Some(stream) = client
			.try_get_sandbox_status_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the sandbox status"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	async fn try_get_sandbox_status_stream_remotes(
		&self,
		id: &tg::sandbox::Id,
		remotes: &[crate::location::Remote],
		timeout: Option<Duration>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_sandbox_status_stream_remote(id, remote, timeout))
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

	async fn try_get_sandbox_status_stream_remote(
		&self,
		id: &tg::sandbox::Id,
		remote: &crate::location::Remote,
		timeout: Option<Duration>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::status::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			timeout,
		};
		let Some(stream) = client
			.try_get_sandbox_status_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, %id, remote = %remote.name, "failed to get the sandbox status"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	pub(crate) async fn try_get_sandbox_status_stream_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;
		let arg = request
			.query_params()
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the query params"))?
			.unwrap_or_default();
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let Some(stream) = self.try_get_sandbox_status_stream(&id, arg).await? else {
			return Ok(http::Response::builder()
				.not_found()
				.empty()
				.unwrap()
				.boxed_body());
		};
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
