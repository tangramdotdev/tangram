use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _,
		future::{self, BoxFuture},
		stream::{self, BoxStream, FuturesUnordered},
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
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
};

impl Session {
	pub async fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		if let Some(stream) = self.try_get_process_status_stream_runner(id, &arg).await? {
			return Ok(Some(stream));
		}
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			let stopper = self.context.stopper.clone();
			if local.current {
				let wakeups = if arg.timeout == Some(Duration::ZERO) {
					None
				} else {
					Some(
						self.create_process_status_wakeup_stream(id, stopper, arg.timeout)
							.await?,
					)
				};
				if let Some(process) = self
					.try_get_process_observation_local(id, arg.tokens.local_authorization())
					.await?
				{
					let initial = Some(process);
					let stream =
						self.create_process_status_stream_local_with_wakeups(id, initial, wakeups);
					return Ok(Some(stream));
				}
			}

			if let Some(status) = self
				.try_get_process_status_stream_regions(id, &local.regions, arg.timeout, &arg.tokens)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the process status from another region"),
				)? {
				return Ok(Some(status));
			}
		}

		if let Some(status) = self
			.try_get_process_status_stream_remotes(id, &locations.remotes, arg.timeout, &arg.tokens)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to get the process status from a remote"),
			)? {
			return Ok(Some(status));
		}

		Ok(None)
	}

	async fn try_get_process_status_stream_runner(
		&self,
		id: &tg::process::Id,
		arg: &tg::process::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let Some(runner) = self.try_get_process_runner_inner(id, arg.location.as_ref()) else {
			return Ok(None);
		};
		if self
			.authorize_process_runner(
				id,
				&arg.tokens,
				tg::authorization::permission::process::Set::NODE,
			)
			.await?
			.is_none()
		{
			return Ok(None);
		}
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let session = self.clone();
		let id = id.clone();
		let arg_ = arg.clone();
		let task = Task::spawn(|_| async move {
			let result = session
				.try_get_process_status_stream_runner_task(&id, arg_, runner, sender.clone())
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});
		let stream = ReceiverStream::new(receiver).attach(task).boxed();
		let stream = match arg.timeout.filter(|timeout| !timeout.is_zero()) {
			Some(timeout) => stream.take_until(tokio::time::sleep(timeout)).boxed(),
			None => stream,
		};
		let stream = stream.with_stopper(self.context.stopper.clone());
		Ok(Some(stream))
	}

	fn try_get_process_status_stream_runner_task<'a>(
		&'a self,
		id: &'a tg::process::Id,
		mut arg: tg::process::status::Arg,
		mut runner: crate::process::Runner,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::status::Event>>,
	) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			let mut previous = None;
			loop {
				let status = runner.processes.get(id).map(|process| process.data.status);
				let Some(status) = status else {
					// Resume at the owning location when the runner releases its state.
					arg.location = Some(runner.location_arg);
					let mut stream = self
						.try_get_process_status_stream(id, arg)
						.boxed()
						.await?
						.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
					while let Some(event) = stream.next().await {
						let event = event?;
						if let tg::process::status::Event::Status(status) = &event {
							if previous == Some(*status) {
								continue;
							}
							previous = Some(*status);
						}
						if sender.send(Ok(event)).await.is_err() {
							break;
						}
					}
					return Ok(());
				};
				if previous != Some(status) {
					if sender
						.send(Ok(tg::process::status::Event::Status(status)))
						.await
						.is_err()
					{
						return Ok(());
					}
					previous = Some(status);
				}
				if status.is_finished() || arg.timeout == Some(Duration::ZERO) {
					sender.send(Ok(tg::process::status::Event::End)).await.ok();
					return Ok(());
				}
				runner.changed.changed().await.ok();
			}
		}
		.boxed()
	}

	pub(super) async fn try_get_process_observation_local(
		&self,
		id: &tg::process::Id,
		tokens: &[tg::authorization::Token],
	) -> tg::Result<Option<tg::process::Data>> {
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		let authorize_future = self.authorize(resource, permission);
		let get_future = self.try_get_process_data_local(id);
		let (permissions, output) = future::try_join(authorize_future, get_future).await?;

		if !permissions.is_some_and(|permissions| permissions.contains(permission)) {
			return Ok(None);
		}
		Ok(output)
	}

	fn create_process_status_stream_local_with_wakeups(
		&self,
		id: &tg::process::Id,
		initial: Option<tg::process::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
	) -> BoxStream<'static, tg::Result<tg::process::status::Event>> {
		let once = wakeups.is_none();
		let mut previous = None;
		self.create_process_data_stream_local(id, initial, wakeups)
			.flat_map(move |result| {
				let mut events = Vec::new();
				match result {
					Err(error) => events.push(Err(error)),
					Ok(data) => {
						let status = data.status;
						if previous != Some(status) {
							previous = Some(status);
							events.push(Ok(tg::process::status::Event::Status(status)));
						}
						if once || status.is_finished() {
							events.push(Ok(tg::process::status::Event::End));
						}
					},
				}
				stream::iter(events)
			})
			.boxed()
	}

	pub(super) fn create_process_data_stream_local(
		&self,
		id: &tg::process::Id,
		initial: Option<tg::process::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
	) -> BoxStream<'static, tg::Result<tg::process::Data>> {
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let session = self.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = session
				.create_process_data_stream_local_task(&id, sender.clone(), initial, wakeups)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});
		ReceiverStream::new(receiver).attach(task).boxed()
	}

	pub(super) async fn create_process_status_wakeup_stream(
		&self,
		id: &tg::process::Id,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> tg::Result<BoxStream<'static, ()>> {
		let subject = format!("processes.{id}.status");
		let notifications = self
			.server
			.messenger
			.subscribe::<()>(subject)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to subscribe to the process status stream")
			})?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(
			self.server.config.process.status_wakeup_interval,
		))
		.skip(1)
		.map(|_| ());
		let wakeups = stream::select(notifications, interval);
		let wakeups = match timeout {
			Some(timeout) => wakeups.take_until(tokio::time::sleep(timeout)).boxed(),
			None => wakeups.boxed(),
		};
		let wakeups = wakeups.with_stopper(stopper);

		Ok(wakeups)
	}

	async fn create_process_data_stream_local_task(
		&self,
		id: &tg::process::Id,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::Data>>,
		mut initial: Option<tg::process::Data>,
		mut wakeups: Option<BoxStream<'static, ()>>,
	) -> tg::Result<()> {
		loop {
			let data = if let Some(initial) = initial.take() {
				Some(initial)
			} else {
				match &mut wakeups {
					None => self.try_get_process_data_local(id).await?,
					Some(wakeups) => {
						tokio::select! {
							result = self.try_get_process_data_local(id) => result?,
							wakeup = wakeups.next() => {
								if wakeup.is_none() { return Ok(()); }
								continue;
							},
						}
					},
				}
			}
			.ok_or_else(
				|| tg::error!(%id, "failed to find the process while observing its status"),
			)?;
			let finished = data.status.is_finished();
			if sender.send(Ok(data)).await.is_err() || finished {
				return Ok(());
			}
			let Some(wakeups) = &mut wakeups else {
				return Ok(());
			};
			if wakeups.next().await.is_none() {
				return Ok(());
			}
		}
	}

	async fn try_get_process_data_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Data>> {
		let output = self
			.get_process_state_local(id, self.get_process_from_control(id), |_| false, false)
			.boxed()
			.await?;
		if let Some(data) = output.control {
			return Ok(Some(data.without_location_and_tokens()));
		}
		let Some(process) = output.indexed else {
			return Ok(None);
		};
		let Some(data) = process.data else {
			return Ok(None);
		};
		if !data.status.is_finished()
			&& process
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		{
			return Ok(None);
		}
		let data = data.without_location_and_tokens();
		Ok(Some(data))
	}

	async fn try_get_process_status_stream_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_status_stream_region(id, region, timeout, tokens))
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

	async fn try_get_process_status_stream_region(
		&self,
		id: &tg::process::Id,
		region: &str,
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let client = self.get_region_session_for_process(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::status::Arg {
			location: Some(location.clone().into()),
			timeout,
			tokens: tokens.for_location(&location),
		};
		let Some(stream) = client
			.try_get_process_status_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the process status"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	async fn try_get_process_status_stream_remotes(
		&self,
		id: &tg::process::Id,
		remotes: &[crate::location::Remote],
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_status_stream_remote(id, remote, timeout, tokens))
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

	async fn try_get_process_status_stream_remote(
		&self,
		id: &tg::process::Id,
		remote: &crate::location::Remote,
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let client = self
			.get_remote_session_for_process(&remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let arg = tg::process::status::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			timeout,
			tokens: tokens.for_location(&location),
		};
		let Some(stream) = client
			.try_get_process_status_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the process status"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	pub(crate) async fn try_get_process_status_stream_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

		// Parse the arg.
		let (arg, request) = request
			.arg()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the arg"))?;
		let arg = arg.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the stream.
		let Some(stream) = self.try_get_process_status_stream(&id, arg).await? else {
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
