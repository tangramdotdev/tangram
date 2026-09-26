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
	pub async fn try_get_sandbox_status_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		if !arg.source.is_index()
			&& let Some(stream) = self.try_get_sandbox_status_stream_runner(id, &arg).await?
		{
			return Ok(Some(stream));
		}
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			let stopper = self.context.stopper.clone();
			if local.current {
				let mut wakeups = if arg.timeout == Some(Duration::ZERO) {
					None
				} else {
					Some(
						self.create_sandbox_status_wakeup_stream(id, stopper, arg.timeout)
							.await?,
					)
				};
				let deadline = self.server.control_read_deadline();
				let initial = loop {
					tokio::select! {
						output = self
					.try_get_sandbox_observation_local(id, arg.tokens.local_authorization(), arg.source, deadline) => break output?,
						wakeup = async {
							match &mut wakeups {
								Some(wakeups) => wakeups.next().await,
								None => std::future::pending().await,
							}
						} => {
							if wakeup.is_none() { return Ok(None); }
						},
					}
				};
				if let Some(sandbox) = initial {
					let initial = Some(sandbox);
					let stream = self.create_sandbox_status_stream_local_with_wakeups(
						id, initial, wakeups, arg.source,
					);
					return Ok(Some(stream));
				}
			}

			if let Some(status) = self
				.try_get_sandbox_status_stream_regions(
					id,
					&local.regions,
					arg.timeout,
					&arg.tokens,
					arg.source,
				)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the sandbox status from another region"),
				)? {
				return Ok(Some(status));
			}
		}

		if let Some(status) = self
			.try_get_sandbox_status_stream_remotes(
				id,
				&locations.remotes,
				arg.timeout,
				&arg.tokens,
				arg.source,
			)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to get the sandbox status from a remote"),
			)? {
			return Ok(Some(status));
		}

		Ok(None)
	}

	async fn try_get_sandbox_status_stream_runner(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let Some(runner) = self.try_get_sandbox_runner_inner(id, arg.location.as_ref()) else {
			return Ok(None);
		};
		if !self
			.authorize_sandbox_runner(
				id,
				arg.tokens.local_authorization(),
				tg::authorization::permission::sandbox::Permission::Read,
			)
			.await?
		{
			return Ok(None);
		}
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let session = self.clone();
		let id = id.clone();
		let arg_ = arg.clone();
		let task = Task::spawn(|_| async move {
			let result = session
				.try_get_sandbox_status_stream_runner_task(&id, arg_, runner, sender.clone())
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

	fn try_get_sandbox_status_stream_runner_task<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		mut arg: tg::sandbox::status::Arg,
		mut runner: crate::sandbox::Runner,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sandbox::status::Event>>,
	) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			let mut previous = None;
			loop {
				let status = self
					.server
					.runner
					.state()
					.sandboxes()
					.get(runner.index)
					.map(|sandbox| sandbox.status);
				let Some(status) = status else {
					// Resume at the owning location when the runner releases its state.
					arg.location = Some(runner.location_arg);
					let mut stream = self
						.try_get_sandbox_status_stream(id, arg)
						.boxed()
						.await?
						.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
					while let Some(event) = stream.next().await {
						let event = event?;
						if let tg::sandbox::status::Event::Status(status) = &event {
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
						.send(Ok(tg::sandbox::status::Event::Status(status)))
						.await
						.is_err()
					{
						return Ok(());
					}
					previous = Some(status);
				}
				if status.is_destroyed() || arg.timeout == Some(Duration::ZERO) {
					sender.send(Ok(tg::sandbox::status::Event::End)).await.ok();
					return Ok(());
				}
				runner.changed.changed().await.ok();
			}
		}
		.boxed()
	}

	pub(super) async fn try_get_sandbox_observation_local(
		&self,
		id: &tg::sandbox::Id,
		tokens: &[tg::authorization::Token],
		source: tg::sandbox::Source,
		deadline: tokio::time::Instant,
	) -> tg::Result<Option<tg::sandbox::Data>> {
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Read,
		);
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let authorize_future = self.authorize(resource, permission);
		let get_future = self.try_get_sandbox_data_local(id, source, deadline);
		let (permissions, output) = future::try_join(authorize_future, get_future).await?;

		if !permissions.is_some_and(|permissions| permissions.contains(permission)) {
			return Ok(None);
		}
		Ok(output)
	}

	pub(crate) fn create_sandbox_status_stream_local_with_wakeups(
		&self,
		id: &tg::sandbox::Id,
		initial: Option<tg::sandbox::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
		source: tg::sandbox::Source,
	) -> BoxStream<'static, tg::Result<tg::sandbox::status::Event>> {
		let once = wakeups.is_none();
		let mut previous = None;
		self.create_sandbox_data_stream_local(id, initial, wakeups, source)
			.flat_map(move |result| {
				let mut events = Vec::new();
				match result {
					Err(error) => events.push(Err(error)),
					Ok(data) => {
						let status = data.status;
						if previous != Some(status) {
							previous = Some(status);
							events.push(Ok(tg::sandbox::status::Event::Status(status)));
						}
						if once || status.is_destroyed() {
							events.push(Ok(tg::sandbox::status::Event::End));
						}
					},
				}
				stream::iter(events)
			})
			.boxed()
	}

	pub(super) fn create_sandbox_data_stream_local(
		&self,
		id: &tg::sandbox::Id,
		initial: Option<tg::sandbox::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
		source: tg::sandbox::Source,
	) -> BoxStream<'static, tg::Result<tg::sandbox::Data>> {
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let session = self.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = session
				.create_sandbox_data_stream_local_task(
					&id,
					sender.clone(),
					initial,
					wakeups,
					source,
				)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});
		ReceiverStream::new(receiver).attach(task).boxed()
	}

	pub(crate) async fn create_sandbox_status_wakeup_stream(
		&self,
		id: &tg::sandbox::Id,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> tg::Result<BoxStream<'static, ()>> {
		let subject = format!("sandboxes.{id}.status");
		let notifications = self
			.server
			.messenger
			.subscribe::<()>(subject)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to subscribe to the sandbox status stream")
			})?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(
			self.server.config.sandbox.status_wakeup_interval,
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

	async fn create_sandbox_data_stream_local_task(
		&self,
		id: &tg::sandbox::Id,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sandbox::Data>>,
		mut initial: Option<tg::sandbox::Data>,
		mut wakeups: Option<BoxStream<'static, ()>>,
		source: tg::sandbox::Source,
	) -> tg::Result<()> {
		loop {
			let deadline = self.server.control_read_deadline();
			let data = if let Some(initial) = initial.take() {
				Some(initial)
			} else {
				match &mut wakeups {
					None => {
						self.try_get_sandbox_data_local(id, source, deadline)
							.await?
					},
					Some(wakeups) => loop {
						tokio::select! {
							result = self.try_get_sandbox_data_local(id, source, deadline) => break result?,
							wakeup = wakeups.next() => {
								if wakeup.is_none() { return Ok(()); }
							},
						}
					},
				}
			}
			.ok_or_else(
				|| tg::error!(%id, "failed to find the sandbox while observing its status"),
			)?;
			let finished = data.status.is_destroyed();
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

	async fn try_get_sandbox_data_local(
		&self,
		id: &tg::sandbox::Id,
		source: tg::sandbox::Source,
		deadline: tokio::time::Instant,
	) -> tg::Result<Option<tg::sandbox::Data>> {
		let output = self
			.get_sandbox_state_local(
				id,
				self.get_sandbox_from_control(id),
				|_| false,
				false,
				source,
				deadline,
			)
			.boxed()
			.await?;
		if let Some(output) = output.control {
			return Ok(Some(output.data));
		}
		let Some(sandbox) = output.indexed else {
			return Ok(None);
		};
		let Some(output) = sandbox.data else {
			return Ok(None);
		};
		if !output.data.status.is_destroyed()
			&& sandbox
				.location
				.as_ref()
				.is_some_and(tg::Location::is_remote)
		{
			return Ok(None);
		}
		Ok(Some(output.data))
	}

	async fn try_get_sandbox_status_stream_regions(
		&self,
		id: &tg::sandbox::Id,
		regions: &[String],
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| {
				self.try_get_sandbox_status_stream_region(id, region, timeout, tokens, source)
			})
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
		tokens: &tg::authorization::Tokens,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::status::Arg {
			location: Some(location.clone().into()),
			source,
			timeout,
			tokens: tokens.for_location(&location),
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
		tokens: &tg::authorization::Tokens,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| {
				self.try_get_sandbox_status_stream_remote(id, remote, timeout, tokens, source)
			})
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
		tokens: &tg::authorization::Tokens,
		source: tg::sandbox::Source,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let arg = tg::sandbox::status::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			source,
			timeout,
			tokens: tokens.for_location(&location),
		};
		let Some(stream) = client
			.try_get_sandbox_status_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the sandbox status"),
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
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;

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
		let Some(stream) = self.try_get_sandbox_status_stream(&id, arg).await? else {
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
