use {
	crate::Component,
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
	tangram_server_core::{Session, location::await_local_future_over_regions_remotes_futures},
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
};

/// A sandbox's data that a principal observes.
pub(super) struct Observation {
	pub data: tg::sandbox::Data,

	/// Whether the sandbox's status changed during the read, so that a stream that follows the sandbox reads it again.
	pub woken: bool,
}

/// A sandbox's data from its holder or the index.
struct DataOutput {
	data: Option<tg::sandbox::Data>,

	/// Whether the sandbox's status changed during the read, so that a stream that follows the sandbox reads it again.
	woken: bool,
}

impl Component {
	pub async fn try_get_sandbox_status_stream(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let timeout = arg.timeout;
		let output = self
			.try_get_sandbox_status_stream_inner(session, id, arg)
			.await?
			.map(|stream| {
				tangram_server_core::stream::end_after_timeout(
					stream,
					timeout,
					tg::sandbox::status::Event::End,
					|event| matches!(event, tg::sandbox::status::Event::End),
				)
			});

		Ok(output)
	}

	async fn try_get_sandbox_status_stream_inner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		if let Some(stream) = self
			.try_get_sandbox_status_stream_runner(session, id, &arg)
			.await?
		{
			return Ok(Some(stream));
		}
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		let current = locations.local.as_ref().is_some_and(|local| local.current);

		// Read the local status without a search.
		if current
			&& let Some(stream) = self
				.try_get_sandbox_status_stream_local(session, id, &arg, false)
				.await?
		{
			return Ok(Some(stream));
		}

		// Search locally while trying the regions and then the remotes.
		let local_future = async {
			if !current {
				return Ok(None);
			}
			self.try_get_sandbox_status_stream_local(session, id, &arg, true)
				.await
		};
		let regions_and_remotes_future = async {
			if let Some(local) = &locations.local
				&& let Some(status) = self
					.try_get_sandbox_status_stream_regions(
						session,
						id,
						&local.regions,
						arg.timeout,
						&arg.tokens,
					)
					.await
					.map_err(
						|error| tg::error!(!error, %id, "failed to get the sandbox status from another region"),
					)? {
				return Ok(Some(status));
			}
			self.try_get_sandbox_status_stream_remotes(
				session,
				id,
				&locations.remotes,
				arg.timeout,
				&arg.tokens,
			)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to get the sandbox status from a remote"),
			)
		};
		let stream = await_local_future_over_regions_remotes_futures(
			local_future,
			regions_and_remotes_future,
		)
		.boxed()
		.await?;

		Ok(stream)
	}

	async fn try_get_sandbox_status_stream_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::status::Arg,
		exhaustive: bool,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let stopper = session.context.stopper.clone();
		let mut wakeups = if arg.timeout == Some(Duration::ZERO) {
			None
		} else {
			Some(
				self.create_sandbox_status_wakeup_stream(session, id, stopper, arg.timeout)
					.await?,
			)
		};
		let deadline = session.server.control_read_deadline();
		let Some(observation) = self
			.try_get_sandbox_observation_local(
				session,
				id,
				arg.tokens.local_authorization(),
				deadline,
				exhaustive,
				wakeups.as_mut(),
			)
			.await?
		else {
			return Ok(None);
		};

		// Read the sandbox again in the stream if its status changed during the read and the stream follows the sandbox.
		let initial = (!(observation.woken && wakeups.is_some())).then_some(observation.data);
		let stream =
			self.create_sandbox_status_stream_local_with_wakeups(session, id, initial, wakeups);

		Ok(Some(stream))
	}

	async fn try_get_sandbox_status_stream_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let Some(runner) = self
			.try_get_sandbox_boot_runner(session, id, arg.location.as_ref())
			.await?
		else {
			return Ok(None);
		};
		if !self
			.authorize_sandbox_runner(
				session,
				id,
				arg.tokens.local_authorization(),
				tg::authorization::permission::sandbox::Permission::Node,
			)
			.await?
		{
			return Ok(None);
		}
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let id = id.clone();
		let arg_ = arg.clone();
		let task = Task::spawn({
			let component = self.clone();
			let session = session.clone();
			move |_| async move {
				let result = component
					.try_get_sandbox_status_stream_runner_task(
						&session,
						&id,
						arg_,
						runner,
						sender.clone(),
					)
					.await;
				if let Err(error) = result {
					sender.send(Err(error)).await.ok();
				}
			}
		});
		let stream = ReceiverStream::new(receiver).attach(task).boxed();
		let stream = match arg.timeout.filter(|timeout| !timeout.is_zero()) {
			Some(timeout) => stream.take_until(tokio::time::sleep(timeout)).boxed(),
			None => stream,
		};
		let stream = stream.with_stopper(session.context.stopper.clone());
		Ok(Some(stream))
	}

	fn try_get_sandbox_status_stream_runner_task<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::sandbox::Id,
		mut arg: tg::sandbox::status::Arg,
		mut runner: crate::Runner,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sandbox::status::Event>>,
	) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			let mut previous = None;
			loop {
				let status = session
					.server
					.components
					.runner
					.state()
					.sandboxes()
					.get(runner.index)
					.map(|sandbox| sandbox.status);
				let Some(status) = status else {
					// Resume at the owning location when the runner releases its state.
					arg.location = Some(runner.location_arg);
					let mut stream = self
						.try_get_sandbox_status_stream(session, id, arg)
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
		session: &Session,
		id: &tg::sandbox::Id,
		tokens: &[tg::authorization::Token],
		deadline: tokio::time::Instant,
		exhaustive: bool,
		wakeups: Option<&mut BoxStream<'static, ()>>,
	) -> tg::Result<Option<Observation>> {
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Node,
		);
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let authorize_future = session.authorize_with_exhaustive(resource, permission, exhaustive);
		let get_future = self.try_get_sandbox_data_local(session, id, deadline, wakeups);
		let (permissions, output) = future::try_join(authorize_future, get_future).await?;
		let permissions = permissions.check_exhaustion()?;

		if !permissions.permissions.contains(permission) {
			return Ok(None);
		}

		// Return nothing if the wakeups ended or the sandbox was not found.
		let Some(DataOutput {
			data: Some(data),
			woken,
		}) = output
		else {
			return Ok(None);
		};
		let observation = Observation { data, woken };

		Ok(Some(observation))
	}

	pub fn create_sandbox_status_stream_local_with_wakeups(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		initial: Option<tg::sandbox::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
	) -> BoxStream<'static, tg::Result<tg::sandbox::status::Event>> {
		let once = wakeups.is_none();
		let mut previous = None;
		self.create_sandbox_data_stream_local(session, id, initial, wakeups)
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
		session: &Session,
		id: &tg::sandbox::Id,
		initial: Option<tg::sandbox::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
	) -> BoxStream<'static, tg::Result<tg::sandbox::Data>> {
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let component = self.clone();
		let session = session.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = component
				.create_sandbox_data_stream_local_task(
					&session,
					&id,
					sender.clone(),
					initial,
					wakeups,
				)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});
		ReceiverStream::new(receiver).attach(task).boxed()
	}

	pub async fn create_sandbox_status_wakeup_stream(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> tg::Result<BoxStream<'static, ()>> {
		let subject = format!("sandboxes.{id}.status");
		let notifications = session
			.server
			.messenger
			.subscribe::<()>(subject)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to subscribe to the sandbox status stream")
			})?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(
			session.server.config.sandbox.status_wakeup_interval,
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
		session: &Session,
		id: &tg::sandbox::Id,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sandbox::Data>>,
		mut initial: Option<tg::sandbox::Data>,
		mut wakeups: Option<BoxStream<'static, ()>>,
	) -> tg::Result<()> {
		loop {
			// Read the sandbox, and stop if the wakeups ended during the read.
			let deadline = session.server.control_read_deadline();
			let (data, woken) = if let Some(initial) = initial.take() {
				(Some(initial), false)
			} else {
				let Some(output) = self
					.try_get_sandbox_data_local(session, id, deadline, wakeups.as_mut())
					.await?
				else {
					return Ok(());
				};
				(output.data, output.woken)
			};
			let data = data.ok_or_else(
				|| tg::error!(%id, "failed to find the sandbox while observing its status"),
			)?;

			// Send the data, and stop if the sandbox was destroyed or the stream does not follow the sandbox.
			let finished = data.status.is_destroyed();
			if sender.send(Ok(data)).await.is_err() || finished {
				return Ok(());
			}
			let Some(wakeups) = &mut wakeups else {
				return Ok(());
			};

			// Wait for the next wakeup, unless the sandbox's status changed during the read.
			if !woken && wakeups.next().await.is_none() {
				return Ok(());
			}
		}
	}

	/// Get a sandbox's data from its holder or the index, sending the request to the holder again on each of the given wakeups, or on the sandbox's status wakeups if none are given. Returns `None` if the wakeups end.
	async fn try_get_sandbox_data_local(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		deadline: tokio::time::Instant,
		wakeups: Option<&mut BoxStream<'static, ()>>,
	) -> tg::Result<Option<DataOutput>> {
		// Subscribe to the sandbox's status if the caller does not follow the sandbox, so that the request is sent again to a holder that starts the sandbox during the read.
		let mut subscription = None;
		let wakeups = if let Some(wakeups) = wakeups {
			wakeups
		} else {
			let wakeups = self
				.create_sandbox_status_wakeup_stream(session, id, None, None)
				.await?;
			subscription.insert(wakeups)
		};

		// Get the sandbox from its holder or the index.
		let arg = Self::get_sandbox_holder_arg(
			session,
			deadline,
			tangram_server_core::control::id(),
			true,
			wakeups,
		);
		let Some(output) = self
			.request_sandbox_holder(session, id, arg, |_| false)
			.boxed()
			.await?
		else {
			return Ok(None);
		};
		let woken = output.woken;
		let control = output
			.reply
			.and_then(Result::ok)
			.and_then(|reply| Self::try_get_sandbox_from_holder_reply(session, reply));
		let data = match (control, output.indexed) {
			(Some(output), _) => Some(output.data),
			(None, Some(sandbox)) => {
				let remote = sandbox
					.location
					.as_ref()
					.is_some_and(tg::Location::is_remote);
				sandbox
					.data
					.filter(|output| output.data.status.is_destroyed() || !remote)
					.map(|output| output.data)
			},
			(None, None) => None,
		};
		let output = DataOutput { data, woken };

		Ok(Some(output))
	}

	async fn try_get_sandbox_status_stream_regions(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		regions: &[String],
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| {
				self.try_get_sandbox_status_stream_region(session, id, region, timeout, tokens)
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
		session: &Session,
		id: &tg::sandbox::Id,
		region: &str,
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let client = session.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::sandbox::status::Arg {
			location: Some(location.clone().into()),
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
		session: &Session,
		id: &tg::sandbox::Id,
		remotes: &[tangram_server_core::location::Remote],
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| {
				self.try_get_sandbox_status_stream_remote(session, id, remote, timeout, tokens)
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
		session: &Session,
		id: &tg::sandbox::Id,
		remote: &tangram_server_core::location::Remote,
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::status::Event>>>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session(session, &remote.name)
			.await
			.map_err(
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
		session: &Session,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|error| tg::error!(argument, !error, "failed to parse the sandbox id"))?;

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
			.map_err(|error| tg::error!(argument, !error, "failed to parse the accept header"))?;

		// Get the stream.
		let Some(stream) = self
			.try_get_sandbox_status_stream(session, &id, arg)
			.await?
		else {
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
				return Err(tg::error!(argument, %type_, %subtype, "invalid accept type"));
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
