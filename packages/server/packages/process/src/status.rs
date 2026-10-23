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
	tangram_server_core::Session,
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
};

impl Component {
	pub async fn try_get_process_status_stream(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let output = self
			.try_get_process_status_stream_inner(session, process, arg)
			.await?;

		// Let tests observe that the request reached the server before it changes what the request observes.
		if output.is_some() {
			let process = match process {
				tg::Either::Left(nonce) => nonce.to_string(),
				tg::Either::Right(id) => id.to_string(),
			};
			tangram_server_core::checkpoint!(session.server, "process.status.subscribed", process = %process).await;
		}

		Ok(output)
	}

	async fn try_get_process_status_stream_inner(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		mut arg: tg::process::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let mut process = process.clone();
		if self
			.try_resolve_process(
				session,
				&mut process,
				&mut arg.location,
				Some(&mut arg.tokens),
				&[],
			)
			.await?
			.is_none()
		{
			return Ok(None);
		}
		let id = process.clone().right();
		let process = &process;
		if !arg.source.is_index()
			&& let Some(id) = &id
			&& let Some(stream) = self
				.try_get_process_status_stream_runner(session, id, &arg)
				.await?
		{
			return Ok(Some(stream));
		}
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			let stopper = session.context.stopper.clone();
			if local.current
				&& let Some(id) = &id
			{
				let mut wakeups = if arg.timeout == Some(Duration::ZERO) {
					None
				} else {
					Some(
						self.create_process_status_wakeup_stream(session, id, stopper, arg.timeout)
							.await?,
					)
				};
				let deadline = session.server.control_read_deadline();
				let initial = loop {
					tokio::select! {
						output = self
					.try_get_process_observation_local(session,
						id,
						arg.tokens.local_authorization(),
						arg.source,
						deadline,
					) => break output?,
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
				if let Some(process) = initial {
					let initial = Some(process);
					let stream = self.create_process_status_stream_local_with_wakeups(
						session, id, initial, wakeups, arg.source,
					);
					return Ok(Some(stream));
				}
			}

			if let Some(status) = self
				.try_get_process_status_stream_regions(
					session,
					process,
					&local.regions,
					arg.timeout,
					&arg.tokens,
					arg.source,
				)
				.await
				.map_err(
					|error| tg::error!(!error, %process, "failed to get the process status from another region"),
				)? {
				return Ok(Some(status));
			}
		}

		if let Some(status) = self
			.try_get_process_status_stream_remotes(
				session,
				process,
				&locations.remotes,
				arg.timeout,
				&arg.tokens,
				arg.source,
			)
			.await
			.map_err(
				|error| tg::error!(!error, %process, "failed to get the process status from a remote"),
			)? {
			return Ok(Some(status));
		}

		Ok(None)
	}

	async fn try_get_process_status_stream_runner(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: &tg::process::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let Some(runner) = Self::try_get_process_runner_inner(session, id, arg.location.as_ref())
		else {
			return Ok(None);
		};
		if self
			.authorize_process_runner(
				session,
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
		let stopper = session.context.stopper.clone();
		let component = self.clone();
		let session = session.clone();
		let id = id.clone();
		let arg_ = arg.clone();
		let task = Task::spawn(|_| async move {
			let result = component
				.try_get_process_status_stream_runner_task(
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
		});
		let stream = ReceiverStream::new(receiver).attach(task).boxed();
		let stream = match arg.timeout.filter(|timeout| !timeout.is_zero()) {
			Some(timeout) => stream.take_until(tokio::time::sleep(timeout)).boxed(),
			None => stream,
		};
		let stream = stream.with_stopper(stopper);
		Ok(Some(stream))
	}

	fn try_get_process_status_stream_runner_task<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		mut arg: tg::process::status::Arg,
		mut runner: crate::Runner,
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
						.try_get_process_status_stream(session, &tg::Either::Right(id.clone()), arg)
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
		session: &Session,
		id: &tg::process::Id,
		tokens: &[tg::authorization::Token],
		source: tg::process::Source,
		deadline: tokio::time::Instant,
	) -> tg::Result<Option<tg::process::Data>> {
		let requested = tg::authorization::permission::process::Set::NODE;
		let output = self
			.try_get_process_observation_local_with_permissions(
				session, id, tokens, source, deadline, requested,
			)
			.await?;
		Ok(output.map(|(data, _)| data))
	}

	pub(super) async fn try_get_process_observation_local_with_permissions(
		&self,
		session: &Session,
		id: &tg::process::Id,
		tokens: &[tg::authorization::Token],
		source: tg::process::Source,
		deadline: tokio::time::Instant,
		requested: tg::authorization::permission::process::Set,
	) -> tg::Result<
		Option<(
			tg::process::Data,
			tg::authorization::permission::process::Set,
		)>,
	> {
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let required = tg::authorization::permission::Set::Process(
			tg::authorization::permission::process::Set::NODE,
		);
		let requested = tg::authorization::permission::Set::Process(requested);
		let authorize_future =
			session.authorize_batch_with_required([(resource, requested)], required, None);
		let get_future = self.try_get_process_data_local(session, id, source, deadline);
		let (mut permissions, output) = future::try_join(authorize_future, get_future).await?;
		let Some(tg::authorization::permission::Set::Process(permissions)) =
			Some(permissions.pop().unwrap().check_exhaustion()?.permissions)
		else {
			return Ok(None);
		};
		if !permissions.contains(tg::authorization::permission::process::Set::NODE) {
			return Ok(None);
		}
		Ok(output.map(|data| (data, permissions)))
	}

	fn create_process_status_stream_local_with_wakeups(
		&self,
		session: &Session,
		id: &tg::process::Id,
		initial: Option<tg::process::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
		source: tg::process::Source,
	) -> BoxStream<'static, tg::Result<tg::process::status::Event>> {
		let once = wakeups.is_none();
		let mut previous = None;
		self.create_process_data_stream_local(session, id, initial, wakeups, source)
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
		session: &Session,
		id: &tg::process::Id,
		initial: Option<tg::process::Data>,
		wakeups: Option<BoxStream<'static, ()>>,
		source: tg::process::Source,
	) -> BoxStream<'static, tg::Result<tg::process::Data>> {
		let (sender, receiver) = tokio::sync::mpsc::channel(1);
		let component = self.clone();
		let session = session.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = component
				.create_process_data_stream_local_task(
					&session,
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

	pub(super) async fn create_process_status_wakeup_stream(
		&self,
		session: &Session,
		id: &tg::process::Id,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> tg::Result<BoxStream<'static, ()>> {
		let subject = format!("processes.{id}.status");
		let notifications = session
			.server
			.messenger
			.subscribe::<()>(subject)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to subscribe to the process status stream")
			})?
			.map(|_| ());
		let interval = IntervalStream::new(tokio::time::interval(
			session.server.config.process.status_wakeup_interval,
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
		session: &Session,
		id: &tg::process::Id,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::Data>>,
		mut initial: Option<tg::process::Data>,
		mut wakeups: Option<BoxStream<'static, ()>>,
		source: tg::process::Source,
	) -> tg::Result<()> {
		loop {
			let deadline = session.server.control_read_deadline();
			let data = if let Some(initial) = initial.take() {
				Some(initial)
			} else {
				match &mut wakeups {
					None => {
						self.try_get_process_data_local(session, id, source, deadline)
							.await?
					},
					Some(wakeups) => loop {
						tokio::select! {
							result = self.try_get_process_data_local(session, id, source, deadline) => break result?,
							wakeup = wakeups.next() => {
								if wakeup.is_none() { return Ok(()); }
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
		session: &Session,
		id: &tg::process::Id,
		source: tg::process::Source,
		deadline: tokio::time::Instant,
	) -> tg::Result<Option<tg::process::Data>> {
		let output = self
			.get_process_state_local(
				session,
				id,
				self.get_process_from_control(session, id),
				|_| false,
				source,
				deadline,
			)
			.boxed()
			.await?;
		if let Some(data) = output.control {
			return Ok(Some(data));
		}
		let Some((process, data)) = output.indexed else {
			return Ok(None);
		};
		let Some(data) = data else {
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
		Ok(Some(data))
	}

	async fn try_get_process_status_stream_regions(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		regions: &[String],
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
		source: tg::process::Source,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| {
				self.try_get_process_status_stream_region(
					session, process, region, timeout, tokens, source,
				)
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

	async fn try_get_process_status_stream_region(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		region: &str,
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
		source: tg::process::Source,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let client = session
			.get_region_session_for_process(region)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the region client"),
			)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::status::Arg {
			location: Some(location.clone().into()),
			source,
			timeout,
			tokens: tokens.for_location(&location),
		};
		let Some(stream) = client
			.try_get_process_status_stream(process, arg)
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
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		remotes: &[tangram_server_core::location::Remote],
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
		source: tg::process::Source,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| {
				self.try_get_process_status_stream_remote(
					session, process, remote, timeout, tokens, source,
				)
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

	async fn try_get_process_status_stream_remote(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		remote: &tangram_server_core::location::Remote,
		timeout: Option<Duration>,
		tokens: &tg::authorization::Tokens,
		source: tg::process::Source,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let client = session
			.server
			.components
			.remote
			.get_remote_session_for_process(session, &remote.name)
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
			source,
			timeout,
			tokens: tokens.for_location(&location),
		};
		let Some(stream) = client
			.try_get_process_status_stream(process, arg)
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
		session: &Session,
		request: http::Request<BoxBody>,
		process: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the process.
		let process = process
			.parse::<tg::Either<tg::process::Nonce, tg::process::Id>>()
			.map_err(|_| tg::error!(argument, "failed to parse the process"))?;

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
			.try_get_process_status_stream(session, &process, arg)
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
