use {
	crate::Component,
	futures::{
		FutureExt as _, StreamExt as _,
		future::{self, BoxFuture},
		stream::{self, BoxStream, FuturesUnordered},
	},
	num::ToPrimitive as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::prelude::*,
	tangram_server_core::Session,
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
};

struct Output {
	control: Option<tg::process::control::GetChildrenClientResponseOutput>,
	indexed: Option<tangram_index::process::Process>,
}

struct LocalChildren {
	children: Vec<tg::process::data::Child>,
	status: tg::process::Status,
}

struct GetProcessChildrenLocalArg<'a> {
	deadline: tokio::time::Instant,
	id: &'a tg::process::Id,
	initial: Option<Output>,
	length: u64,
	position: u64,
	source: tg::process::Source,
}

impl Component {
	pub async fn try_get_process_children_stream(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		mut arg: tg::process::children::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
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
				.try_get_process_children_runner(session, id, &arg)
				.await?
		{
			return Ok(Some(stream));
		}
		let locations = session
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(id) = &id
				&& let Some(stream) = self
					.try_get_process_children_local(session, id, arg.clone())
					.await
					.map_err(|error| tg::error!(!error, "failed to get the process children"))?
			{
				let location = tg::Location::Local(tg::location::Local::default());
				let stream = Self::update_process_children_stream_referents_for_location(
					session, stream, location, false,
				);

				return Ok(Some(stream));
			}

			if let Some(stream) = self
				.try_get_process_children_regions(session, process, arg.clone(), &local.regions)
				.await
				.map_err(|error| {
					tg::error!(
						!error,
						"failed to get the process children from another region"
					)
				})? {
				return Ok(Some(stream));
			}
		}

		if let Some(stream) = self
			.try_get_process_children_remotes(session, process, arg, &locations.remotes)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to get the process children from a remote")
			})? {
			return Ok(Some(stream));
		}

		Ok(None)
	}

	async fn try_get_process_children_runner(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: &tg::process::children::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
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
				.try_get_process_children_runner_task(&session, &id, arg_, runner, sender.clone())
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

	fn try_get_process_children_runner_task<'a>(
		&'a self,
		session: &'a Session,
		id: &'a tg::process::Id,
		mut arg: tg::process::children::get::Arg,
		mut runner: crate::Runner,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::children::get::Event>>,
	) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			let mut position = arg.position.unwrap_or(std::io::SeekFrom::Start(0));
			let default_size = session.server.config.process.default_children_page_size;
			let mut read = 0;
			loop {
				let size = arg
					.size
					.unwrap_or(default_size)
					.min(arg.length.map_or(u64::MAX, |length| length - read));
				let output = runner
					.processes
					.get(id)
					.map(|process| -> tg::Result<_> {
						let length = u64::try_from(process.children.len()).unwrap();
						let position = match position {
							std::io::SeekFrom::Current(seek) | std::io::SeekFrom::End(seek) => {
								length
									.checked_add_signed(seek)
									.ok_or_else(|| tg::error!("invalid position"))?
							},
							std::io::SeekFrom::Start(position) => position,
						};
						let start = usize::try_from(position.min(length)).unwrap();
						let end =
							usize::try_from(position.saturating_add(size).min(length)).unwrap();
						let children = process
							.children
							.get_range(start..end)
							.unwrap()
							.values()
							.map(|child| {
								let location = child
									.data
									.process
									.options
									.location
									.clone()
									.unwrap_or_else(|| runner.location.clone());
								let mut child = child.data.clone();
								child.process.options.location = Some(location);
								child
							})
							.collect::<Vec<_>>();
						Ok((position, children, process.data.status))
					})
					.transpose()?;
				let Some((start, children, status)) = output else {
					// Resume from the next unread child at the owning location.
					arg.location = Some(runner.location_arg);
					arg.position = Some(position);
					arg.length = arg.length.map(|length| length - read);
					let mut stream = self
						.try_get_process_children_stream(
							session,
							&tg::Either::Right(id.clone()),
							arg,
						)
						.boxed()
						.await?
						.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
					while let Some(event) = stream.next().await {
						if sender.send(event).await.is_err() {
							break;
						}
					}
					return Ok(());
				};
				let mut children = children;
				self.mask_process_child_tokens(session, id, &arg.tokens, &mut children)
					.await?;
				let length = u64::try_from(children.len()).unwrap();
				position = std::io::SeekFrom::Start(
					start
						.checked_add(length)
						.ok_or_else(|| tg::error!("invalid position"))?,
				);
				read += length;
				if !children.is_empty() {
					let chunk = tg::process::children::get::Chunk {
						data: children,
						position: start,
					};
					if sender
						.send(Ok(tg::process::children::get::Event::Chunk(chunk)))
						.await
						.is_err()
					{
						return Ok(());
					}
					continue;
				}
				if status.is_finished()
					|| arg.length.is_some_and(|length| read >= length)
					|| arg.timeout == Some(Duration::ZERO)
				{
					sender
						.send(Ok(tg::process::children::get::Event::End))
						.await
						.ok();
					return Ok(());
				}
				runner.changed.changed().await.ok();
			}
		}
		.boxed()
	}

	async fn try_get_process_children_local(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		let resource = tg::Referent::with_node_and_local_tokens(
			id.clone(),
			arg.tokens.local_authorization().to_vec(),
		);
		let permission = tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Node,
		);
		let permissions = session
			.authorize(resource, permission)
			.await?
			.check_exhaustion()?;
		if !permissions.permissions.contains(permission) {
			return Ok(None);
		}

		// Create the wakeups stream.
		let mut wakeups = if arg.timeout == Some(Duration::ZERO) {
			None
		} else {
			let subject = format!("processes.{id}.children");
			let children_wakeups = session
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ());
			let subject = format!("processes.{id}.status");
			let status_wakeups = session
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ());
			let wakeups = stream::select(children_wakeups, status_wakeups);
			let interval = IntervalStream::new(tokio::time::interval(
				session.server.config.process.children_wakeup_interval,
			))
			.skip(1)
			.map(|_| ());
			let wakeups = stream::select(wakeups, interval);
			let wakeups = match arg.timeout {
				Some(timeout) => wakeups.take_until(tokio::time::sleep(timeout)).boxed(),
				None => wakeups.boxed(),
			};
			Some(wakeups.with_stopper(session.context.stopper.clone()))
		};

		let position = arg.position.unwrap_or(std::io::SeekFrom::Start(0));
		let default_size = session.server.config.process.default_children_page_size;
		let size = arg
			.size
			.unwrap_or(default_size)
			.min(arg.length.unwrap_or(u64::MAX));
		let (start, length) = match position {
			std::io::SeekFrom::Start(position) => (position, size),
			std::io::SeekFrom::Current(_) | std::io::SeekFrom::End(_) => (0, 0),
		};
		let deadline = session.server.control_read_deadline();
		let output = loop {
			tokio::select! {
				output = self.get_process_children_local_inner(session, id, start, length, arg.source, deadline) => break output?,
				wakeup = async {
					match &mut wakeups {
						Some(wakeups) => wakeups.next().await,
						None => std::future::pending().await,
					}
				} => {
					if wakeup.is_none() {
						return Ok(None);
					}
				},
			}
		};
		if output.control.is_none() && output.indexed.is_none() {
			return Ok(None);
		}
		let mut arg = arg;
		let initial = match position {
			std::io::SeekFrom::Start(_) => Some(output),
			std::io::SeekFrom::Current(seek) | std::io::SeekFrom::End(seek) => {
				let length = if let Some(control) = &output.control {
					control.length
				} else {
					session
						.server
						.index
						.try_get_process_children_count(id)
						.await?
						.ok_or_else(|| tg::error!("missing the process children"))?
				};
				let position = length
					.checked_add_signed(seek)
					.ok_or_else(|| tg::error!("invalid position"))?;
				arg.position = Some(std::io::SeekFrom::Start(position));
				None
			},
		};

		// Create the channel.
		let (sender, receiver) = tokio::sync::mpsc::channel(1);

		// Spawn the task.
		let component = self.clone();
		let session = session.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = component
				.try_get_process_children_local_task(
					&session,
					&id,
					arg,
					sender.clone(),
					wakeups,
					initial,
				)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});

		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(Some(stream))
	}

	async fn get_process_children_local_inner(
		&self,
		session: &Session,
		id: &tg::process::Id,
		position: u64,
		length: u64,
		source: tg::process::Source,
		deadline: tokio::time::Instant,
	) -> tg::Result<Output> {
		// Create the index future, which reads only the process record.
		let get_index = || async {
			let indexed = self.try_get_process_from_index(session, id).await?;
			let indexed = indexed.filter(|process| process.set.children);
			Ok::<_, tg::Error>(indexed)
		};
		let index_future = async {
			let indexed = get_index().await;
			tangram_server_core::checkpoint!(session.server, "process.get.index", process = %id)
				.await;
			indexed
		}
		.boxed();

		// Create the control future.
		let control_future = self.get_process_children_from_control(session, id, position, length);
		let control_future = session
			.server
			.read_control_response_until(deadline, async {
				let output = control_future.await;
				tangram_server_core::checkpoint!(session.server, "process.get.control", process = %id).await;
				output
			})
			.boxed();

		match source {
			tg::process::Source::Auto => {},
			tg::process::Source::Index => {
				let indexed = index_future.await?;
				let output = Output {
					control: None,
					indexed,
				};
				return Ok(output);
			},
			tg::process::Source::Runner => {
				let control = control_future.await.ok();
				let output = Output {
					control,
					indexed: None,
				};
				return Ok(output);
			},
		}

		// Race the index against control, and return the indexed process if the index finds it first.
		let control = match future::select(index_future, control_future).await {
			future::Either::Left((indexed, control_future)) => {
				let indexed = indexed?;
				if indexed.is_some() {
					let output = Output {
						control: None,
						indexed,
					};
					return Ok(output);
				}
				control_future.await
			},
			future::Either::Right((control, _)) => control,
		};

		// Fall back to the index if the control read fails.
		let output = match control {
			Err(_) => {
				let indexed = get_index().await?;
				Output {
					control: None,
					indexed,
				}
			},
			Ok(control) => Output {
				control: Some(control),
				indexed: None,
			},
		};

		Ok(output)
	}

	async fn get_process_children_from_control(
		&self,
		session: &Session,
		id: &tg::process::Id,
		position: u64,
		length: u64,
	) -> tg::Result<tg::process::control::GetChildrenClientResponseOutput> {
		let request = tg::process::control::ServerRequestArg::GetChildren(
			tg::process::control::GetChildrenServerRequestArg { length, position },
		);
		let retry = session.server.config.control.request_retry.clone().into();
		let timeout = session.server.config.control.read_timeout;
		let options = tangram_server_core::control::Options { retry, timeout };
		let response = self
			.request_process_control(session, id, request, options)
			.boxed()
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the get children control request"),
			)?
			.map_err(|error| tg::error!(!error, %id, "the get children control request failed"))?;
		let output = response
			.try_unwrap_get_children()
			.map_err(|_| tg::error!("expected a get children response"))?;

		Ok(output)
	}

	async fn try_get_process_children_local_task(
		&self,
		session: &Session,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::children::get::Event>>,
		mut wakeups: Option<BoxStream<'static, ()>>,
		mut initial: Option<Output>,
	) -> tg::Result<()> {
		let std::io::SeekFrom::Start(mut position) =
			arg.position.unwrap_or(std::io::SeekFrom::Start(0))
		else {
			return Err(tg::error!(%id, "invalid position"));
		};
		let default_size = session.server.config.process.default_children_page_size;
		let size = arg.size.unwrap_or(default_size);
		let mut read = 0;

		// Send the events.
		loop {
			// Send as many data events as possible.
			let status = loop {
				// Determine the size.
				let size = match arg.length {
					None => size,
					Some(length) => size.min(length - read),
				};

				// Read the chunk.
				let deadline = session.server.control_read_deadline();
				let output = loop {
					let children_arg = GetProcessChildrenLocalArg {
						deadline,
						id,
						initial: initial.take(),
						length: size,
						position,
						source: arg.source,
					};
					tokio::select! {
						output = self.get_process_children_local(session, children_arg) => break output?,
						wakeup = async {
							match &mut wakeups {
								Some(wakeups) => wakeups.next().await,
								None => std::future::pending().await,
							}
						} => {
							if wakeup.is_none() {
								return Ok(());
							}
						},
					}
				};

				// If the chunk is empty, then break.
				if output.children.is_empty() {
					break output.status;
				}
				let mut children = output.children;
				self.mask_process_child_tokens(session, id, &arg.tokens, &mut children)
					.await?;
				let chunk = tg::process::children::get::Chunk {
					data: children,
					position,
				};

				// Update the state.
				let length = chunk.data.len().to_u64().unwrap();
				position = position
					.checked_add(length)
					.ok_or_else(|| tg::error!("invalid position"))?;
				read += length;

				// Send the data.
				let result = sender
					.send(Ok(tg::process::children::get::Event::Chunk(chunk)))
					.await;
				if result.is_err() {
					return Ok(());
				}
			};

			// If the process is finished or the length is reached, then send the end event and break.
			let end = arg.length.is_some_and(|length| read >= length);
			if end || status.is_finished() {
				let result = sender
					.send(Ok(tg::process::children::get::Event::End))
					.await;
				if result.is_err() {
					return Ok(());
				}
				break;
			}

			// Wait for an event before returning to the top of the loop.
			let Some(wakeups) = &mut wakeups else {
				sender
					.send(Ok(tg::process::children::get::Event::End))
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

	async fn get_process_children_local(
		&self,
		session: &Session,
		arg: GetProcessChildrenLocalArg<'_>,
	) -> tg::Result<LocalChildren> {
		let GetProcessChildrenLocalArg {
			deadline,
			id,
			initial,
			length,
			position,
			source,
		} = arg;
		let output = match initial {
			Some(output) => output,
			None => {
				self.get_process_children_local_inner(
					session, id, position, length, source, deadline,
				)
				.await?
			},
		};
		if let Some(output) = output.control {
			let children = output.children;
			let status = output.status;
			let output = LocalChildren { children, status };
			return Ok(output);
		}
		let process = output
			.indexed
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;

		let status = process
			.status
			.ok_or_else(|| tg::error!(%id, "missing the process data"))?;
		let children = session
			.server
			.index
			.try_get_process_children(id, std::io::SeekFrom::Start(position), length)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
		let output = LocalChildren { children, status };

		Ok(output)
	}

	async fn try_get_process_children_regions(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::children::get::Arg,
		regions: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| {
				self.try_get_process_children_region(session, process, arg.clone(), region)
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

	async fn try_get_process_children_region(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::children::get::Arg,
		region: &str,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		let client = session
			.get_region_session_for_process(region)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the region client"),
			)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let tokens = arg.tokens.for_location(&location);
		let arg = tg::process::children::get::Arg {
			location: Some(location.clone().into()),
			tokens,
			..arg
		};
		let Some(stream) = client
			.try_get_process_children_stream(process, arg)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the process children"),
			)?
		else {
			return Ok(None);
		};
		let stream = Self::update_process_children_stream_referents_for_location(
			session,
			stream.boxed(),
			location,
			false,
		);
		Ok(Some(stream))
	}

	async fn try_get_process_children_remotes(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::children::get::Arg,
		remotes: &[tangram_server_core::location::Remote],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| {
				self.try_get_process_children_remote(session, process, arg.clone(), remote)
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

	async fn try_get_process_children_remote(
		&self,
		session: &Session,
		process: &tg::Either<tg::process::Nonce, tg::process::Id>,
		arg: tg::process::children::get::Arg,
		remote: &tangram_server_core::location::Remote,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		let client = session
			.get_remote_session_for_process(&remote.name)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
			)?;
		let trusted = client.trusted();
		let location = tg::Location::Remote(tg::location::Remote {
			name: remote.name.clone(),
			region: None,
		});
		let tokens = arg.tokens.for_location(&location);
		let arg = tg::process::children::get::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			tokens,
			..arg
		};
		let Some(stream) = client
			.try_get_process_children_stream(process, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the process children"),
			)?
		else {
			return Ok(None);
		};
		let stream = Self::update_process_children_stream_referents_for_location(
			session,
			stream.boxed(),
			location,
			trusted,
		);
		Ok(Some(stream))
	}

	fn update_process_children_stream_referents_for_location(
		session: &Session,
		stream: BoxStream<'static, tg::Result<tg::process::children::get::Event>>,
		location: tg::Location,
		trusted: bool,
	) -> BoxStream<'static, tg::Result<tg::process::children::get::Event>> {
		let session = session.clone();
		stream
			.map(move |event| {
				let mut event = event?;
				if let tg::process::children::get::Event::Chunk(chunk) = &mut event {
					for child in &mut chunk.data {
						session.update_process_child_referent_for_location(
							child, &location, trusted,
						)?;
					}
				}

				Ok(event)
			})
			.boxed()
	}

	pub(crate) async fn try_get_process_children_stream_request(
		&self,
		session: &Session,
		request: http::Request<BoxBody>,
		process: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the process.
		let process = process
			.parse::<tg::Either<tg::process::Nonce, tg::process::Id>>()
			.map_err(|_| tg::error!(argument, "failed to parse the process"))?;

		// Get the query.
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
			.try_get_process_children_stream(session, &process, arg)
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
