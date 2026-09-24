use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _,
		future::BoxFuture,
		stream::{self, BoxStream, FuturesUnordered},
	},
	num::ToPrimitive as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_index::Index as _,
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
};

type Output = crate::process::get::Output<tg::process::control::GetChildrenClientResponseOutput>;

struct LocalChildren {
	children: Vec<tg::process::data::Child>,
	status: tg::process::Status,
}

impl Session {
	pub async fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		if let Some(stream) = self.try_get_process_children_runner(id, &arg).await? {
			return Ok(Some(stream));
		}
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(stream) = self
					.try_get_process_children_local(id, arg.clone())
					.await
					.map_err(|error| tg::error!(!error, "failed to get the process children"))?
			{
				let location = tg::Location::Local(tg::location::Local::default());
				let stream = self
					.update_process_children_stream_referents_for_location(stream, location, false);

				return Ok(Some(stream));
			}

			if let Some(stream) = self
				.try_get_process_children_regions(id, arg.clone(), &local.regions)
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
			.try_get_process_children_remotes(id, arg, &locations.remotes)
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
		id: &tg::process::Id,
		arg: &tg::process::children::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
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
				.try_get_process_children_runner_task(&id, arg_, runner, sender.clone())
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

	fn try_get_process_children_runner_task<'a>(
		&'a self,
		id: &'a tg::process::Id,
		mut arg: tg::process::children::get::Arg,
		mut runner: crate::process::Runner,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::children::get::Event>>,
	) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			let mut position = arg.position.unwrap_or(std::io::SeekFrom::Start(0));
			let mut read = 0;
			loop {
				let size = arg
					.size
					.unwrap_or(256)
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
								let mut child = child.data.clone().without_location_and_tokens();
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
						.try_get_process_children_stream(id, arg)
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
		let permissions = self.authorize(resource, permission).await?;
		if !permissions.is_some_and(|permissions| permissions.contains(permission)) {
			return Ok(None);
		}

		// Create the wakeups stream.
		let wakeups = if arg.timeout == Some(Duration::ZERO) {
			None
		} else {
			let subject = format!("processes.{id}.children");
			let children_wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ());
			let subject = format!("processes.{id}.status");
			let status_wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ());
			let wakeups = stream::select(children_wakeups, status_wakeups);
			let interval = IntervalStream::new(tokio::time::interval(
				self.server.config.process.children_wakeup_interval,
			))
			.skip(1)
			.map(|_| ());
			let wakeups = stream::select(wakeups, interval);
			let wakeups = match arg.timeout {
				Some(timeout) => wakeups.take_until(tokio::time::sleep(timeout)).boxed(),
				None => wakeups.boxed(),
			};
			Some(wakeups.with_stopper(self.context.stopper.clone()))
		};

		let position = arg.position.unwrap_or(std::io::SeekFrom::Start(0));
		let size = arg.size.unwrap_or(256).min(arg.length.unwrap_or(u64::MAX));
		let (start, length) = match position {
			std::io::SeekFrom::Start(position) => (position, size),
			std::io::SeekFrom::Current(_) | std::io::SeekFrom::End(_) => (0, 0),
		};
		let output = self
			.get_process_children_local_inner(id, start, length)
			.await?;
		if output.control.is_none() && output.indexed.is_none() {
			return Ok(None);
		}
		let mut arg = arg;
		let initial = match position {
			std::io::SeekFrom::Start(_) => Some(output),
			std::io::SeekFrom::Current(seek) | std::io::SeekFrom::End(seek) => {
				if let Some(control) = &output.control {
					let position = control
						.length
						.checked_add_signed(seek)
						.ok_or_else(|| tg::error!("invalid position"))?;
					arg.position = Some(std::io::SeekFrom::Start(position));
					None
				} else {
					arg.position = Some(std::io::SeekFrom::End(seek));
					Some(output)
				}
			},
		};

		// Create the channel.
		let (sender, receiver) = tokio::sync::mpsc::channel(1);

		// Spawn the task.
		let session = self.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = session
				.try_get_process_children_local_task(&id, arg, sender.clone(), wakeups, initial)
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
		id: &tg::process::Id,
		position: u64,
		length: u64,
	) -> tg::Result<Output> {
		self.get_process_state_local(
			id,
			self.get_process_children_from_control(id, position, length),
			|_| false,
			true,
		)
		.boxed()
		.await
	}

	async fn get_process_children_from_control(
		&self,
		id: &tg::process::Id,
		position: u64,
		length: u64,
	) -> tg::Result<tg::process::control::GetChildrenClientResponseOutput> {
		let request = tg::process::control::ServerRequestArg::GetChildren(
			tg::process::control::GetChildrenServerRequestArg { length, position },
		);
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options::default(),
			timeout: Duration::from_secs(10),
		};
		let response = self
			.send_process_control_request(id, request, options)
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
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::children::get::Event>>,
		mut wakeups: Option<BoxStream<'static, ()>>,
		mut initial: Option<Output>,
	) -> tg::Result<()> {
		let mut position = arg.position.unwrap_or(std::io::SeekFrom::Start(0));

		// Create the state.
		let size = arg.size.unwrap_or(256);
		let mut output_position = match position {
			std::io::SeekFrom::Start(position) => position,
			std::io::SeekFrom::End(_) => 0,
			std::io::SeekFrom::Current(_) => unreachable!(),
		};
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
				let output = self
					.get_process_children_local(id, position, size, initial.take())
					.await?;

				// If the chunk is empty, then break.
				if output.children.is_empty() {
					break output.status;
				}
				let chunk = tg::process::children::get::Chunk {
					position: output_position,
					data: output.children,
				};

				// Update the state.
				let length = chunk.data.len().to_u64().unwrap();
				position = match position {
					std::io::SeekFrom::Start(position) => std::io::SeekFrom::Start(
						position
							.checked_add(length)
							.ok_or_else(|| tg::error!("invalid position"))?,
					),
					std::io::SeekFrom::End(position) => std::io::SeekFrom::End(
						position
							.checked_add(length.to_i64().unwrap())
							.ok_or_else(|| tg::error!("invalid position"))?,
					),
					std::io::SeekFrom::Current(_) => unreachable!(),
				};
				output_position += length;
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
		id: &tg::process::Id,
		position: std::io::SeekFrom,
		length: u64,
		initial: Option<Output>,
	) -> tg::Result<LocalChildren> {
		let output = match initial {
			Some(output) => output,
			None => match position {
				std::io::SeekFrom::Current(_) => return Err(tg::error!(%id, "invalid position")),
				std::io::SeekFrom::End(_) => {
					let indexed = self
						.try_get_process_from_index(id)
						.await?
						.filter(|process| process.set.children);
					Output {
						control: None,
						indexed,
					}
				},
				std::io::SeekFrom::Start(position) => {
					self.get_process_children_local_inner(id, position, length)
						.await?
				},
			},
		};
		if let Some(output) = output.control {
			let children = output
				.children
				.into_iter()
				.map(tg::process::data::Child::without_location_and_tokens)
				.collect();
			let status = output.status;
			let output = LocalChildren { children, status };
			return Ok(output);
		}
		let process = output
			.indexed
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;

		let status = process
			.data
			.ok_or_else(|| tg::error!(%id, "missing the process data"))?
			.status;
		let children = self
			.server
			.index
			.try_get_process_children(id, position, length)
			.await?
			.ok_or_else(|| tg::error!(%id, "failed to find the process"))?;
		let children = children
			.into_iter()
			.map(tg::process::data::Child::without_location_and_tokens)
			.collect();
		let output = LocalChildren { children, status };

		Ok(output)
	}

	async fn try_get_process_children_regions(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
		regions: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_children_region(id, arg.clone(), region))
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
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
		region: &str,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		let client = self.get_region_session_for_process(region).await.map_err(
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
			.try_get_process_children_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the process children"),
			)?
		else {
			return Ok(None);
		};
		let stream = self.update_process_children_stream_referents_for_location(
			stream.boxed(),
			location,
			false,
		);
		Ok(Some(stream))
	}

	async fn try_get_process_children_remotes(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_children_remote(id, arg.clone(), remote))
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
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		let client = self
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
			.try_get_process_children_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the process children"),
			)?
		else {
			return Ok(None);
		};
		let stream = self.update_process_children_stream_referents_for_location(
			stream.boxed(),
			location,
			trusted,
		);
		Ok(Some(stream))
	}

	fn update_process_children_stream_referents_for_location(
		&self,
		stream: BoxStream<'static, tg::Result<tg::process::children::get::Event>>,
		location: tg::Location,
		trusted: bool,
	) -> BoxStream<'static, tg::Result<tg::process::children::get::Event>> {
		let session = self.clone();
		stream
			.map(move |event| {
				let mut event = event?;
				if let tg::process::children::get::Event::Chunk(chunk) = &mut event {
					for child in &mut chunk.data {
						session.update_tokens_and_location(
							&mut child.process.options.tokens,
							Some(&mut child.process.options.location),
							&location,
							trusted,
						)?;
					}
				}

				Ok(event)
			})
			.boxed()
	}

	pub(crate) async fn try_get_process_children_stream_request(
		&self,
		request: http::Request<BoxBody>,
		id: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|error| tg::error!(!error, "failed to parse the process id"))?;

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
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;

		// Get the stream.
		let Some(stream) = self.try_get_process_children_stream(&id, arg).await? else {
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
