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
	tangram_index::prelude::*,
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::{IntervalStream, ReceiverStream},
};

type Output = crate::sandbox::get::Output<tg::sandbox::control::GetProcessesClientResponseOutput>;

struct LocalProcesses {
	processes: Vec<tg::process::Id>,
	status: tg::sandbox::Status,
}

impl Session {
	pub async fn try_get_sandbox_processes_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>> {
		if !arg.source.is_index()
			&& let Some(stream) = self.try_get_sandbox_processes_runner(id, &arg).await?
		{
			return Ok(Some(stream));
		}
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			if local.current
				&& let Some(stream) = self
					.try_get_sandbox_processes_local(id, arg.clone())
					.await
					.map_err(|error| tg::error!(!error, "failed to get the sandbox processes"))?
			{
				return Ok(Some(stream));
			}

			if let Some(stream) = self
				.try_get_sandbox_processes_regions(id, arg.clone(), &local.regions)
				.await
				.map_err(|error| {
					tg::error!(
						!error,
						"failed to get the sandbox processes from another region"
					)
				})? {
				return Ok(Some(stream));
			}
		}

		if let Some(stream) = self
			.try_get_sandbox_processes_remotes(id, arg, &locations.remotes)
			.await
			.map_err(|error| {
				tg::error!(!error, "failed to get the sandbox processes from a remote")
			})? {
			return Ok(Some(stream));
		}

		Ok(None)
	}

	async fn try_get_sandbox_processes_runner(
		&self,
		id: &tg::sandbox::Id,
		arg: &tg::sandbox::processes::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>> {
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
				.try_get_sandbox_processes_runner_task(&id, arg_, runner, sender.clone())
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

	fn try_get_sandbox_processes_runner_task<'a>(
		&'a self,
		id: &'a tg::sandbox::Id,
		mut arg: tg::sandbox::processes::get::Arg,
		mut runner: crate::sandbox::Runner,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sandbox::processes::get::Event>>,
	) -> BoxFuture<'a, tg::Result<()>> {
		async move {
			let mut position = arg.position.unwrap_or(std::io::SeekFrom::Start(0));
			let mut read = 0;
			loop {
				let size = arg
					.size
					.unwrap_or(256)
					.min(arg.length.map_or(u64::MAX, |length| length - read));
				let output = self
					.server
					.runner
					.state()
					.sandboxes()
					.get(runner.index)
					.map(|sandbox| -> tg::Result<_> {
						let length = u64::try_from(sandbox.process_ids.len()).unwrap();
						let position = match position {
							std::io::SeekFrom::Current(seek) | std::io::SeekFrom::End(seek) => {
								length
									.checked_add_signed(seek)
									.ok_or_else(|| tg::error!("invalid position"))?
							},
							std::io::SeekFrom::Start(position) => position,
						};
						let output = sandbox.processes(position, size);
						Ok((position, output.processes, output.status))
					})
					.transpose()?;
				let Some((start, processes, status)) = output else {
					// Resume from the next unread process at the owning location.
					arg.location = Some(runner.location_arg);
					arg.position = Some(position);
					arg.length = arg.length.map(|length| length - read);
					let mut stream = self
						.try_get_sandbox_processes_stream(id, arg)
						.boxed()
						.await?
						.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
					while let Some(event) = stream.next().await {
						if sender.send(event).await.is_err() {
							break;
						}
					}
					return Ok(());
				};
				let length = u64::try_from(processes.len()).unwrap();
				position = std::io::SeekFrom::Start(
					start
						.checked_add(length)
						.ok_or_else(|| tg::error!("invalid position"))?,
				);
				read += length;
				if !processes.is_empty() {
					let chunk = tg::sandbox::processes::get::Chunk {
						data: processes,
						position: start,
					};
					if sender
						.send(Ok(tg::sandbox::processes::get::Event::Chunk(chunk)))
						.await
						.is_err()
					{
						return Ok(());
					}
					continue;
				}
				if status.is_destroyed()
					|| arg.length.is_some_and(|length| read >= length)
					|| arg.timeout == Some(Duration::ZERO)
				{
					sender
						.send(Ok(tg::sandbox::processes::get::Event::End))
						.await
						.ok();
					return Ok(());
				}
				runner.changed.changed().await.ok();
			}
		}
		.boxed()
	}

	async fn try_get_sandbox_processes_local(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>> {
		let resource = tg::Referent::with_node_and_local_tokens(
			id.clone(),
			arg.tokens.local_authorization().to_vec(),
		);
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Read,
		);
		let permissions = self.authorize(resource, permission).await?;
		if !permissions.is_some_and(|permissions| permissions.contains(permission)) {
			return Ok(None);
		}

		// Create the wakeups stream.
		let mut wakeups = if arg.timeout == Some(Duration::ZERO) {
			None
		} else {
			let subject = format!("sandboxes.{id}.processes");
			let processes_wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ());
			let subject = format!("sandboxes.{id}.status");
			let status_wakeups = self
				.server
				.messenger
				.subscribe::<()>(subject)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ());
			let wakeups = stream::select(processes_wakeups, status_wakeups);
			let interval = IntervalStream::new(tokio::time::interval(
				self.server.config.sandbox.processes_wakeup_interval,
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
		let deadline = self.server.control_read_deadline();
		let output = loop {
			tokio::select! {
				output = self.get_sandbox_processes_local_inner(id, start, length, arg.source, deadline) => break output?,
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
					self.server
						.index
						.try_get_sandbox_processes_count(id)
						.await?
						.ok_or_else(|| tg::error!("missing the sandbox processes"))?
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
		let session = self.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = session
				.try_get_sandbox_processes_local_task(&id, arg, sender.clone(), wakeups, initial)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});

		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(Some(stream))
	}

	async fn get_sandbox_processes_local_inner(
		&self,
		id: &tg::sandbox::Id,
		position: u64,
		length: u64,
		source: tg::sandbox::Source,
		deadline: tokio::time::Instant,
	) -> tg::Result<Output> {
		self.get_sandbox_state_local(
			id,
			self.get_sandbox_processes_from_control(id, position, length),
			|_| false,
			true,
			source,
			deadline,
		)
		.boxed()
		.await
	}

	async fn get_sandbox_processes_from_control(
		&self,
		id: &tg::sandbox::Id,
		position: u64,
		length: u64,
	) -> tg::Result<tg::sandbox::control::GetProcessesClientResponseOutput> {
		let request = tg::sandbox::control::ServerRequestArg::GetProcesses(
			tg::sandbox::control::GetProcessesServerRequestArg { length, position },
		);
		let options = crate::control::Options {
			retry: tangram_futures::retry::Options::default(),
			timeout: self.server.config.control.read_timeout,
		};
		let response = self
			.request_sandbox_control(id, request, options)
			.boxed()
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the get processes control request"),
			)?
			.map_err(|error| tg::error!(!error, %id, "the get processes control request failed"))?;
		let output = response
			.try_unwrap_get_processes()
			.map_err(|_| tg::error!("expected a get processes response"))?;

		Ok(output)
	}

	async fn try_get_sandbox_processes_local_task(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sandbox::processes::get::Event>>,
		mut wakeups: Option<BoxStream<'static, ()>>,
		mut initial: Option<Output>,
	) -> tg::Result<()> {
		let std::io::SeekFrom::Start(mut position) =
			arg.position.unwrap_or(std::io::SeekFrom::Start(0))
		else {
			return Err(tg::error!(%id, "invalid position"));
		};
		let size = arg.size.unwrap_or(256);
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
				let deadline = self.server.control_read_deadline();
				let output = loop {
					tokio::select! {
						output = self.get_sandbox_processes_local(id, position, size, initial.take(), arg.source, deadline) => break output?,
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
				if output.processes.is_empty() {
					break output.status;
				}
				let chunk = tg::sandbox::processes::get::Chunk {
					data: output.processes,
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
					.send(Ok(tg::sandbox::processes::get::Event::Chunk(chunk)))
					.await;
				if result.is_err() {
					return Ok(());
				}
			};

			// If the sandbox is destroyed or the length is reached, then send the end event and break.
			let end = arg.length.is_some_and(|length| read >= length);
			if end || status.is_destroyed() {
				let result = sender
					.send(Ok(tg::sandbox::processes::get::Event::End))
					.await;
				if result.is_err() {
					return Ok(());
				}
				break;
			}

			// Wait for an event before returning to the top of the loop.
			let Some(wakeups) = &mut wakeups else {
				sender
					.send(Ok(tg::sandbox::processes::get::Event::End))
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

	async fn get_sandbox_processes_local(
		&self,
		id: &tg::sandbox::Id,
		position: u64,
		length: u64,
		initial: Option<Output>,
		source: tg::sandbox::Source,
		deadline: tokio::time::Instant,
	) -> tg::Result<LocalProcesses> {
		let output = match initial {
			Some(output) => output,
			None => {
				self.get_sandbox_processes_local_inner(id, position, length, source, deadline)
					.await?
			},
		};
		if let Some(output) = output.control {
			let output = LocalProcesses {
				processes: output.processes,
				status: output.status,
			};
			return Ok(output);
		}
		let sandbox = output
			.indexed
			.ok_or_else(|| tg::error!(%id, "failed to find the sandbox"))?;
		let status = sandbox
			.data
			.ok_or_else(|| tg::error!(%id, "missing the sandbox data"))?
			.data
			.status;
		let processes = self
			.server
			.index
			.try_get_sandbox_processes(id, std::io::SeekFrom::Start(position), length)
			.await?
			.ok_or_else(|| tg::error!("missing the sandbox processes"))?;
		let output = LocalProcesses { processes, status };
		Ok(output)
	}

	async fn try_get_sandbox_processes_regions(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
		regions: &[String],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_sandbox_processes_region(id, arg.clone(), region))
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

	async fn try_get_sandbox_processes_region(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
		region: &str,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let tokens = arg.tokens.for_location(&location);
		let arg = tg::sandbox::processes::get::Arg {
			location: Some(location.clone().into()),
			tokens,
			..arg
		};
		let Some(stream) = client
			.try_get_sandbox_processes_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, region = %region, "failed to get the sandbox processes"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	async fn try_get_sandbox_processes_remotes(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
		remotes: &[crate::location::Remote],
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_sandbox_processes_remote(id, arg.clone(), remote))
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

	async fn try_get_sandbox_processes_remote(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
		remote: &crate::location::Remote,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>> {
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
		let tokens = arg.tokens.for_location(&location);
		let arg = tg::sandbox::processes::get::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			tokens,
			..arg
		};
		let Some(stream) = client
			.try_get_sandbox_processes_stream(id, arg)
			.await
			.map_err(
				|error| tg::error!(!error, remote = %remote.name, "failed to get the sandbox processes"),
			)?
		else {
			return Ok(None);
		};
		Ok(Some(stream.boxed()))
	}

	pub(crate) async fn try_get_sandbox_processes_stream_request(
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
		let Some(stream) = self.try_get_sandbox_processes_stream(&id, arg).await? else {
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
