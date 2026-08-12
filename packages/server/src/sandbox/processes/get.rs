use {
	super::Output,
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _, TryStreamExt as _, future,
		stream::{self, BoxStream, FuturesOrdered, FuturesUnordered},
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

impl Session {
	pub async fn try_get_sandbox_processes_stream(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>> {
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

	async fn try_get_sandbox_processes_local(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>>> {
		let permission = tg::authorization::Permission::Sandbox(
			tg::authorization::permission::sandbox::Permission::Read,
		);
		let resource = tg::Referent::with_node_and_tokens(id.clone(), arg.tokens.clone());
		let check_future = async move {
			let authorized = self.authorize(resource, permission).await?;
			let authorized = authorized.is_some_and(|permissions| permissions.contains(permission));
			if !authorized {
				return Ok(false);
			}
			self.try_get_sandbox_local_inner(id)
				.await
				.map(|output| output.is_some())
		}
		.boxed();
		let create_future = self.create_sandbox_processes_stream_local(id, arg).boxed();
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
		let Some(stream) = stream else {
			return Ok(None);
		};

		Ok(Some(stream?))
	}

	async fn create_sandbox_processes_stream_local(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::sandbox::processes::get::Event>>> {
		// Create the wakeups stream.
		let wakeups = if arg.timeout == Some(Duration::ZERO) {
			None
		} else {
			let subject = format!("sandboxes.{id}.processes");
			let process_wakeups = self
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
			let wakeups = stream::select(process_wakeups, status_wakeups);
			let interval = IntervalStream::new(tokio::time::interval(Duration::from_mins(1)))
				.skip(1)
				.map(|_| ());
			let wakeups = stream::select(wakeups, interval);
			let wakeups = match arg.timeout {
				Some(timeout) => wakeups.take_until(tokio::time::sleep(timeout)).boxed(),
				None => wakeups.boxed(),
			};
			Some(wakeups.with_stopper(self.context.stopper.clone()))
		};

		// Create the channel.
		let (sender, receiver) = tokio::sync::mpsc::channel(1);

		// Spawn the task.
		let session = self.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = session
				.try_get_sandbox_processes_local_task(&id, arg, sender.clone(), wakeups)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});

		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(stream)
	}

	async fn try_get_sandbox_processes_local_task(
		&self,
		id: &tg::sandbox::Id,
		arg: tg::sandbox::processes::get::Arg,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::sandbox::processes::get::Event>>,
		mut wakeups: Option<BoxStream<'static, ()>>,
	) -> tg::Result<()> {
		// Get the position.
		let position = match arg.position {
			Some(std::io::SeekFrom::Start(seek)) => seek,
			Some(std::io::SeekFrom::End(seek) | std::io::SeekFrom::Current(seek)) => self
				.get_sandbox_processes_local(id, 0, 0)
				.await
				.map_err(|error| tg::error!(!error, "failed to get the current position"))?
				.length
				.to_i64()
				.unwrap()
				.checked_add(seek)
				.ok_or_else(|| tg::error!("invalid position"))?
				.to_u64()
				.ok_or_else(|| tg::error!("invalid position"))?,
			None => 0,
		};

		// Create the state.
		let size = arg.size.unwrap_or(10);
		let mut position = position;
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
				let output = self.get_sandbox_processes_local(id, position, size).await?;

				// If the chunk is empty, then break.
				if output.processes.is_empty() {
					break output.status;
				}
				let chunk = tg::sandbox::processes::get::Chunk {
					data: output.processes,
					position,
				};

				// Update the state.
				position += chunk.data.len().to_u64().unwrap();
				read += chunk.data.len().to_u64().unwrap();

				// Send the data.
				if sender
					.send(Ok(tg::sandbox::processes::get::Event::Chunk(chunk)))
					.await
					.is_err()
				{
					return Ok(());
				}
			};

			// End when the sandbox is destroyed or the requested length is reached.
			let end = arg.length.is_some_and(|length| read >= length);
			if end || status.is_destroyed() {
				sender
					.send(Ok(tg::sandbox::processes::get::Event::End))
					.await
					.ok();
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
	) -> tg::Result<Output> {
		if let Some(output) = self
			.server
			.runner
			.state()
			.try_get_sandbox_processes(id, position, length)
		{
			return Ok(output);
		}

		let status = self
			.try_get_sandbox_status_local(id)
			.await?
			.unwrap_or(tg::sandbox::Status::Destroyed);
		let mut processes = self
			.server
			.index
			.get_sandbox_processes(id)
			.await?
			.into_iter()
			.map(|(id, _)| id)
			.collect::<Vec<_>>();
		processes.sort();
		let output = Output {
			length: processes.len().to_u64().unwrap(),
			processes: processes
				.into_iter()
				.skip(position.to_usize().unwrap())
				.take(length.to_usize().unwrap())
				.collect(),
			status,
		};

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
			location: Some(location.into()),
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
		let mut remotes = remotes.to_owned();
		remotes.sort_by(|a, b| a.name.cmp(&b.name));
		let futures = remotes
			.iter()
			.map(|remote| self.try_get_sandbox_processes_remote(id, arg.clone(), remote))
			.collect::<FuturesOrdered<_>>();
		let streams = futures.try_collect::<Vec<_>>().await?;
		let Some(stream) = streams.into_iter().flatten().next() else {
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
		let client = self.get_remote_session(&remote.name).await.map_err(
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
			.map_err(|error| tg::error!(!error, "failed to parse the sandbox id"))?;

		// Get the query.
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
