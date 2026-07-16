use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _, future,
		stream::{self, BoxStream, FuturesUnordered},
	},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{stream::Ext as _, task::Task},
	tangram_http::{
		body::Boxed as BoxBody, request::Ext as _, response::Ext as _, response::builder::Ext as _,
	},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::ReceiverStream,
};

impl Session {
	pub async fn try_get_process_children_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
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

	async fn try_get_process_children_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::children::get::Event>>>> {
		let permission =
			tg::grant::Permission::Process(tg::grant::permission::process::Permission::Node);
		let resource = tg::Referent::with_item_and_token(id.clone(), arg.token.clone());
		let authorize_future = self.authorize(resource, permission).boxed();
		let exists_future = self.exists(id.clone(), permission).boxed();
		let check_future = async {
			let (authorized, exists) = future::try_join(authorize_future, exists_future).await?;
			Ok::<_, tg::Error>(
				exists && authorized.is_some_and(|permissions| permissions.contains(permission)),
			)
		}
		.boxed();
		let create_future = self.create_process_children_stream_local(id, arg).boxed();
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
		let stream = stream?;
		Ok(Some(stream))
	}

	async fn create_process_children_stream_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::children::get::Event>>> {
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
				.try_get_process_children_local_task(&id, arg, sender.clone(), wakeups)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});

		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(stream)
	}

	async fn try_get_process_children_local_task(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::children::get::Event>>,
		mut wakeups: Option<BoxStream<'static, ()>>,
	) -> tg::Result<()> {
		// Get the position.
		let position = match arg.position {
			Some(std::io::SeekFrom::Start(seek)) => seek,
			Some(std::io::SeekFrom::End(seek) | std::io::SeekFrom::Current(seek)) => self
				.get_process_children_local(id, 0, 0)
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
				let output = self.get_process_children_local(id, position, size).await?;

				// If the chunk is empty, then break.
				if output.children.is_empty() {
					break output.status;
				}
				let chunk = tg::process::children::get::Chunk {
					position,
					data: output.children,
				};

				// Update the state.
				position += chunk.data.len().to_u64().unwrap();
				read += chunk.data.len().to_u64().unwrap();

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
		position: u64,
		length: u64,
	) -> tg::Result<tg::process::control::GetChildrenClientResponseOutput> {
		if let Some(output) = self
			.server
			.runner
			.state
			.try_get_process_children(id, position, length)
		{
			return Ok(output);
		}
		let control_future = self
			.get_process_children_from_control(id, position, length)
			.boxed();
		let process_store_future = self
			.try_get_process_children_from_process_store(id, position, length)
			.boxed();
		match future::select(control_future, process_store_future).await {
			future::Either::Left((result, _)) => result,
			future::Either::Right((result, control_future)) => match result? {
				Some(output) => Ok(output),
				None => control_future.await,
			},
		}
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
		let retry = tangram_futures::retry::Options {
			max_retries: u64::MAX,
			..Default::default()
		};
		let options = crate::control::Options {
			retry,
			timeout: Duration::from_secs(10),
		};
		let response = self
			.send_process_control_request(id, request, options)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to send the get children process control request"),
			)?
			.map_err(
				|error| tg::error!(!error, %id, "the get children process control request failed"),
			)?;
		response
			.try_unwrap_get_children()
			.map_err(|_| tg::error!("expected a get children response"))
	}

	async fn try_get_process_children_from_process_store(
		&self,
		id: &tg::process::Id,
		position: u64,
		length: u64,
	) -> tg::Result<Option<tg::process::control::GetChildrenClientResponseOutput>> {
		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let p = connection.p();
		#[derive(db::row::Deserialize)]
		struct ProcessRow {
			length: u64,
			#[tangram_database(as = "db::value::FromStr")]
			status: tg::process::Status,
		}
		let statement = formatdoc!(
			"
				select
					(select count(*) from process_children where process = processes.id) as length,
					processes.status
				from processes
				where processes.id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let Some(process) = connection
			.query_optional_into::<ProcessRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		else {
			return Ok(None);
		};

		#[derive(db::row::Deserialize)]
		struct ChildRow {
			cached: bool,
			#[tangram_database(as = "db::value::FromStr")]
			child: tg::process::Id,
			#[tangram_database(as = "db::value::Json<tg::referent::Options>")]
			options: tg::referent::Options,
		}
		let statement = formatdoc!(
			"
				select cached, child, options
				from process_children
				where process = {p}1
				order by position
				limit {p}2
				offset {p}3;
			"
		);
		let params = db::params![id.to_string(), length, position];
		let children = connection
			.query_all_into::<ChildRow>(statement.into(), params)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			.into_iter()
			.map(|row| tg::process::data::Child {
				cached: row.cached,
				process: tg::Referent::new(row.child, row.options),
			})
			.collect();
		Ok(Some(
			tg::process::control::GetChildrenClientResponseOutput {
				children,
				length: process.length,
				status: process.status,
			},
		))
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
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::children::get::Arg {
			location: Some(location.into()),
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
		Ok(Some(stream.boxed()))
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
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::process::children::get::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
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
		Ok(Some(stream.boxed()))
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
