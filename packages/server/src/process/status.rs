use {
	crate::Session,
	futures::{
		FutureExt as _, StreamExt as _, future,
		stream::{BoxStream, FuturesUnordered},
	},
	indoc::formatdoc,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
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
	pub async fn try_get_process_status_stream(
		&self,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let locations = self
			.locations(arg.location.as_ref())
			.await
			.map_err(|error| tg::error!(!error, "failed to resolve the locations"))?;

		if let Some(local) = &locations.local {
			let stopper = self.context.stopper.clone();
			if local.current {
				let permission = tg::grant::Permission::Process(
					tg::grant::permission::process::Permission::Node,
				);
				let resource = tg::Referent::with_item_and_token(id.clone(), arg.token.clone());
				let authorize_future = self.authorize(resource, permission).boxed();
				let exists_future = self.exists(id.clone(), permission).boxed();
				let check_future = async {
					let (authorized, exists) =
						future::try_join(authorize_future, exists_future).await?;
					Ok::<_, tg::Error>(
						exists
							&& authorized
								.is_some_and(|permissions| permissions.contains(permission)),
					)
				}
				.boxed();
				let create_future = self
					.create_process_status_stream_local(id, stopper, arg.timeout)
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
						|error| tg::error!(!error, %id, "failed to get the process status stream"),
					)?;
					return Ok(Some(stream));
				}
			}

			if let Some(status) = self
				.try_get_process_status_stream_regions(
					id,
					&local.regions,
					arg.timeout,
					arg.token.as_ref(),
				)
				.await
				.map_err(
					|error| tg::error!(!error, %id, "failed to get the process status from another region"),
				)? {
				return Ok(Some(status));
			}
		}

		if let Some(status) = self
			.try_get_process_status_stream_remotes(
				id,
				&locations.remotes,
				arg.timeout,
				arg.token.as_ref(),
			)
			.await
			.map_err(
				|error| tg::error!(!error, %id, "failed to get the process status from a remote"),
			)? {
			return Ok(Some(status));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_status_stream_local(
		&self,
		id: &tg::process::Id,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		Ok(Some(
			self.create_process_status_stream_local(id, stopper, timeout)
				.await?,
		))
	}

	pub(crate) async fn create_process_status_stream_local(
		&self,
		id: &tg::process::Id,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> tg::Result<BoxStream<'static, tg::Result<tg::process::status::Event>>> {
		// Create the wakeups stream.
		let wakeups = if timeout == Some(Duration::ZERO) {
			None
		} else {
			let subject = format!("processes.{id}.status");
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
				.try_get_process_status_stream_local_task(&id, sender.clone(), wakeups)
				.await;
			if let Err(error) = result {
				sender.send(Err(error)).await.ok();
			}
		});

		let stream = ReceiverStream::new(receiver).attach(task).boxed();

		Ok(stream)
	}

	async fn try_get_process_status_stream_local_task(
		&self,
		id: &tg::process::Id,
		sender: tokio::sync::mpsc::Sender<tg::Result<tg::process::status::Event>>,
		mut wakeups: Option<BoxStream<'static, ()>>,
	) -> tg::Result<()> {
		let mut previous: Option<tg::process::Status> = None;
		loop {
			// Retry the read when the status changes while the request is in flight.
			let status = match &mut wakeups {
				None => self.try_get_process_status_local(id).await?,
				Some(wakeups) => {
					tokio::select! {
						result = self.try_get_process_status_local(id) => result?,
						wakeup = wakeups.next() => {
							if wakeup.is_none() {
								return Ok(());
							}
							continue;
						},
					}
				},
			}
			.unwrap_or(tg::process::Status::Finished);
			if previous != Some(status) {
				previous.replace(status);
				let event = tg::process::status::Event::Status(status);
				if sender.send(Ok(event)).await.is_err() {
					return Ok(());
				}
			}
			if status.is_finished() {
				sender.send(Ok(tg::process::status::Event::End)).await.ok();
				return Ok(());
			}
			let Some(wakeups) = &mut wakeups else {
				sender.send(Ok(tg::process::status::Event::End)).await.ok();
				return Ok(());
			};
			if wakeups.next().await.is_none() {
				return Ok(());
			}
		}
	}

	pub(crate) async fn get_process_status_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<tg::process::Status> {
		self.try_get_process_status_local(id)
			.await?
			.ok_or_else(|| tg::error!("failed to find the process"))
	}

	pub(crate) async fn try_get_process_status_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Status>> {
		if let Some(data) = self.server.runner.state.try_get_process(id) {
			return Ok(Some(data.status));
		}
		let control_future = self.get_process_from_control(id).boxed();
		let process_store_future = self.try_get_process_status_from_process_store(id).boxed();
		match future::select(control_future, process_store_future).await {
			future::Either::Left((result, _)) => result.map(|data| Some(data.status)),
			future::Either::Right((result, control_future)) => match result? {
				Some(status) => Ok(Some(status)),
				None => control_future.await.map(|data| Some(data.status)),
			},
		}
	}

	async fn try_get_process_status_from_process_store(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Status>> {
		let connection = self
			.server
			.process_store
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a process store connection"))?;
		let p = connection.p();
		let statement = formatdoc!(
			"
				select status
				from processes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let status = connection
			.query_optional_value_into::<db::value::Serde<tg::process::Status>>(
				statement.into(),
				params,
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
			.map(|status| status.0);
		Ok(status)
	}

	async fn try_get_process_status_stream_regions(
		&self,
		id: &tg::process::Id,
		regions: &[String],
		timeout: Option<Duration>,
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let mut futures = regions
			.iter()
			.map(|region| self.try_get_process_status_stream_region(id, region, timeout, token))
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
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let client = self.get_region_session(region).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.to_owned()),
		});
		let arg = tg::process::status::Arg {
			location: Some(location.into()),
			timeout,
			token: token.cloned(),
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
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let mut futures = remotes
			.iter()
			.map(|remote| self.try_get_process_status_stream_remote(id, remote, timeout, token))
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
		token: Option<&tg::grant::Token>,
	) -> tg::Result<Option<BoxStream<'static, tg::Result<tg::process::status::Event>>>> {
		let client = self.get_remote_session(&remote.name).await.map_err(
			|error| tg::error!(!error, remote = %remote.name, "failed to get the remote client"),
		)?;
		let arg = tg::process::status::Arg {
			location: Some(tg::location::Arg(vec![
				tg::location::arg::Component::Local(tg::location::arg::LocalComponent {
					regions: remote.regions.clone(),
				}),
			])),
			timeout,
			token: token.cloned(),
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
