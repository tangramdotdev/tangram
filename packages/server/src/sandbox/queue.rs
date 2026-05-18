use {
	crate::{Session, context::Authentication, database::Database},
	futures::{StreamExt as _, future, stream},
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_futures::{stream::Ext as _, task::Stopper},
	tangram_http::{body::Boxed as BoxBody, request::Ext as _},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "sqlite")]
mod sqlite;

pub(super) struct LocalOutput {
	process: Option<tg::process::Id>,
	process_token: Option<String>,
	sandbox: tg::sandbox::Id,
	token: Option<String>,
}

impl Session {
	pub(crate) async fn try_dequeue_sandbox(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		match &self.context.authentication {
			Some(Authentication::Root | Authentication::Runner) => (),
			authentication
				if self.server.config().authentication.is_none()
					&& !matches!(authentication, Some(Authentication::Process(_))) => {},
			_ => return Err(tg::error!("unauthorized")),
		}

		let location = self.server.location(arg.location.as_ref())?;
		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_dequeue_sandbox_local(self.context.stopper.clone(), arg.timeout)
					.await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => self.try_dequeue_sandbox_region(region, arg.timeout).await?,
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.try_dequeue_sandbox_remote(remote, region, arg.timeout)
					.await?
			},
		};

		Ok(output)
	}

	fn try_dequeue_sandbox_stream(
		&self,
		arg: tg::sandbox::queue::Arg,
	) -> impl futures::Stream<Item = tg::Result<tg::sandbox::queue::Output>> + Send + use<> {
		let session = self.clone();
		let dequeue = async move { session.try_dequeue_sandbox(arg).await };
		stream::once(dequeue).filter_map(|output| future::ready(output.transpose()))
	}

	async fn try_dequeue_sandbox_local(
		&self,
		stopper: Option<Stopper>,
		timeout: Option<Duration>,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		let mut wakeups = if timeout == Some(Duration::ZERO) {
			None
		} else {
			let wakeups = self
				.server
				.messenger
				.subscribe_with_delivery::<()>("sandboxes.created".into(), Delivery::One)
				.await
				.map_err(|error| tg::error!(!error, "failed to subscribe"))?
				.map(|_| ());
			let interval = Duration::from_secs(1);
			let interval = IntervalStream::new(tokio::time::interval(interval))
				.skip(1)
				.map(|_| ());
			let wakeups = stream::select(wakeups, interval);
			let wakeups = match timeout {
				Some(timeout) => wakeups.take_until(tokio::time::sleep(timeout)).boxed(),
				None => wakeups.boxed(),
			};
			Some(wakeups.with_stopper(stopper))
		};

		// Dequeue.
		loop {
			let token = self.server.config.authentication.is_some();
			let output = match &self.server.process_store {
				#[cfg(feature = "postgres")]
				Database::Postgres(process_store) => {
					self.try_dequeue_sandbox_postgres(process_store, token)
						.await?
				},
				#[cfg(feature = "sqlite")]
				Database::Sqlite(process_store) => {
					self.try_dequeue_sandbox_sqlite(process_store, token)
						.await?
				},
			};
			if let Some(output) = output {
				let output = tg::sandbox::queue::Output {
					process: output.process,
					process_token: output.process_token,
					sandbox: output.sandbox,
					token: output.token,
				};
				self.server
					.spawn_publish_sandbox_status_task(&output.sandbox);
				if let Some(process) = &output.process {
					self.server
						.messenger
						.publish(format!("processes.{process}.status"), ())
						.await
						.ok();
				}
				return Ok(Some(output));
			}
			let Some(wakeups) = &mut wakeups else {
				break;
			};
			if wakeups.next().await.is_none() {
				break;
			}
		}

		Ok(None)
	}

	async fn try_dequeue_sandbox_region(
		&self,
		region: String,
		timeout: Option<Duration>,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		let client = self.get_region_session(region.clone()).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::sandbox::queue::Arg {
			location: Some(location.into()),
			timeout,
		};
		let output = client.try_dequeue_sandbox(arg).await.map_err(
			|error| tg::error!(!error, region = %region, "failed to dequeue the sandbox"),
		)?;
		Ok(output)
	}

	async fn try_dequeue_sandbox_remote(
		&self,
		remote: String,
		region: Option<String>,
		timeout: Option<Duration>,
	) -> tg::Result<Option<tg::sandbox::queue::Output>> {
		let client = self.get_remote_session(remote.clone()).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::queue::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
			timeout,
		};
		let output = client.try_dequeue_sandbox(arg).await.map_err(
			|error| tg::error!(!error, remote = %remote, "failed to dequeue the sandbox"),
		)?;
		Ok(output)
	}

	pub(crate) fn spawn_publish_sandboxes_created_message_task(&self) {
		tokio::spawn({
			let session = self.clone();
			async move {
				session
					.server
					.messenger
					.publish("sandboxes.created".into(), ())
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to publish the sandbox created message");
					})
					.ok();
			}
		});
	}

	pub(crate) async fn try_dequeue_sandbox_request(
		&self,
		request: http::Request<BoxBody>,
	) -> tg::Result<http::Response<BoxBody>> {
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|error| tg::error!(!error, "failed to parse the accept header"))?;
		let arg = request
			.json_or_default()
			.await
			.map_err(|error| tg::error!(!error, "failed to deserialize the request body"))?;
		let stream = self.try_dequeue_sandbox_stream(arg);
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream =
					stream.map(
						|result: tg::Result<tg::sandbox::queue::Output>| match result {
							Ok(event) => event.try_into(),
							Err(error) => error.try_into(),
						},
					);
				(Some(content_type), BoxBody::with_sse_stream(stream))
			},
			Some((type_, subtype)) => {
				return Err(tg::error!(%type_, %subtype, "invalid accept type"));
			},
		};
		let mut response = http::Response::builder();
		if let Some(content_type) = content_type {
			response = response.header(http::header::CONTENT_TYPE, content_type.to_string());
		}
		Ok(response.body(body).unwrap())
	}
}
