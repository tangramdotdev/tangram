use {
	crate::{Context, Server, database::Database},
	futures::{StreamExt as _, future, stream},
	std::{pin::pin, time::Duration},
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

impl Server {
	pub(crate) async fn try_dequeue_sandbox_process_with_context(
		&self,
		context: &Context,
		sandbox: &tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}

		let location = self.location(arg.location.as_ref())?;

		let output = match location {
			tg::Location::Local(tg::location::Local { region: None }) => {
				self.try_dequeue_sandbox_process_local(sandbox, context.stopper.clone())
					.await?
			},
			tg::Location::Local(tg::location::Local {
				region: Some(region),
			}) => {
				self.try_dequeue_sandbox_process_region(sandbox, region)
					.await?
			},
			tg::Location::Remote(tg::location::Remote {
				name: remote,
				region,
			}) => {
				self.try_dequeue_sandbox_process_remote(sandbox, remote, region)
					.await?
			},
		};

		Ok(output)
	}

	fn try_dequeue_sandbox_process_stream_with_context(
		&self,
		context: &Context,
		sandbox: &tg::sandbox::Id,
		arg: tg::sandbox::process::queue::Arg,
	) -> impl futures::Stream<Item = tg::Result<tg::sandbox::process::queue::Output>> + Send + use<>
	{
		let handle = self.clone();
		let sandbox = sandbox.clone();
		let task_context = context.clone();
		let dequeue = async move {
			handle
				.try_dequeue_sandbox_process_with_context(&task_context, &sandbox, arg)
				.await
		};
		stream::once(dequeue).filter_map(|output| future::ready(output.transpose()))
	}

	async fn try_dequeue_sandbox_process_local(
		&self,
		sandbox: &tg::sandbox::Id,
		stopper: Option<Stopper>,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		// Verify the sandbox exists.
		if !self.get_sandbox_exists_local(sandbox).await.map_err(
			|source| tg::error!(!source, %sandbox, "failed to check if the sandbox exists"),
		)? {
			return Ok(None);
		}

		// Create the wakeups stream.
		let subject = format!("sandboxes.{sandbox}.processes.created");
		let wakeups = self
			.messenger
			.subscribe_with_delivery::<()>(subject, Delivery::One)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());
		let interval = Duration::from_secs(1);
		let interval = IntervalStream::new(tokio::time::interval(interval)).map(|_| ());
		let wakeups = stream::select(wakeups, interval).with_stopper(stopper);

		// Dequeue.
		let mut wakeups = pin!(wakeups);
		loop {
			let output = match &self.process_store {
				#[cfg(feature = "postgres")]
				Database::Postgres(process_store) => {
					self.try_dequeue_sandbox_process_postgres(process_store, sandbox)
						.await?
				},
				#[cfg(feature = "sqlite")]
				Database::Sqlite(process_store) => {
					self.try_dequeue_sandbox_process_sqlite(process_store, sandbox)
						.await?
				},
			};
			if let Some(output) = output {
				self.messenger
					.publish(format!("processes.{}.status", output.process), ())
					.await
					.ok();
				return Ok(Some(output));
			}
			if wakeups.next().await.is_none() {
				break;
			}
		}

		Ok(None)
	}

	async fn try_dequeue_sandbox_process_region(
		&self,
		sandbox: &tg::sandbox::Id,
		region: String,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		let client = self.get_region_client(region.clone()).await.map_err(
			|source| tg::error!(!source, region = %region, "failed to get the region client"),
		)?;
		let location = tg::Location::Local(tg::location::Local {
			region: Some(region.clone()),
		});
		let arg = tg::sandbox::process::queue::Arg {
			location: Some(location.into()),
		};
		let output = client
			.try_dequeue_sandbox_process(sandbox, arg)
			.await
			.map_err(
				|source| tg::error!(!source, region = %region, %sandbox, "failed to dequeue the process"),
			)?;
		Ok(output)
	}

	async fn try_dequeue_sandbox_process_remote(
		&self,
		sandbox: &tg::sandbox::Id,
		remote: String,
		region: Option<String>,
	) -> tg::Result<Option<tg::sandbox::process::queue::Output>> {
		let client = self.get_remote_client(remote.clone()).await.map_err(
			|source| tg::error!(!source, remote = %remote, "failed to get the remote client"),
		)?;
		let arg = tg::sandbox::process::queue::Arg {
			location: Some(tg::Location::Local(tg::location::Local { region }).into()),
		};
		let output = client
			.try_dequeue_sandbox_process(sandbox, arg)
			.await
			.map_err(
				|source| tg::error!(!source, remote = %remote, %sandbox, "failed to dequeue the process"),
			)?;
		Ok(output)
	}

	pub(crate) fn spawn_publish_sandbox_processes_created_message_task(
		&self,
		sandbox: &tg::sandbox::Id,
	) {
		let subject = format!("sandboxes.{sandbox}.processes.created");
		tokio::spawn({
			let server = self.clone();
			async move {
				server
					.messenger
					.publish(subject, ())
					.await
					.inspect_err(|error| {
						tracing::error!(%error, "failed to publish the sandbox process created message");
					})
					.ok();
			}
		});
	}

	pub(crate) async fn handle_dequeue_sandbox_process_request(
		&self,
		request: http::Request<BoxBody>,
		context: &Context,
		sandbox: &str,
	) -> tg::Result<http::Response<BoxBody>> {
		let sandbox = sandbox
			.parse::<tg::sandbox::Id>()
			.map_err(|source| tg::error!(!source, "failed to parse the sandbox id"))?;

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Parse the arg.
		let arg = request
			.json_or_default()
			.await
			.map_err(|source| tg::error!(!source, "failed to deserialize the request body"))?;

		// Get the stream.
		let stream = self.try_dequeue_sandbox_process_stream_with_context(context, &sandbox, arg);

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			None | Some((mime::STAR, mime::STAR) | (mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream =
					stream.map(|result: tg::Result<tg::sandbox::process::queue::Output>| {
						match result {
							Ok(event) => event.try_into(),
							Err(error) => error.try_into(),
						}
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
