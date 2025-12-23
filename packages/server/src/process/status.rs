use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	indoc::formatdoc,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{stream::Ext as _, task::Stop},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

impl Server {
	pub async fn try_get_process_status_stream_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::status::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static + use<>>,
	> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(status) = self.try_get_process_status_stream_local(id).await?
		{
			return Ok(Some(status.left_stream()));
		}

		// Try remotes.
		let remotes = self.remotes(arg.remotes.clone()).await?;
		if let Some(status) = self.try_get_process_status_remote(id, &remotes).await? {
			return Ok(Some(status.right_stream()));
		}

		Ok(None)
	}

	pub(crate) async fn try_get_process_status_stream_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static + use<>>,
	> {
		// Verify the process is local.
		if !self.get_process_exists_local(id).await? {
			return Ok(None);
		}

		// Subscribe to status events.
		let subject = format!("processes.{id}.status");
		let status = self
			.messenger
			.subscribe::<()>(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());

		// Create the interval.
		let interval =
			IntervalStream::new(tokio::time::interval(Duration::from_secs(60))).map(|_| ());

		// Create the stream.
		let server = self.clone();
		let id = id.clone();
		let mut previous: Option<tg::process::Status> = None;
		let stream = stream::select(status, interval)
			.boxed()
			.then(move |()| {
				let server = server.clone();
				let id = id.clone();
				async move { server.get_current_process_status_local(&id).await }
			})
			.try_filter(move |status| {
				future::ready(match (previous.as_mut(), *status) {
					(None, status) => {
						previous.replace(status);
						true
					},
					(Some(previous), status) if *previous == status => false,
					(Some(previous), status) => {
						*previous = status;
						true
					},
				})
			})
			.take_while_inclusive(move |result| {
				if let Ok(status) = result {
					future::ready(!status.is_finished())
				} else {
					future::ready(false)
				}
			})
			.map_ok(tg::process::status::Event::Status)
			.chain(stream::once(future::ok(tg::process::status::Event::End)));

		Ok(Some(stream))
	}

	pub(crate) async fn get_current_process_status_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<tg::process::Status> {
		self.try_get_current_process_status_local(id)
			.await?
			.ok_or_else(|| tg::error!("failed to find the process"))
	}

	pub(crate) async fn try_get_current_process_status_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<tg::process::Status>> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the status.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select status
				from processes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let Some(status) = connection
			.query_optional_value_into::<db::value::Serde<tg::process::Status>>(
				statement.into(),
				params,
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.map(|value| value.0)
		else {
			return Ok(None);
		};

		// Drop the database connection.
		drop(connection);

		Ok(Some(status))
	}

	async fn try_get_process_status_remote(
		&self,
		id: &tg::process::Id,
		remotes: &[String],
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::process::status::Event>> + Send + 'static + use<>>,
	> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let futures = remotes
			.iter()
			.map(|remote| {
				let remote = remote.clone();
				let id = id.clone();
				async move {
					let client = self.get_remote_client(remote).await?;
					client
						.get_process_status(&id, tg::process::status::Arg::default())
						.await
						.map(futures::StreamExt::boxed)
				}
				.boxed()
			})
			.collect::<Vec<_>>();
		let Ok((stream, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		let stream = stream
			.map_ok(tg::process::status::Event::Status)
			.chain(stream::once(future::ok(tg::process::status::Event::End)));
		Ok(Some(stream))
	}

	pub(crate) async fn handle_get_process_status_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Parse the ID.
		let id = id.parse()?;

		// Parse the arg.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Get the stream.
		let Some(stream) = self
			.try_get_process_status_stream_with_context(context, &id, arg)
			.await?
		else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.wait().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let stream = stream.map(|result| match result {
					Ok(event) => event.try_into(),
					Err(error) => error.try_into(),
				});
				(Some(content_type), Body::with_sse_stream(stream))
			},

			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
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
