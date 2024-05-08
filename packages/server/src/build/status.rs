use crate::Server;
use futures::{future, stream, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _};
use http_body_util::StreamBody;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{outgoing::ResponseExt as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;
use tokio_stream::wrappers::IntervalStream;

impl Server {
	pub async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>> {
		if let Some(status) = self.try_get_build_status_local(id, arg.clone()).await? {
			Ok(Some(status.left_stream()))
		} else if let Some(status) = self.try_get_build_status_remote(id, arg.clone()).await? {
			Ok(Some(status.right_stream()))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn try_get_build_status_local(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Create the event stream.
		let status = self
			.messenger
			.subscribe(format!("builds.{id}.status"), None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());
		let interval =
			IntervalStream::new(tokio::time::interval(std::time::Duration::from_secs(60)))
				.map(|_| ());
		let timeout = arg.timeout.map_or_else(
			|| future::pending().left_future(),
			|timeout| tokio::time::sleep(timeout).right_future(),
		);
		let events = stream::once(future::ready(()))
			.chain(stream::select(status, interval).take_until(timeout))
			.boxed();

		// Create the stream.
		let server = self.clone();
		let id = id.clone();
		let stream = stream::try_unfold(
			(server, id, events, None, false),
			move |(server, id, mut events, mut previous, mut end)| async move {
				// If the stream should end, return None.
				if end {
					return Ok(None);
				}

				// Wait for the next event.
				let Some(()) = events.next().await else {
					return Ok(None);
				};

				// Get the current status.
				let status = server.try_get_build_status_local_inner(&id).await?;

				// Handle the previous status.
				if Some(status) == previous {
					return Ok::<_, tg::Error>(Some((
						stream::iter(None),
						(server, id, events, previous, end),
					)));
				}
				previous = Some(status);

				// If the build is finished, then the stream should end.
				if status == tg::build::Status::Finished {
					end = true;
				}

				Ok::<_, tg::Error>(Some((
					stream::iter(Some(Ok(status))),
					(server, id, events, previous, end),
				)))
			},
		)
		.try_flatten()
		.boxed();

		Ok(Some(stream))
	}

	async fn try_get_build_status_local_inner(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<tg::build::Status> {
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
				from builds
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let status = connection
			.query_one_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(status)
	}

	async fn try_get_build_status_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::Arg,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::Status>> + Send + 'static>> {
		let Some(remote) = self.remotes.first() else {
			return Ok(None);
		};
		let Some(stream) = remote.try_get_build_status(id, arg).await? else {
			return Ok(None);
		};
		Ok(Some(stream))
	}
}

impl Server {
	pub(crate) async fn handle_get_build_status_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to deserialize the query"))?
			.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.headers()
			.get(http::header::ACCEPT)
			.map(|accept| {
				let accept = accept
					.to_str()
					.map_err(|source| tg::error!(!source, "invalid content type"))?;
				let accept = accept
					.parse::<mime::Mime>()
					.map_err(|source| tg::error!(!source, "invalid content type"))?;
				Ok::<_, tg::Error>(accept)
			})
			.transpose()?;

		// Get the stream.
		let Some(stream) = handle.try_get_build_status(&id, arg).await? else {
			return Ok(http::Response::not_found());
		};

		// Stop the stream when the server stops.
		let mut stop = request
			.extensions()
			.get::<tokio::sync::watch::Receiver<bool>>()
			.cloned()
			.unwrap();
		let stop = async move { stop.wait_for(|stop| *stop).map(|_| ()).await };
		let stream = stream.take_until(stop);

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let body = stream
					.map_ok(|chunk| {
						let data = serde_json::to_string(&chunk).unwrap();
						let event = tangram_http::sse::Event::with_data(data);
						hyper::body::Frame::data(event.to_string().into())
					})
					.err_into();
				(content_type, body)
			},
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};
		let body = Outgoing::body(StreamBody::new(body));

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}
}
