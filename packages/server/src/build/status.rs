use crate::Server;
use futures::{future, stream, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use tangram_client::{self as tg, handle::Ext as _};
use tangram_database::{self as db, prelude::*};
use tangram_futures::task::Stop;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;
use tokio_stream::wrappers::IntervalStream;

impl Server {
	pub async fn try_get_build_status_stream(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::status::Event>> + Send + 'static>>
	{
		if let Some(status) = self.try_get_build_status_local_stream(id).await? {
			Ok(Some(status.left_stream()))
		} else if let Some(status) = self.try_get_build_status_remote(id).await? {
			let status = status.right_stream();
			Ok(Some(status))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn try_get_build_status_local_stream(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::status::Event>> + Send + 'static>>
	{
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Subscribe to status events.
		let subject = format!("builds.{id}.status");
		let status = self
			.messenger
			.subscribe(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());

		// Create the interval.
		let interval =
			IntervalStream::new(tokio::time::interval(std::time::Duration::from_secs(60)))
				.map(|_| ());

		// Create the stream.
		let server = self.clone();
		let id = id.clone();
		let mut previous: Option<tg::build::Status> = None;
		let mut end = false;
		let stream = stream::select(status, interval)
			.boxed()
			.then(move |()| {
				let server = server.clone();
				let id = id.clone();
				async move { server.try_get_build_status_local_inner(&id).await }
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
			.take_while(move |result| {
				if end {
					return future::ready(false);
				}
				if matches!(result, Ok(tangram_client::build::Status::Finished)) {
					end = true;
				}
				future::ready(true)
			})
			.map_ok(tg::build::status::Event::Status)
			.chain(stream::once(future::ok(tg::build::status::Event::End)));

		Ok(Some(stream))
	}

	pub (crate) async fn try_get_build_status_local(&self, id: &tg::build::Id) -> tg::Result<Option<tg::build::Status>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Get the status.
		let status = self.try_get_build_status_local_inner(id).await?;
		Ok(Some(status))
	}

	async fn try_get_build_status_local_inner(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<tg::build::Status> {
		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
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
	) -> tg::Result<Option<impl Stream<Item = tg::Result<tg::build::status::Event>> + Send + 'static>>
	{
		let futures = self
			.remotes
			.iter()
			.map(|remote| {
				{
					let remote = remote.clone();
					let id = id.clone();
					async move {
						remote
							.get_build_status(&id)
							.await
							.map(futures::StreamExt::boxed)
					}
				}
				.boxed()
			})
			.collect_vec();
		if futures.is_empty() {
			return Ok(None);
		}
		let Ok((stream, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		let stream = stream
			.map_ok(tg::build::status::Event::Status)
			.chain(stream::once(future::ok(tg::build::status::Event::End)));
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

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Get the stream.
		let Some(stream) = handle.try_get_build_status_stream(&id).await? else {
			return Ok(http::Response::builder().not_found().empty().unwrap());
		};

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();
		let stop = async move { stop.stopped().await };
		let stream = stream.take_until(stop);

		// Create the body.
		let (content_type, body) = match accept
			.as_ref()
			.map(|accept| (accept.type_(), accept.subtype()))
		{
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let sse = stream.map(|result| match result {
					Ok(tg::build::status::Event::Status(data)) => {
						let data = serde_json::to_string(&data).unwrap();
						Ok::<_, tg::Error>(tangram_http::sse::Event {
							data,
							..Default::default()
						})
					},
					Ok(tg::build::status::Event::End) => {
						let event = "end".to_owned();
						Ok::<_, tg::Error>(tangram_http::sse::Event {
							event: Some(event),
							..Default::default()
						})
					},
					Err(error) => {
						let data = serde_json::to_string(&error).unwrap();
						let event = "error".to_owned();
						Ok::<_, tg::Error>(tangram_http::sse::Event {
							data,
							event: Some(event),
							..Default::default()
						})
					},
				});
				let body = Outgoing::sse(sse);
				(content_type, body)
			},
			_ => {
				return Err(tg::error!(?accept, "invalid accept header"));
			},
		};

		// Create the response.
		let response = http::Response::builder()
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}
}
