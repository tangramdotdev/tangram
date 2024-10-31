use crate::Server;
use futures::{future, stream, StreamExt as _};
use indoc::formatdoc;
use std::time::Duration;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_futures::task::Stop;
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;
use time::format_description::well_known::Rfc3339;
use tokio_stream::wrappers::IntervalStream;

impl Server {
	pub async fn try_dequeue_build(
		&self,
		_arg: tg::build::dequeue::Arg,
	) -> tg::Result<Option<tg::build::dequeue::Output>> {
		// Create the event stream.
		let created = self
			.messenger
			.subscribe("builds.created".to_owned(), Some("queue".to_owned()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());
		let interval =
			IntervalStream::new(tokio::time::interval(Duration::from_secs(60))).map(|_| ());
		let mut events = stream::select(created, interval);

		// Attempt to dequeue a build after each event.
		while let Some(()) = events.next().await {
			let connection = self
				.database
				.write_connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
			let p = connection.p();
			let statement = formatdoc!(
				"
					update builds
					set status = 'dequeued', dequeued_at = {p}1
					where id in (
						select id
						from builds
						where
							status = 'created' or
							(status = 'dequeued' and dequeued_at <= {p}2)
						order by created_at
						limit 1
					)
					returning id;
				"
			);
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			let timeout = self.config.advanced.build_dequeue_timeout;
			let time = (time::OffsetDateTime::now_utc() - timeout)
				.format(&Rfc3339)
				.unwrap();
			let params = db::params![now, time];
			let Some(id) = connection
				.query_optional_value_into(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			else {
				continue;
			};
			return Ok(Some(tg::build::dequeue::Output { build: id }));
		}

		Ok(None)
	}
}

impl Server {
	pub(crate) async fn handle_dequeue_build_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Parse the arg.
		let arg = request.json().await?;

		// Get the stream.
		let handle = handle.clone();
		let future = async move { handle.try_dequeue_build(arg).await };
		let stream = stream::once(future).filter_map(|option| future::ready(option.transpose()));

		// Stop the stream when the server stops.
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
				(Some(content_type), Outgoing::sse(stream))
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
