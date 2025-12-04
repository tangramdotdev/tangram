use {
	crate::{Context, Server},
	futures::{future, stream, StreamExt as _},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::{pin::pin, time::Duration},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::task::Stop,
	tangram_http::{request::Ext as _, Body},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

impl Server {
	pub(crate) async fn try_dequeue_process_with_context(
		&self,
		context: &Context,
		_arg: tg::process::dequeue::Arg,
	) -> tg::Result<Option<tg::process::dequeue::Output>> {
		if context.process.is_some() {
			return Err(tg::error!("forbidden"));
		}
		// Create the event stream.
		let created = self
			.messenger
			.subscribe("processes.created".to_owned(), Some("queue".to_owned()))
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ());
		let created = pin!(created);
		let interval =
			IntervalStream::new(tokio::time::interval(Duration::from_secs(60))).map(|_| ());
		let mut events = stream::select(created, interval);

		// Attempt to dequeue a process after each event.
		while let Some(()) = events.next().await {
			let connection = self
				.database
				.write_connection()
				.await
				.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
			let p = connection.p();
			let statement = formatdoc!(
				"
					update processes
					set
						status = 'dequeued',
						dequeued_at = {p}1
					where id in (
						select id
						from processes
						where
							status = 'enqueued' or
							(status = 'dequeued' and dequeued_at <= {p}2)
						order by enqueued_at
						limit 1
					)
					returning id;
				"
			);
			let now = time::OffsetDateTime::now_utc().unix_timestamp();
			let timeout = self.config.advanced.process_dequeue_timeout;
			let time = now - timeout.as_secs().to_i64().unwrap();
			let params = db::params![now, time];
			let Some(id) = connection
				.query_optional_value_into::<db::value::Serde<tg::process::Id>>(
					statement.into(),
					params,
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
				.map(|value| value.0)
			else {
				continue;
			};
			return Ok(Some(tg::process::dequeue::Output { process: id }));
		}

		Ok(None)
	}

	pub(crate) async fn handle_dequeue_process_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
	) -> tg::Result<http::Response<Body>> {
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Parse the arg.
		let arg = request.json().await?;

		// Get the stream.
		let handle = self.clone();
		let context = context.clone();
		let future = async move { handle.try_dequeue_process_with_context(&context, arg).await };
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
