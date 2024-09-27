use std::time::Duration;
use crate::Server;
use futures::{stream, StreamExt as _};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
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
			IntervalStream::new(tokio::time::interval(Duration::from_secs(60)))
				.map(|_| ());
		let mut events = stream::select(created, interval);

		// Attempt to dequeue a build after each event.
		while let Some(()) = events.next().await {
			let connection = self
				.database
				.connection(db::Priority::Low)
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
			let timeout = self
				.options
				.advanced
				.build_dequeue_timeout
				.unwrap_or(Duration::from_secs(3600));
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
		let arg = request.json().await?;
		let handle = handle.clone();
		let future = async move { handle.try_dequeue_build(arg).await };
		let response = http::Response::builder().future_json(future).unwrap();
		Ok(response)
	}
}
