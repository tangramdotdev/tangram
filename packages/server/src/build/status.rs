use crate::{database::Database, postgres_params, sqlite_params, Http, Server};
use futures::{
	future,
	stream::{self, BoxStream},
	FutureExt, StreamExt, TryStreamExt,
};
use http_body_util::{BodyExt, StreamBody};
use tangram_client as tg;
use tangram_error::{error, Error, Result};
use tangram_util::http::{empty, not_found, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;
use tokio_stream::wrappers::IntervalStream;

impl Server {
	pub async fn try_get_build_status(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Status>>>> {
		if let Some(status) = self
			.try_get_build_status_local(id, arg.clone(), stop.clone())
			.await?
		{
			Ok(Some(status))
		} else if let Some(status) = self
			.try_get_build_status_remote(id, arg.clone(), stop.clone())
			.await?
		{
			Ok(Some(status))
		} else {
			Ok(None)
		}
	}

	pub(crate) async fn try_get_build_status_local(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Status>>>> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Create the event stream.
		let status = self.inner.messenger.subscribe_to_build_status(id).await?;
		let interval =
			IntervalStream::new(tokio::time::interval(std::time::Duration::from_secs(60)))
				.map(|_| ());
		let timeout = arg.timeout.map_or_else(
			|| future::pending().left_future(),
			|timeout| tokio::time::sleep(timeout).right_future(),
		);
		let stop = stop.map_or_else(
			|| future::pending().left_future(),
			|mut stop| async move { stop.wait_for(|stop| *stop).map(|_| ()).await }.right_future(),
		);
		let events = stream::once(future::ready(()))
			.chain(
				stream::select(status, interval)
					.take_until(timeout)
					.take_until(stop),
			)
			.boxed();

		// Create the stream.
		let server = self.clone();
		let id = id.clone();
		let stream = stream::try_unfold(
			(server, id, events, None, false),
			move |(server, id, mut events, mut previous, mut end)| async move {
				if end {
					return Ok(None);
				}
				let Some(()) = events.next().await else {
					return Ok(None);
				};
				let status = server.try_get_build_status_local_inner(&id).await?;
				if Some(status) == previous {
					return Ok::<_, Error>(Some((
						stream::iter(None),
						(server, id, events, previous, end),
					)));
				}
				previous = Some(status);
				if status == tg::build::Status::Finished {
					end = true;
				}
				Ok::<_, Error>(Some((
					stream::iter(Some(Ok(status))),
					(server, id, events, previous, end),
				)))
			},
		)
		.try_flatten()
		.boxed();

		Ok(Some(stream))
	}

	pub async fn try_get_build_status_local_inner(
		&self,
		id: &tg::build::Id,
	) -> Result<tg::build::Status> {
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					select status
					from builds
					where id = ?1;
				";
				let params = sqlite_params![id.to_string()];
				let mut statement = connection
					.prepare_cached(statement)
					.map_err(|error| error!(source = error, "Failed to prepare the query."))?;
				let rows = &mut statement
					.query(params)
					.map_err(|error| error!(source = error, "Failed to execute the statement."))?;
				let row = rows
					.next()
					.map_err(|error| error!(source = error, "Failed to get the row."))?
					.ok_or_else(|| error!("Expected a row."))?;
				let status = row
					.get::<_, String>(0)
					.map_err(|error| error!(source = error, "Failed to deserialize the column."))?
					.parse()?;
				Ok(status)
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = "
					select status
					from builds
					where id = $1;
				";
				let params = postgres_params![id.to_string()];
				let statement = connection
					.prepare_cached(statement)
					.await
					.map_err(|error| error!(source = error, "Failed to prepare the statement."))?;
				let rows = connection
					.query(&statement, params)
					.await
					.map_err(|error| error!(source = error, "Failed to execute the statement."))?;
				let row = rows
					.into_iter()
					.next()
					.ok_or_else(|| error!("Expected a row."))?;
				let status = row
					.try_get::<_, String>(0)
					.map_err(|error| error!(source = error, "Failed to deserialize the column."))?
					.parse()
					.map_err(|error| error!(source = error, "Failed to parse the status."))?;
				Ok(status)
			},
		}
	}

	async fn try_get_build_status_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Status>>>> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(None);
		};
		let Some(stream) = remote.try_get_build_status(id, arg, stop).await? else {
			return Ok(None);
		};
		Ok(Some(stream))
	}

	pub async fn set_build_status(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<()> {
		if self.try_set_build_status_local(user, id, status).await? {
			return Ok(());
		}
		if self.try_set_build_status_remote(user, id, status).await? {
			return Ok(());
		}
		Err(error!("Failed to get the build."))
	}

	async fn try_set_build_status_local(
		&self,
		_user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<bool> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(false);
		}

		// The status cannot be set to created.
		if status == tg::build::Status::Created {
			return Err(error!("The status cannot be set to created."));
		}

		// Get the previous status.
		let previous_status = match status {
			tg::build::Status::Created => unreachable!(),
			tg::build::Status::Queued => tg::build::Status::Created,
			tg::build::Status::Started => tg::build::Status::Queued,
			tg::build::Status::Finished => tg::build::Status::Started,
		};

		// Get the timestamp column.
		let timestamp_column = match status {
			tg::build::Status::Created => unreachable!(),
			tg::build::Status::Queued => "queued_at",
			tg::build::Status::Started => "started_at",
			tg::build::Status::Finished => "finished_at",
		};

		// Update the database.
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = &format!(
					"
						update builds
						set 
							status = ?1,
							{timestamp_column} = ?2 
						where id = ?3 and status = ?4;
					"
				);
				let status = status.to_string();
				let timestamp = time::OffsetDateTime::now_utc()
					.format(&Rfc3339)
					.map_err(|error| error!(source = error, "Failed to format the timestamp."))?;
				let id = id.to_string();
				let previous_status = previous_status.to_string();
				let params = sqlite_params![status, timestamp, id, previous_status];
				let mut statement = connection
					.prepare_cached(statement)
					.map_err(|error| error!(source = error, "Failed to prepare the query."))?;
				let n = statement
					.execute(params)
					.map_err(|error| error!(source = error, "Failed to execute the statement."))?;
				if n == 0 {
					return Err(error!("Cannot set the build's status."));
				}
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = &format!(
					"
						update builds
						set 
							status = $1,
							{timestamp_column} = $2 
						where id = $3 and status = $4;
					"
				);
				let status = status.to_string();
				let timestamp = time::OffsetDateTime::now_utc()
					.format(&Rfc3339)
					.map_err(|error| error!(source = error, "Failed to format the timestamp."))?;
				let id = id.to_string();
				let previous_status = previous_status.to_string();
				let params = postgres_params![status, timestamp, id, previous_status];
				let statement = connection
					.prepare_cached(statement)
					.await
					.map_err(|error| error!(source = error, "Failed to prepare the statement."))?;
				let n = connection
					.execute(&statement, params)
					.await
					.map_err(|error| error!(source = error, "Failed to execute the statement."))?;
				if n == 0 {
					return Err(error!("Cannot set the build's status."));
				}
			},
		}

		// Publish the message.
		self.inner.messenger.publish_to_build_status(id).await?;

		Ok(true)
	}

	async fn try_set_build_status_remote(
		&self,
		user: Option<&tg::User>,
		id: &tg::build::Id,
		status: tg::build::Status,
	) -> Result<bool> {
		let Some(remote) = self.inner.remote.as_ref() else {
			return Ok(false);
		};
		remote.set_build_status(user, id, status).await?;
		Ok(true)
	}
}

impl Http {
	pub async fn handle_get_build_status_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "status"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "Unexpected path."));
		};
		let id = id
			.parse()
			.map_err(|error| error!(source = error, "Failed to parse the ID."))?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|error| error!(source = error, "Failed to deserialize the search params."))?
			.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.headers()
			.get(http::header::ACCEPT)
			.map(|accept| {
				let accept = accept
					.to_str()
					.map_err(|error| error!(source = error, "Invalid content type."))?;
				let accept = accept
					.parse::<mime::Mime>()
					.map_err(|error| error!(source = error, "Invalid content type."))?;
				Ok::<_, Error>(accept)
			})
			.transpose()?;
		let Some(accept) = accept else {
			return Err(error!("The accept header must be set."));
		};

		let stop = request.extensions().get().cloned();
		let Some(stream) = self.inner.tg.try_get_build_status(&id, arg, stop).await? else {
			return Ok(not_found());
		};

		// Choose the content type.
		let content_type = match (accept.type_(), accept.subtype()) {
			(mime::TEXT, mime::EVENT_STREAM) => mime::TEXT_EVENT_STREAM,
			(header, subtype) => return Err(error!(%header, %subtype, "Invalid accept header.")),
		};

		// Create the body.
		let body = stream
			.inspect_err(|error| {
				let trace = error.trace();
				tracing::error!(%trace, "Failed to get build status.");
			})
			.map_ok(|chunk| {
				let data = serde_json::to_string(&chunk).unwrap();
				let event = tangram_util::sse::Event::with_data(data);
				hyper::body::Frame::data(event.to_string().into())
			})
			.map_err(Into::into);
		let body = Outgoing::new(StreamBody::new(body));

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.header(http::header::CONTENT_TYPE, content_type.to_string())
			.body(body)
			.unwrap();

		Ok(response)
	}

	pub async fn handle_set_build_status_request(
		&self,
		request: http::Request<Incoming>,
	) -> Result<hyper::Response<Outgoing>> {
		// Get the path params.
		let path_components: Vec<&str> = request.uri().path().split('/').skip(1).collect();
		let ["builds", id, "status"] = path_components.as_slice() else {
			let path = request.uri().path();
			return Err(error!(%path, "Unexpected path."));
		};
		let build_id: tg::build::Id = id
			.parse()
			.map_err(|error| error!(source = error, "Failed to parse the ID."))?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|error| error!(source = error, "Failed to read the body."))?
			.to_bytes();
		let status = serde_json::from_slice(&bytes)
			.map_err(|error| error!(source = error, "Failed to deserialize the body."))?;

		self.inner
			.tg
			.set_build_status(user.as_ref(), &build_id, status)
			.await?;

		// Create the response.
		let response = http::Response::builder()
			.status(http::StatusCode::OK)
			.body(empty())
			.unwrap();

		Ok(response)
	}
}
