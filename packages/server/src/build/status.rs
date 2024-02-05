use crate::{database::Database, postgres_params, sqlite_params, Http, Messenger, Server};
use bytes::Bytes;
use futures::{
	future,
	stream::{self, BoxStream},
	FutureExt, StreamExt, TryStreamExt,
};
use http_body_util::{BodyExt, StreamBody};
use tangram_client as tg;
use tangram_error::{error, Error, Result, WrapErr};
use tangram_util::http::{empty, not_found, Incoming, Outgoing};
use tokio_stream::wrappers::{IntervalStream, WatchStream};

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
		let status = match &self.inner.messenger {
			Messenger::Local(local) => {
				let channels = local.builds.read().unwrap().get(id).cloned();
				channels
					.map_or_else(
						|| stream::empty().left_stream(),
						|channels| {
							WatchStream::from_changes(channels.status.subscribe()).right_stream()
						},
					)
					.chain(stream::pending())
					.left_stream()
			},
			Messenger::Nats(nats) => {
				let subject = format!("builds.{id}.status");
				nats.client
					.subscribe(subject)
					.await
					.wrap_err("Failed to subscribe.")?
					.map(|_| ())
					.right_stream()
			},
		};
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
					.wrap_err("Failed to prepare the query.")?;
				let rows = &mut statement
					.query(params)
					.wrap_err("Failed to execute the statement.")?;
				let row = rows
					.next()
					.wrap_err("Failed to get the row.")?
					.wrap_err("Expected a row.")?;
				let status = row
					.get::<_, String>(0)
					.wrap_err("Failed to deserialize the column.")?
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
					.wrap_err("Failed to prepare the statement.")?;
				let rows = connection
					.query(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
				let row = rows.into_iter().next().wrap_err("Expected a row.")?;
				let status = row
					.try_get::<_, String>(0)
					.wrap_err("Failed to deserialize the column.")?
					.parse()
					.wrap_err("Failed to parse the status.")?;
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

		// Update the database.
		match &self.inner.database {
			Database::Sqlite(database) => {
				let connection = database.get().await?;
				let statement = "
					update builds
					set status = ?1
					where id = ?2;
				";
				let params = sqlite_params![status.to_string(), id.to_string()];
				let mut statement = connection
					.prepare_cached(statement)
					.wrap_err("Failed to prepare the query.")?;
				statement
					.execute(params)
					.wrap_err("Failed to execute the statement.")?;
			},

			Database::Postgres(database) => {
				let connection = database.get().await?;
				let statement = "
					update builds
					set status = $1
					where id = $2;
				";
				let params = postgres_params![status.to_string(), id.to_string()];
				let statement = connection
					.prepare_cached(statement)
					.await
					.wrap_err("Failed to prepare the statement.")?;
				connection
					.execute(&statement, params)
					.await
					.wrap_err("Failed to execute the statement.")?;
			},
		}

		// Send the event.
		match &self.inner.messenger {
			Messenger::Local(local) => {
				if let Some(channels) = local.builds.read().unwrap().get(id) {
					channels.status.send_replace(());
				}
			},
			Messenger::Nats(nats) => {
				let subject = format!("builds.{id}.status");
				nats.client
					.publish(subject, Bytes::new())
					.await
					.wrap_err("Failed to publish the message.")?;
			},
		}

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
			return Err(error!("Unexpected path."));
		};
		let id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.wrap_err("Failed to deserialize the search params.")?
			.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.headers()
			.get(http::header::ACCEPT)
			.map(|accept| {
				let accept = accept.to_str().wrap_err("Invalid content type.")?;
				let accept = accept
					.parse::<mime::Mime>()
					.wrap_err("Invalid content type.")?;
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
			_ => return Err(error!("Invalid accept header.")),
		};

		// Create the body.
		let body = stream
			.map_err(|error| {
				let trace = error.trace();
				tracing::error!("{trace}");
				error
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
			return Err(error!("Unexpected path."));
		};
		let build_id: tg::build::Id = id.parse().wrap_err("Failed to parse the ID.")?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.wrap_err("Failed to read the body.")?
			.to_bytes();
		let status = serde_json::from_slice(&bytes).wrap_err("Failed to deserialize the body.")?;

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
