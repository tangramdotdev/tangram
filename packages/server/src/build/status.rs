use crate::{
	util::http::{empty, not_found, Incoming, Outgoing},
	Http, Server,
};
use futures::{
	future,
	stream::{self, BoxStream},
	FutureExt, StreamExt, TryStreamExt,
};
use http_body_util::{BodyExt, StreamBody};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_error::{error, Error, Result};
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
		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

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
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(status)
	}

	async fn try_get_build_status_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::status::GetArg,
		stop: Option<tokio::sync::watch::Receiver<bool>>,
	) -> Result<Option<BoxStream<'static, Result<tg::build::Status>>>> {
		let Some(remote) = self.inner.remotes.first() else {
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
		Err(error!("failed to get the build"))
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
			return Err(error!("the status cannot be set to created"));
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

		// Get a database connection.
		let connection = self
			.inner
			.database
			.connection()
			.await
			.map_err(|source| error!(!source, "failed to get a database connection"))?;

		// Update the database.
		let p = connection.p();
		let statement = format!(
			"
				update builds
				set 
					status = {p}1,
					{timestamp_column} = {p}2 
				where id = {p}3 and status = {p}4
				returning id;
			"
		);
		let timestamp = time::OffsetDateTime::now_utc();
		let params = db::params![
			status,
			timestamp.format(&Rfc3339).unwrap(),
			id,
			previous_status,
		];
		connection
			.query_one(statement, params)
			.await
			.map_err(|source| error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

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
		let Some(remote) = self.inner.remotes.first() else {
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
			return Err(error!(%path, "unexpected path"));
		};
		let id = id
			.parse()
			.map_err(|source| error!(!source, "failed to parse the ID"))?;

		// Get the search params.
		let arg = request
			.uri()
			.query()
			.map(serde_urlencoded::from_str)
			.transpose()
			.map_err(|source| error!(!source, "failed to deserialize the search params"))?
			.unwrap_or_default();

		// Get the accept header.
		let accept = request
			.headers()
			.get(http::header::ACCEPT)
			.map(|accept| {
				let accept = accept
					.to_str()
					.map_err(|source| error!(!source, "invalid content type"))?;
				let accept = accept
					.parse::<mime::Mime>()
					.map_err(|source| error!(!source, "invalid content type"))?;
				Ok::<_, Error>(accept)
			})
			.transpose()?;

		let stop = request.extensions().get().cloned().unwrap();
		let Some(stream) = self
			.inner
			.tg
			.try_get_build_status(&id, arg, Some(stop))
			.await?
		else {
			return Ok(not_found());
		};

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
						let event = tangram_sse::Event::with_data(data);
						hyper::body::Frame::data(event.to_string().into())
					})
					.map_err(Into::into);
				(content_type, body)
			},
			_ => {
				return Err(error!(?accept, "invalid accept header"));
			},
		};
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
			return Err(error!(%path, "unexpected path"));
		};
		let build_id: tg::build::Id = id
			.parse()
			.map_err(|source| error!(!source, "failed to parse the ID"))?;

		// Get the user.
		let user = self.try_get_user_from_request(&request).await?;

		// Read the body.
		let bytes = request
			.into_body()
			.collect()
			.await
			.map_err(|source| error!(!source, "failed to read the body"))?
			.to_bytes();
		let status = serde_json::from_slice(&bytes)
			.map_err(|source| error!(!source, "failed to deserialize the body"))?;

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
