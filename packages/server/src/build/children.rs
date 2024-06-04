use crate::Server;
use bytes::Bytes;
use futures::{future, stream, FutureExt as _, Stream, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use itertools::Itertools as _;
use num::ToPrimitive as _;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_futures::task::Stop;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use tangram_messenger::Messenger as _;
use tg::Handle as _;
use tokio_stream::wrappers::IntervalStream;

impl Server {
	pub async fn try_get_build_children_stream(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::children::Event>> + Send + 'static>,
	> {
		if let Some(children) = self.try_get_build_children_local(id, arg.clone()).await? {
			Ok(Some(children.left_stream()))
		} else if let Some(children) = self.try_get_build_children_remote(id, arg.clone()).await? {
			let children = children.right_stream();
			Ok(Some(children))
		} else {
			Ok(None)
		}
	}

	async fn try_get_build_children_local(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::children::Event>> + Send + 'static>,
	> {
		// Verify the build is local.
		if !self.get_build_exists_local(id).await? {
			return Ok(None);
		}

		// Create the channel.
		let (sender, receiver) = async_channel::unbounded();

		// Spawn the task.
		let server = self.clone();
		let id = id.clone();
		tokio::spawn(async move {
			let result = server
				.try_get_build_children_local_task(&id, arg, sender.clone())
				.await;
			if let Err(error) = result {
				sender.try_send(Err(error)).ok();
			}
		});

		Ok(Some(receiver))
	}

	async fn try_get_build_children_local_task(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
		sender: async_channel::Sender<tg::Result<tg::build::children::Event>>,
	) -> tg::Result<()> {
		// Get the position.
		let position = match arg.position {
			Some(std::io::SeekFrom::Start(seek)) => seek,
			Some(std::io::SeekFrom::End(seek) | std::io::SeekFrom::Current(seek)) => self
				.try_get_build_children_local_current_position(id)
				.await?
				.to_i64()
				.unwrap()
				.checked_add(seek)
				.ok_or_else(|| tg::error!("invalid offset"))?
				.to_u64()
				.ok_or_else(|| tg::error!("invalid offset"))?,
			None => 0,
		};

		// Subscribe to children events.
		let subject = format!("builds.{id}.children");
		let children = self
			.messenger
			.subscribe(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		// Subscribe to status events.
		let subject = format!("builds.{id}.status");
		let status = self
			.messenger
			.subscribe(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		// Create the interval.
		let interval =
			IntervalStream::new(tokio::time::interval(std::time::Duration::from_secs(60)))
				.map(|_| ())
				.boxed();

		// Create the events stream.
		let mut events = stream::select_all([children, status, interval]).boxed();

		// Create the state.
		let size = arg.size.unwrap_or(10);
		let mut position = position;
		let mut read = 0;

		// Send the events.
		loop {
			// Get the build's status.
			let status = self
				.try_get_build_status_local(id)
				.await?
				.unwrap()
				.boxed()
				.try_next()
				.await?
				.unwrap()
				.try_unwrap_data()
				.unwrap();

			// Send as many data events as possible.
			loop {
				// Determine the size.
				let size = match arg.length {
					None => size,
					Some(length) => size.min(length - read),
				};

				// Read the data.
				let data = self
					.try_get_build_children_local_inner(&id, position, size)
					.await?;

				// If the data is empty, then break.
				if data.items.is_empty() {
					break;
				}

				// Update the state.
				position += data.items.len().to_u64().unwrap();
				read += data.items.len().to_u64().unwrap();

				// Send the data.
				let result = sender.try_send(Ok(tg::build::children::Event::Data(data)));
				if result.is_err() {
					return Ok(());
				}
			}

			// If the build was finished or the length was reached, then send the end event and break.
			let end = arg.length.is_some_and(|length| read >= length);
			if end || status == tg::build::Status::Finished {
				let result = sender.try_send(Ok(tg::build::children::Event::End));
				if result.is_err() {
					return Ok(());
				}
				break;
			}

			// Wait for an event before returning to the top of the loop.
			events.next().await;
		}

		Ok(())
	}

	async fn try_get_build_children_local_current_position(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<u64> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the position.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count(*)
				from build_children
				where build = {p}1;
			"
		);
		let params = db::params![id];
		let position = connection
			.query_one_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(position)
	}

	async fn try_get_build_children_local_inner(
		&self,
		id: &tg::build::Id,
		position: u64,
		length: u64,
	) -> tg::Result<tg::build::children::Data> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the children.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select child
				from build_children
				where build = {p}1
				order by position
				limit {p}2
				offset {p}3
			"
		);
		let params = db::params![id, length, position,];
		let children = connection
			.query_all_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Create the chunk.
		let chunk = tg::build::children::Data {
			position,
			items: children,
		};

		Ok(chunk)
	}

	async fn try_get_build_children_remote(
		&self,
		id: &tg::build::Id,
		arg: tg::build::children::Arg,
	) -> tg::Result<
		Option<impl Stream<Item = tg::Result<tg::build::children::Event>> + Send + 'static>,
	> {
		let futures = self
			.remotes
			.iter()
			.map(|remote| {
				{
					let remote = remote.clone();
					let id = id.clone();
					let arg = arg.clone();
					async move {
						remote
							.get_build_children(&id, arg)
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
			.map_ok(tg::build::children::Event::Data)
			.chain(stream::once(future::ok(tg::build::children::Event::End)));
		Ok(Some(stream))
	}

	pub async fn add_build_child(
		&self,
		build_id: &tg::build::Id,
		child_id: &tg::build::Id,
	) -> tg::Result<()> {
		// Verify the build is local.
		if !self.get_build_exists_local(build_id).await? {
			return Err(tg::error!("failed to find the build"));
		}

		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Add the child to the database.
		let p = connection.p();
		let statement = formatdoc!(
			"
				insert into build_children (build, position, child)
				values ({p}1, (select coalesce(max(position) + 1, 0) from build_children where build = {p}1), {p}2)
				on conflict (build, child) do nothing;
			"
		);
		let params = db::params![build_id, child_id];
		connection
			.execute(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		// Publish the message.
		tokio::spawn({
			let server = self.clone();
			let build_id = build_id.clone();
			async move {
				let subject = format!("builds.{build_id}.children");
				let payload = Bytes::new();
				server
					.messenger
					.publish(subject, payload)
					.await
					.inspect_err(|error| tracing::error!(%error, "failed to publish"))
					.ok();
			}
		});

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_get_build_children_request<H>(
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
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request.parse_header(http::header::ACCEPT).transpose()?;

		// Get the stream.
		let Some(stream) = handle.try_get_build_children_stream(&id, arg).await? else {
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
			None | Some((mime::STAR, mime::STAR) | (mime::APPLICATION, mime::JSON)) => {
				let content_type = mime::APPLICATION_JSON;
				let future = async move {
					let children: Vec<tg::build::Id> = stream
						.take_while(|event| {
							future::ready(!matches!(event, Ok(tg::build::children::Event::End)))
						})
						.map_ok(|event| {
							let tg::build::children::Event::Data(chunk) = event else {
								unreachable!()
							};
							stream::iter(chunk.items.into_iter()).map(Ok::<_, tg::Error>)
						})
						.try_flatten()
						.try_collect()
						.await?;
					let json = serde_json::to_string(&children)
						.map_err(|source| tg::error!(!source, "failed to serialize the body"))?;
					Ok::<_, tg::Error>(json)
				};
				let body = Outgoing::future_json(future);
				(content_type, body)
			},
			Some((mime::TEXT, mime::EVENT_STREAM)) => {
				let content_type = mime::TEXT_EVENT_STREAM;
				let sse = stream.map(|result| match result {
					Ok(tg::build::children::Event::Data(data)) => {
						let data = serde_json::to_string(&data).unwrap();
						Ok::<_, tg::Error>(tangram_http::sse::Event {
							data,
							..Default::default()
						})
					},
					Ok(tg::build::children::Event::End) => {
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

	pub(crate) async fn handle_add_build_child_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let build_id = id.parse()?;
		let child_id = request.json().await?;
		handle.add_build_child(&build_id, &child_id).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
