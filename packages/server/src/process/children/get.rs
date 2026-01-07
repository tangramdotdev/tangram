use {
	crate::{Context, Server},
	futures::{FutureExt as _, Stream, StreamExt as _, TryStreamExt as _, future, stream},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::time::Duration,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_futures::{
		stream::Ext as _,
		task::{Stop, Task},
	},
	tangram_http::{Body, request::Ext as _, response::builder::Ext as _},
	tangram_messenger::prelude::*,
	tokio_stream::wrappers::IntervalStream,
};

impl Server {
	pub async fn try_get_process_children_stream_with_context(
		&self,
		_context: &Context,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<
			impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static + use<>,
		>,
	> {
		// Try local first if requested.
		if Self::local(arg.local, arg.remotes.as_ref())
			&& let Some(stream) = self
				.try_get_process_children_local(id, arg.clone())
				.await
				.map_err(|source| tg::error!(!source, "failed to get the process children"))?
		{
			return Ok(Some(stream.left_stream()));
		}

		// Try remotes.
		let remotes = self
			.remotes(arg.local, arg.remotes.clone())
			.await
			.map_err(|source| tg::error!(!source, "failed to get the remotes"))?;
		if let Some(stream) = self
			.try_get_process_children_remote(id, arg.clone(), &remotes)
			.await?
		{
			return Ok(Some(stream.right_stream()));
		}

		Ok(None)
	}

	async fn try_get_process_children_local(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
	) -> tg::Result<
		Option<
			impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static + use<>,
		>,
	> {
		// Verify the process is local.
		if !self
			.get_process_exists_local(id)
			.await
			.map_err(|source| tg::error!(!source, "failed to check if the process exists"))?
		{
			return Ok(None);
		}

		// Create the channel.
		let (sender, receiver) = async_channel::unbounded();

		// Spawn the task.
		let server = self.clone();
		let id = id.clone();
		let task = Task::spawn(|_| async move {
			let result = server
				.try_get_process_children_local_task(&id, arg, sender.clone())
				.await;
			if let Err(error) = result {
				sender.try_send(Err(error)).ok();
			}
		});

		let stream = receiver.attach(task);

		Ok(Some(stream))
	}

	async fn try_get_process_children_local_task(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
		sender: async_channel::Sender<tg::Result<tg::process::children::get::Event>>,
	) -> tg::Result<()> {
		// Get the position.
		let position = match arg.position {
			Some(std::io::SeekFrom::Start(seek)) => seek,
			Some(std::io::SeekFrom::End(seek) | std::io::SeekFrom::Current(seek)) => self
				.try_get_process_children_local_current_position(id)
				.await
				.map_err(|source| tg::error!(!source, "failed to get the current position"))?
				.to_i64()
				.unwrap()
				.checked_add(seek)
				.ok_or_else(|| tg::error!("invalid position"))?
				.to_u64()
				.ok_or_else(|| tg::error!("invalid position"))?,
			None => 0,
		};

		// Subscribe to children events.
		let subject = format!("processes.{id}.children");
		let children = self
			.messenger
			.subscribe::<()>(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		// Subscribe to status events.
		let subject = format!("processes.{id}.status");
		let status = self
			.messenger
			.subscribe::<()>(subject, None)
			.await
			.map_err(|source| tg::error!(!source, "failed to subscribe"))?
			.map(|_| ())
			.boxed();

		// Create the interval.
		let interval = IntervalStream::new(tokio::time::interval(Duration::from_secs(60)))
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
			// Get the process's status.
			let status = self
				.try_get_current_process_status_local(id)
				.await?
				.ok_or_else(|| tg::error!(process = %id, "process does not exist"))?;

			// Send as many data events as possible.
			loop {
				// Determine the size.
				let size = match arg.length {
					None => size,
					Some(length) => size.min(length - read),
				};

				// Read the chunk.
				let chunk = self
					.try_get_process_children_local_inner(id, position, size)
					.await?;

				// If the chunk is empty, then break.
				if chunk.data.is_empty() {
					break;
				}

				// Update the state.
				position += chunk.data.len().to_u64().unwrap();
				read += chunk.data.len().to_u64().unwrap();

				// Send the data.
				let result = sender.try_send(Ok(tg::process::children::get::Event::Chunk(chunk)));
				if result.is_err() {
					return Ok(());
				}
			}

			// If the process is finished or the length is reached, then send the end event and break.
			let end = arg.length.is_some_and(|length| read >= length);
			if end || status.is_finished() {
				let result = sender.try_send(Ok(tg::process::children::get::Event::End));
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

	async fn try_get_process_children_local_current_position(
		&self,
		id: &tg::process::Id,
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
				from process_children
				where process = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let position = connection
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(position)
	}

	async fn try_get_process_children_local_inner(
		&self,
		id: &tg::process::Id,
		position: u64,
		length: u64,
	) -> tg::Result<tg::process::children::get::Chunk> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the children.
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			child: tg::process::Id,
			#[tangram_database(as = "db::value::Json<tg::referent::Options>")]
			options: tg::referent::Options,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select child, options
				from process_children
				where process = {p}1
				order by position
				limit {p}2
				offset {p}3;
			"
		);
		let params = db::params![id.to_string(), length, position];
		let children = connection
			.query_all_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.into_iter()
			.map(|row| tg::Referent {
				item: row.child,
				options: row.options,
			})
			.collect();

		// Drop the database connection.
		drop(connection);

		// Create the chunk.
		let chunk = tg::process::children::get::Chunk {
			position,
			data: children,
		};

		Ok(chunk)
	}

	async fn try_get_process_children_remote(
		&self,
		id: &tg::process::Id,
		arg: tg::process::children::get::Arg,
		remotes: &[String],
	) -> tg::Result<
		Option<
			impl Stream<Item = tg::Result<tg::process::children::get::Event>> + Send + 'static + use<>,
		>,
	> {
		if remotes.is_empty() {
			return Ok(None);
		}
		let arg = tg::process::children::get::Arg {
			local: None,
			remotes: None,
			..arg
		};
		let futures = remotes.iter().map(|remote| {
			let remote = remote.clone();
			let arg = arg.clone();
			async move {
				let client = self.get_remote_client(remote.clone()).await.map_err(
					|source| tg::error!(!source, %remote, "failed to get the remote client"),
				)?;
				client
					.get_process_children(id, arg)
					.await
					.map(futures::StreamExt::boxed)
					.map_err(
						|source| tg::error!(!source, %remote, "failed to get the process children"),
					)
			}
			.boxed()
		});
		let Ok((stream, _)) = future::select_ok(futures).await else {
			return Ok(None);
		};
		let stream = stream
			.map_ok(tg::process::children::get::Event::Chunk)
			.chain(stream::once(future::ok(
				tg::process::children::get::Event::End,
			)));
		Ok(Some(stream))
	}

	pub(crate) async fn handle_get_process_children_request(
		&self,
		request: http::Request<Body>,
		context: &Context,
		id: &str,
	) -> tg::Result<http::Response<Body>> {
		// Parse the ID.
		let id = id
			.parse()
			.map_err(|source| tg::error!(!source, "failed to parse the process id"))?;

		// Get the query.
		let arg = request
			.query_params()
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the query params"))?
			.unwrap_or_default();

		// Get the accept header.
		let accept: Option<mime::Mime> = request
			.parse_header(http::header::ACCEPT)
			.transpose()
			.map_err(|source| tg::error!(!source, "failed to parse the accept header"))?;

		// Get the stream.
		let Some(stream) = self
			.try_get_process_children_stream_with_context(context, &id, arg)
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
