use crate::Server;
use futures::{Stream, StreamExt as _, TryStreamExt as _, stream};
use indoc::formatdoc;
use tangram_client::{self as tg};
use tangram_database::{self as db, Database as _, Query as _};
use tangram_either::Either;
use tangram_futures::task::Stop;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger as messenger;

impl Server {
	pub async fn get_pty_size(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::read::Arg,
	) -> tg::Result<Option<tg::pty::Size>> {
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			return remote.get_pty_size(id, arg).await;
		}
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		#[derive(serde::Deserialize)]
		struct Row {
			window_size: db::value::Json<tg::pty::Size>,
		}
		let p = connection.p();
		let statement = formatdoc!(
			"
				select window_size
				from ptys
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let Some(row) = connection
			.query_optional_into::<Row>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the query"))?
		else {
			return Ok(None);
		};
		Ok(Some(row.window_size.0))
	}

	pub async fn read_pty(
		&self,
		id: &tg::pty::Id,
		mut arg: tg::pty::read::Arg,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote.take() {
			let remote = self.get_remote_client(remote).await?;
			let stream = remote.read_pty(id, arg).await?.boxed();
			return Ok(stream);
		}
		let deleted = self.pty_deleted(id.clone());

		// Create the stream from the messenger.
		let stream = match &self.messenger {
			Either::Left(messenger) => self
				.read_pty_memory(messenger, id, arg.master)
				.await
				.map_err(|source| tg::error!(!source, "failed to get pipe stream"))?
				.left_stream(),
			Either::Right(messenger) => self
				.read_pty_nats(messenger, id, arg.master)
				.await
				.map_err(|source| tg::error!(!source, "failed to get pipe stream"))?
				.right_stream(),
		};

		Ok(stream
			.take_until(deleted)
			.chain(stream::once(async move {
				Ok::<_, tg::Error>(tg::pty::Event::End)
			}))
			.boxed())
	}

	async fn read_pty_memory(
		&self,
		messenger: &messenger::memory::Messenger,
		id: &tg::pty::Id,
		master: bool,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static> {
		let subject = if master {
			format!("{id}.master")
		} else {
			format!("{id}.slave")
		};
		let stream = messenger
			.streams()
			.subscribe(subject)
			.await
			.map_err(|source| tg::error!(!source, "the pipe was closed or does not exist"))?
			.map(|message| {
				serde_json::from_slice::<tg::pty::Event>(&message.payload)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))
			})
			.boxed();
		Ok(stream)
	}

	async fn read_pty_nats(
		&self,
		messenger: &messenger::nats::Messenger,
		id: &tg::pty::Id,
		master: bool,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::pty::Event>> + Send + 'static> {
		let subject = if master {
			format!("{id}_master")
		} else {
			format!("{id}_slave")
		};
		let stream = messenger
			.jetstream
			.get_stream(subject)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?;

		// Get the consumer.
		let name = tg::Id::new_uuidv7(tg::id::Kind::Pty);
		let consumer_config = async_nats::jetstream::consumer::pull::Config {
			durable_name: Some(name.to_string()),
			..Default::default()
		};
		let consumer = stream
			.get_or_create_consumer(&id.to_string(), consumer_config)
			.await
			.map_err(|source| tg::error!(!source, "failed to get the index consumer"))?;

		// Create the stream.
		let stream = consumer
			.stream()
			.messages()
			.await
			.map_err(|source| tg::error!(!source, "failed to get the stream"))?
			.map_err(|source| tg::error!(!source, "failed to get message"))
			.and_then(|message| async move {
				message
					.ack()
					.await
					.map_err(|source| tg::error!(!source, "failed to ack message"))?;
				let event = serde_json::from_slice::<tg::pty::Event>(&message.payload)
					.map_err(|source| tg::error!(!source, "failed to deserialize the event"))?;
				Ok::<_, tg::Error>(event)
			});

		Ok(stream)
	}
}

impl Server {
	pub(crate) async fn handle_get_pty_size_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;
		let arg = request
			.json()
			.await
			.map_err(|source| tg::error!(!source, "failed to parse the body"))?;
		let window_size = handle.get_pty_size(&id, arg).await?;
		let response = http::Response::builder().json(window_size).unwrap();
		Ok(response)
	}

	pub(crate) async fn handle_read_pty_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the ID.
		let id = id.parse()?;

		// Get the query.
		let arg = request.query_params().transpose()?.unwrap_or_default();

		// Get the stream.
		let stream = handle.read_pty(&id, arg).await?;

		// Stop the stream when the server stops.
		let stop = request.extensions().get::<Stop>().cloned().unwrap();

		let stop = async move {
			stop.wait().await;
		};
		let stream = stream.take_until(stop);

		// Create the body.
		let body = Body::with_stream(stream.map(move |result| {
			let event = match result {
				Ok(event) => match event {
					tg::pty::Event::Chunk(bytes) => hyper::body::Frame::data(bytes),
					tg::pty::Event::Size(window_size) => {
						let mut trailers = http::HeaderMap::new();
						trailers
							.insert("x-tg-event", http::HeaderValue::from_static("window-size"));
						let json = serde_json::to_string(&window_size).unwrap();
						trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
						hyper::body::Frame::trailers(trailers)
					},
					tg::pty::Event::End => {
						let mut trailers = http::HeaderMap::new();
						trailers.insert("x-tg-event", http::HeaderValue::from_static("end"));
						hyper::body::Frame::trailers(trailers)
					},
				},
				Err(error) => {
					let mut trailers = http::HeaderMap::new();
					trailers.insert("x-tg-event", http::HeaderValue::from_static("error"));
					let json = serde_json::to_string(&error).unwrap();
					trailers.insert("x-tg-data", http::HeaderValue::from_str(&json).unwrap());
					hyper::body::Frame::trailers(trailers)
				},
			};
			Ok::<_, tg::Error>(event)
		}));

		// Create the response.
		let response = http::Response::builder().body(body).unwrap();

		Ok(response)
	}
}
