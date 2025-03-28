use crate::Server;
use futures::{Stream, StreamExt as _, TryFutureExt, TryStreamExt as _, future};
use indoc::formatdoc;
use tangram_client::{self as tg};
use tangram_database::{self as db, Database as _, Query as _};
use tangram_futures::task::Stop;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use tangram_messenger::Messenger as _;

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

		// Create the stream from the messenger.
		let stream = if arg.master {
			format!("{id}_master")
		} else {
			format!("{id}_slave")
		};
		let name = tg::Id::new_uuidv7(tg::id::Kind::Pty);
		let stream = self
			.messenger
			.stream_subscribe(stream, Some(name.to_string()))
			.await
			.map_err(|source| tg::error!(!source, "the pty was closed or does not exist"))?
			.map_err(|source| tg::error!(!source, "stream error"))
			.and_then(|message| {
				future::ready({
					serde_json::from_slice::<tg::pty::Event>(&message.payload)
						.map_err(|source| tg::error!(!source, "failed to deserialize the event"))
				})
			})
			.boxed();

		let deleted = self
			.pty_deleted(id.clone())
			.inspect_err(|error| tracing::error!(?error, "failed to check if pty was deleted"));

		Ok(stream.take_until(deleted).boxed())
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
