use crate::Server;
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use indoc::formatdoc;
use num::ToPrimitive as _;
use std::pin::pin;
use tangram_client::{self as tg, Handle as _};
use tangram_database::{self as db, prelude::*};
use tangram_futures::{read::Ext as _, stream::Ext};
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;
use tokio::io::AsyncRead;
use tokio_util::{io::InspectReader, task::AbortOnDropHandle};

impl Server {
	pub(crate) async fn import_object(
		&self,
		arg: tg::object::import::Arg,
		reader: impl AsyncRead + Unpin + Send + 'static,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::object::import::Output>>> + Send + 'static,
	> {
		if let Some(remote) = arg.remote.clone() {
			let stream = self
				.import_object_remote(arg, reader, remote)
				.await?
				.left_stream();
			Ok(stream)
		} else {
			let stream = self.import_object_local(arg, reader).await?.right_stream();
			Ok(stream)
		}
	}

	async fn import_object_remote(
		&self,
		arg: tg::object::import::Arg,
		reader: impl AsyncRead + Unpin + Send + 'static,
		remote: String,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::object::import::Output>>> + Send + 'static,
	> {
		let remote = self
			.remotes
			.get(&remote)
			.ok_or_else(|| tg::error!(%remote, "the remote does not exist"))?;
		remote
			.import_object(arg, reader)
			.await
			.map(futures::StreamExt::boxed)
	}

	async fn import_object_local(
		&self,
		arg: tg::object::import::Arg,
		reader: impl AsyncRead + Send + 'static,
	) -> tg::Result<
		impl Stream<Item = tg::Result<tg::progress::Event<tg::object::import::Output>>> + Send + 'static,
	> {
		// Create the progress.
		let progress = crate::progress::Handle::new();

		// Spawn a task for the import.
		let task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				match server
					.import_object_local_inner(arg, reader, progress.clone())
					.await
				{
					Ok(output) => progress.output(output),
					Err(error) => {
						progress.error(error);
					},
				}
			}
		});
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = progress.stream().attach(abort_handle);

		Ok(stream)
	}

	async fn import_object_local_inner(
		&self,
		_arg: tg::object::import::Arg,
		reader: impl AsyncRead + Send + 'static,
		progress: crate::progress::Handle<tg::object::import::Output>,
	) -> tg::Result<tg::object::import::Output> {
		progress.start(
			"bytes".into(),
			"Bytes".into(),
			tg::progress::IndicatorFormat::Bytes,
			Some(0),
			None,
		);

		// Create a reader that inspects progress.
		let reader = InspectReader::new(reader, {
			let progress = progress.clone();
			move |chunk| {
				progress.increment("bytes", chunk.len().to_u64().unwrap());
			}
		})
		.boxed();

		let stream = self.extract_object(reader).await?;
		let mut stream = pin!(stream);

		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let mut object = None;
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
		while let Some((id, bytes)) = stream.try_next().await? {
			if object.is_none() {
				object.replace(id.clone());
			}
			let p = transaction.p();
			let statement = formatdoc!(
				"
					insert into objects (id, bytes, touched_at)
					values ({p}1, {p}2, {p}3)
					on conflict (id) do update set touched_at = {p}3;
				"
			);
			let params = db::params![id, bytes, now];
			transaction
				.execute(statement, params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		let object = object.ok_or_else(|| tg::error!("the archive is empty"))?;

		let output = tg::object::import::Output { object };

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_object_import_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Get the query.
		let arg = request
			.query_params()
			.transpose()?
			.ok_or_else(|| tg::error!("failed to get the args"))?;

		// Get the body
		let body = request.reader();

		// Get the stream.
		let stream = handle.import_object(arg, body).await?;

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
