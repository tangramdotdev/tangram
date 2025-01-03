use crate::{store::Store, Server};
use bytes::Bytes;
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use indoc::indoc;
use num::ToPrimitive as _;
use std::{pin::pin, sync::Arc};
use tangram_client::{self as tg, Handle as _};
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_futures::{read::Ext as _, stream::Ext};
use tangram_http::{incoming::request::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;
use tokio::{io::AsyncRead, task::JoinSet};
use tokio_postgres as postgres;
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
			let remote = self.get_remote_client(remote).await?;
			let arg = tg::object::import::Arg { remote: None };
			let stream = remote.import_object(arg, reader).await?.boxed();
			return Ok(stream.left_stream());
		}

		// Create the progress.
		let progress = crate::progress::Handle::new();

		// Spawn a task for the import.
		let task = tokio::spawn({
			let server = self.clone();
			let progress = progress.clone();
			async move {
				match server
					.import_object_inner(arg, reader, progress.clone())
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

		Ok(stream.right_stream())
	}

	async fn import_object_inner(
		&self,
		_arg: tg::object::import::Arg,
		reader: impl AsyncRead + Send + 'static,
		progress: crate::progress::Handle<tg::object::import::Output>,
	) -> tg::Result<tg::object::import::Output> {
		progress.start(
			"bytes".into(),
			"bytes".into(),
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

		// Create the stream.
		let stream = self.extract_object(reader).await?;

		if let Some(store) = &self.store {
			let object = self.import_object_inner_store(stream, store).await?;
			let output = tg::object::import::Output { object };
			return Ok(output);
		}

		// Get a database connection.
		let mut connection = self
			.database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let object = match connection.as_mut() {
			Either::Left(connection) => self.import_object_inner_sqlite(stream, connection).await?,
			Either::Right(connection) => {
				self.import_object_inner_postgres(stream, connection)
					.await?
			},
		};

		// Drop the database connection.
		drop(connection);

		let output = tg::object::import::Output { object };

		Ok(output)
	}

	async fn import_object_inner_sqlite(
		&self,
		stream: impl Stream<Item = tg::Result<(tg::object::Id, Bytes)>> + Send + 'static,
		connection: &mut db::sqlite::Connection,
	) -> tg::Result<tg::object::Id> {
		let mut object = None;
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let statement = "
			insert into objects (id, bytes, touched_at)
			values (?1, ?2, ?3)
			on conflict (id) do update set touched_at = ?3;
		";
		let mut stream = pin!(stream);
		while let Some((id, bytes)) = stream.try_next().await? {
			if object.is_none() {
				object.replace(id.clone());
			}
			let params = db::params![id, bytes, now];
			transaction
				.execute(statement.to_owned(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		let object = object.ok_or_else(|| tg::error!("the archive is empty"))?;

		Ok(object)
	}

	async fn import_object_inner_postgres(
		&self,
		stream: impl Stream<Item = tg::Result<(tg::object::Id, Bytes)>> + Send + 'static,
		connection: &mut db::postgres::Connection,
	) -> tg::Result<tg::object::Id> {
		let client = connection.client();
		let mut object = None;
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();

		let mut stream = pin!(stream);
		loop {
			let mut ids = Vec::new();
			let mut bytes = Vec::new();
			for _ in 0..500 {
				let Some((id, bytes_)) = stream.try_next().await? else {
					break;
				};
				if object.is_none() {
					object.replace(id.clone());
				}
				ids.push(id.to_string());
				bytes.push(bytes_.to_vec());
			}

			// If there were no objects, then break.
			if ids.is_empty() {
				break;
			}

			// Insert the objects.
			let statement = indoc!(
				"
					insert into objects (id, bytes, touched_at)
					select id, bytes, $3
					from unnest($1::text[], $2::blob[]) as t (id, bytes)
					on conflict (id) do update set touched_at = excluded.touched_at;
				"
			);
			client
				.execute(statement, &[&ids, &bytes, &now])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Get the root object.
		let object = object.ok_or_else(|| tg::error!("the archive is empty"))?;

		Ok(object)
	}

	#[allow(dead_code)]
	async fn import_object_inner_postgres_with_copy(
		&self,
		stream: impl Stream<Item = tg::Result<(tg::object::Id, Bytes)>> + Send + 'static,
		connection: &mut db::postgres::Connection,
	) -> tg::Result<tg::object::Id> {
		let client = connection.client();
		let mut object = None;
		let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();

		let mut stream = pin!(stream);
		loop {
			// Create a temporary table to copy the objects to.
			let statement = indoc!(
				"
					create temporary table objects_temporary (id text primary key, bytes blob, touched_at text);
				"
			);
			client
				.execute(statement, &[])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Create a writer to copy the objects.
			let sink = client
				.copy_in("copy objects_temporary (id, bytes, touched_at) from stdin binary")
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let types = &[
				postgres::types::Type::TEXT,
				postgres::types::Type::BYTEA,
				postgres::types::Type::TEXT,
			];
			let writer = tokio_postgres::binary_copy::BinaryCopyInWriter::new(sink, types);
			let mut writer = pin!(writer);
			let mut i = 0;
			while let Some((id, bytes)) = stream.try_next().await? {
				if i == 1000 {
					break;
				}
				if object.is_none() {
					object.replace(id.clone());
				}
				writer
					.as_mut()
					.write(&[&id.to_string(), &bytes.as_ref(), &now])
					.await
					.map_err(|source| tg::error!(!source, "failed to write the row"))?;
				i += 1;
			}

			// Finish copying the object.
			writer
				.finish()
				.await
				.map_err(|source| tg::error!(!source, "failed to finish writing"))?;

			// Move the objects from the temporary table to the objects table.
			let statement = indoc!(
				"
					insert into objects (id, bytes, touched_at)
					select (id, bytes, touched_at) from objects_temporary
					on conflict (id) do update set touched_at = excluded.touched_at;
				"
			);
			let n = client
				.execute(statement, &[])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// If there were no objects moved, then break.
			if n == 0 {
				break;
			}
		}

		// Get the root object.
		let object = object.ok_or_else(|| tg::error!("the archive is empty"))?;

		Ok(object)
	}

	async fn import_object_inner_store(
		&self,
		stream: impl Stream<Item = tg::Result<(tg::object::Id, Bytes)>> + Send + 'static,
		store: &Arc<Store>,
	) -> tg::Result<tg::object::Id> {
		let mut object = None;
		let mut tasks = JoinSet::new();
		let mut stream = pin!(stream);
		while let Some((id, bytes)) = stream.try_next().await? {
			if object.is_none() {
				object.replace(id.clone());
			}
			tasks.spawn({
				let store = store.clone();
				async move { store.put(id, bytes).await }
			});
		}
		while let Some(result) = tasks.join_next().await {
			result.unwrap()?;
		}
		let object = object.ok_or_else(|| tg::error!("the archive is empty"))?;
		Ok(object)
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
