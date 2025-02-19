use crate::Server;
use futures::{future, stream, Stream, StreamExt as _, TryStreamExt as _};
use indoc::indoc;
use itertools::Itertools;
use num::ToPrimitive;
use std::pin::{pin, Pin};
use tangram_client as tg;
use tangram_database::{self as db, Database};
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{request::Ext as _, Body};
use time::format_description::well_known::Rfc3339;
use tokio::task::JoinSet;
use tokio_stream::wrappers::{ReceiverStream, UnboundedReceiverStream};
use tokio_util::task::AbortOnDropHandle;

impl Server {
	pub(crate) async fn post_objects(
		&self,
		mut stream: Pin<
			Box<dyn Stream<Item = tg::Result<tg::object::post::Item>> + Send + 'static>,
		>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::object::post::Event>> + Send + 'static> {
		let limit = 256;
		let (event_sender, event_receiver) =
			tokio::sync::mpsc::unbounded_channel::<tg::Result<tg::object::post::Event>>();
		let (database_sender, database_receiver) =
			tokio::sync::mpsc::channel::<tg::object::post::Item>(limit);
		let (store_sender, mut store_receiver) =
			tokio::sync::mpsc::channel::<tg::object::post::Item>(limit);
		let (complete_sender, complete_receiver) =
			tokio::sync::mpsc::channel::<tg::object::post::Item>(limit);

		// Create the complete task.
		let complete_task = tokio::spawn({
			let server = self.clone();
			let event_sender = event_sender.clone();
			async move {
				let stream = ReceiverStream::new(complete_receiver);
				let mut stream = pin!(stream);
				while let Some(item) = stream.next().await {
					let complete = server
						.try_get_object_metadata(&item.id)
						.await?
						.is_some_and(|metadata| metadata.complete);
					if complete {
						let event = tg::object::post::Event::Complete(item.id);
						event_sender.send(Ok(event)).ok();
					}
				}
				Ok(())
			}
		});

		// Create the database task.
		let database_task = tokio::spawn({
			let server = self.clone();
			async move {
				let stream = ReceiverStream::new(database_receiver);
				match &server.database {
					Either::Left(database) => {
						let mut connection =
							database.write_connection().await.map_err(|source| {
								tg::error!(!source, "failed to get a database connection")
							})?;
						server
							.insert_objects_sqlite(stream, &mut connection)
							.await
							.inspect_err(|error| eprintln!("failed to insert: {error}"))?;
					},
					Either::Right(database) => {
						let mut connection =
							database.write_connection().await.map_err(|source| {
								tg::error!(!source, "failed to get a database connection")
							})?;
						server
							.insert_objects_postgres(stream, &mut connection)
							.await?;
					},
				}
				Ok(())
			}
		});

		// Create the store task.
		let store_task = tokio::spawn({
			let server = self.clone();
			async move {
				let mut join_set = JoinSet::new();
				loop {
					let Some(item) = store_receiver.recv().await else {
						break;
					};
					if let Some(store) = server.store.clone() {
						join_set.spawn(async move { store.put(item.id, item.bytes).await });
						while let Some(result) = join_set.try_join_next() {
							result
								.map_err(|source| tg::error!(!source, "a store task panicked"))??;
						}
					}
				}
				while let Some(result) = join_set.join_next().await {
					result.map_err(|source| tg::error!(!source, "a store task panicked"))??;
				}
				Ok(())
			}
		});

		// Spawn a task that sends items from the stream to the other tasks.
		let task = tokio::spawn({
			let event_sender = event_sender.clone();
			async move {
				loop {
					let item = match stream.try_next().await {
						Ok(Some(item)) => item,
						Ok(None) => break,
						Err(error) => {
							event_sender.send(Err(error)).ok();
							return;
						},
					};
					let result = future::try_join3(
						complete_sender.send(item.clone()),
						database_sender.send(item.clone()),
						store_sender.send(item.clone()),
					)
					.await;
					if result.is_err() {
						event_sender
							.send(Err(tg::error!(?result, "failed to send the item")))
							.ok();
						return;
					}
				}

				// Close the channels
				drop(complete_sender);
				drop(database_sender);
				drop(store_sender);

				// Join the tasks.
				let result = future::try_join3(complete_task, database_task, store_task).await;
				if let (Err(error), _, _) | (_, Err(error), _) | (_, _, Err(error)) =
					result.unwrap()
				{
					event_sender.send(Err(error)).ok();
				}

				// Send the end event.
				event_sender.send(Ok(tg::object::post::Event::End)).ok();
			}
		});

		// Create the event stream.
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = UnboundedReceiverStream::new(event_receiver).attach(abort_handle);

		Ok(stream)
	}
}

impl Server {
	async fn insert_objects_sqlite(
		&self,
		stream: impl Stream<Item = tg::object::post::Item> + Send + 'static,
		connection: &mut db::sqlite::Connection,
	) -> tg::Result<()> {
		// Drain the stream and insert the data.
		let mut stream = pin!(stream);
		while let Some(item) = stream.next().await {
			// Get the children.
			let children = tg::object::Data::deserialize(item.id.kind(), &item.bytes)?.children();

			let size = item.bytes.len().to_u64().unwrap();
			let id = item.id.clone();
			connection
				.with(move |connection| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Insert the object children.
					let statement = indoc!(
						"
							insert into object_children (object, child)
							values (?1, ?2)
							on conflict (object, child) do nothing;
						"
					);
					let mut statement = transaction
						.prepare_cached(statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					for child in children {
						let child = child.to_string();
						let params = rusqlite::params![&id.to_string(), &child];
						statement.execute(params).map_err(|source| {
							tg::error!(!source, "failed to execute the statement")
						})?;
					}
					drop(statement);

					// Insert the object.
					let statement = indoc!(
						"
							insert into objects (id, bytes, size, touched_at)
							values (?1, ?2, ?3, ?4)
							on conflict (id) do update set touched_at = ?4;
						"
					);
					let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
					let bytes = item.bytes.as_ref();
					let params = rusqlite::params![&id.to_string(), bytes, size, now];
					let mut statement = transaction
						.prepare_cached(statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					drop(statement);

					// Commit the transaction.
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;

					Ok::<_, tg::Error>(())
				})
				.await?;
		}
		Ok(())
	}

	async fn insert_objects_postgres(
		&self,
		stream: impl Stream<Item = tg::object::post::Item> + Send + 'static,
		connection: &mut db::postgres::Connection,
	) -> tg::Result<()> {
		let client = connection.client();

		let stream = stream.chunks(500);
		let mut stream = pin!(stream);
		while let Some(chunk) = stream.next().await {
			let chunk: Vec<_> = chunk
				.into_iter()
				.map(|item| {
					// Get the children.
					let children =
						tg::object::Data::deserialize(item.id.kind(), &item.bytes)?.children();
					Ok::<_, tg::Error>((item.id, item.bytes, children))
				})
				.try_collect()?;
			let statement = indoc!(
				"
					with inserted_object_children as (
						insert into object_children (object, child)
						select ($1::text[])[parent_index], child
						from unnest($2::text[], $3::integer[]) as c (child, parent_index)
						on conflict (object, child) do nothing
					),
					inserted_objects as (
						insert into objects (id, bytes, size, touched_at)
						select id, bytes, size, $6
						from
							unnest($1::text[], $4::bytea[], $5::integer[]) as t (id, bytes, size)
						on conflict (id) do update set touched_at = $6
					)
					select 1;
				"
			);
			let ids = chunk
				.iter()
				.map(|(id, _, _)| id.to_string())
				.collect::<Vec<_>>();
			let children = chunk
				.iter()
				.flat_map(|(_, _, children)| children.iter().map(ToString::to_string))
				.collect::<Vec<_>>();
			let parent_indices = chunk
				.iter()
				.enumerate()
				.flat_map(|(index, (_, _, children))| {
					std::iter::repeat_n((index + 1).to_i64().unwrap(), children.len())
				})
				.collect::<Vec<_>>();
			let bytes = chunk
				.iter()
				.map(|(_, bytes, _)| {
					if self.store.is_some() {
						None
					} else {
						Some(bytes.as_ref())
					}
				})
				.collect::<Vec<_>>();
			let size = chunk
				.iter()
				.map(|(_, bytes, _)| bytes.len().to_i64().unwrap())
				.collect::<Vec<_>>();
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			client
				.execute(
					statement,
					&[
						&ids.as_slice(),
						&children.as_slice(),
						&parent_indices.as_slice(),
						&bytes.as_slice(),
						&size.as_slice(),
						&now,
					],
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_post_object_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Create the incoming stream.
		let body = request.reader();
		let stream = stream::try_unfold(body, |mut reader| async move {
			let Some(item) = tg::object::post::Item::try_deserialize(&mut reader).await? else {
				return Ok(None);
			};
			Ok(Some((item, reader)))
		})
		.boxed();

		// Create the outgoing stream.
		let stream = handle.post_objects(stream).await?.boxed();

		// Create the response body.
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
