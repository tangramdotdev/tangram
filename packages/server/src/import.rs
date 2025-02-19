use crate::Server;
use futures::{stream, Stream, StreamExt as _, TryStreamExt as _};
use indoc::indoc;
use itertools::Itertools;
use num::ToPrimitive as _;
use std::pin::{pin, Pin};
use tangram_client as tg;
use tangram_database::{self as db, Database as _};
use tangram_either::Either;
use tangram_futures::stream::Ext as _;
use tangram_http::{request::Ext, Body};
use time::format_description::well_known::Rfc3339;
use tokio::task::JoinSet;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::task::AbortOnDropHandle;

#[derive(Debug, Clone)]
struct Progress {
	processes: AtomicU64,
	objects: AtomicU64,
	bytes: AtomicU64,
}

impl Server {
	pub async fn import(
		&self,
		arg: tg::import::Arg,
		mut stream: Pin<Box<dyn Stream<Item = tg::Result<tg::export::Item>> + Send + 'static>>,
	) -> tg::Result<impl Stream<Item = tg::Result<tg::import::Event>> + Send + 'static> {
		// If the remote arg is set, then forward the request.
		if let Some(remote) = arg.remote {
			let client = self.get_remote_client(remote.clone()).await?;
			let arg = tg::import::Arg {
				remote: None,
				..arg
			};
			let stream = client.import(arg, stream).await?;
			return Ok(stream.left_stream());
		}

		let (event_sender, event_receiver) =
			tokio::sync::mpsc::channel::<tg::Result<tg::import::Event>>(256);
		let (complete_sender, complete_receiver) =
			tokio::sync::mpsc::channel::<tg::export::Item>(256);
		let (database_process_sender, database_process_receiver) =
			tokio::sync::mpsc::channel::<tg::export::Item>(256);
		let (database_object_sender, database_object_receiver) =
			tokio::sync::mpsc::channel::<tg::export::Item>(256);
		let (store_sender, store_receiver) = tokio::sync::mpsc::channel::<tg::export::Item>(256);

		// Create the complete task.
		let complete_task = tokio::spawn({
			let server = self.clone();
			let event_sender = event_sender.clone();
			async move {
				let result = server
					.import_complete_task(complete_receiver, &event_sender)
					.await;
				if let Err(error) = result {
					event_sender.send(Err(error)).await.ok();
				}
			}
		});
		let complete_task_abort_handle = complete_task.abort_handle();
		let complete_task_abort_handle =
			scopeguard::guard(complete_task_abort_handle, |complete_task_abort_handle| {
				complete_task_abort_handle.abort();
			});

		// Create the database processes task.
		let database_processes_task = tokio::spawn({
			let server = self.clone();
			let event_sender = event_sender.clone();
			async move {
				let result = server
					.import_database_processes_task(database_process_receiver, &event_sender)
					.await;
				if let Err(error) = result {
					event_sender.send(Err(error)).await.ok();
				}
			}
		});
		let database_processes_task_abort_handle = database_processes_task.abort_handle();
		let database_processes_task_abort_handle = scopeguard::guard(
			database_processes_task_abort_handle,
			|database_processes_task_abort_handle| {
				database_processes_task_abort_handle.abort();
			},
		);

		// Create the database objects task.
		let database_objects_task = tokio::spawn({
			let server = self.clone();
			let event_sender = event_sender.clone();
			async move {
				let result = server
					.import_database_objects_task(database_object_receiver)
					.await;
				if let Err(error) = result {
					event_sender.send(Err(error)).await.ok();
				}
			}
		});
		let database_objects_task_abort_handle = database_objects_task.abort_handle();
		let database_objects_task_abort_handle = scopeguard::guard(
			database_objects_task_abort_handle,
			|database_objects_task_abort_handle| {
				database_objects_task_abort_handle.abort();
			},
		);

		// Create the store task.
		let store_task = tokio::spawn({
			let server = self.clone();
			let event_sender = event_sender.clone();
			async move {
				let result = server.import_store_task(store_receiver).await;
				if let Err(error) = result {
					event_sender.send(Err(error)).await.ok();
				}
			}
		});
		let store_task_abort_handle = store_task.abort_handle();
		let store_task_abort_handle =
			scopeguard::guard(store_task_abort_handle, |store_task_abort_handle| {
				store_task_abort_handle.abort();
			});

		// Spawn a task that sends items from the stream to the other tasks.
		let task = tokio::spawn({
			let event_sender = event_sender.clone();
			async move {
				// Read the items from the stream and send them to the tasks.
				loop {
					let item = match stream.try_next().await {
						Ok(Some(item)) => item,
						Ok(None) => break,
						Err(error) => {
							event_sender.send(Err(error)).await.ok();
							return;
						},
					};
					let complete_sender_future = complete_sender.send(item.clone());
					let database_sender_future = match item {
						tg::export::Item::Process { .. } => {
							database_process_sender.send(item.clone())
						},
						tg::export::Item::Object { .. } => {
							database_object_sender.send(item.clone())
						},
					};
					let store_sender_future = store_sender.send(item.clone());
					let result = futures::try_join!(
						complete_sender_future,
						database_sender_future,
						store_sender_future,
					);
					if result.is_err() {
						event_sender
							.send(Err(tg::error!(?result, "failed to send the item")))
							.await
							.ok();
						return;
					}
				}

				// Close the channels
				drop(complete_sender);
				drop(database_object_sender);
				drop(database_process_sender);
				drop(store_sender);

				// Join the database and store tasks.
				if let Err(error) =
					futures::try_join!(database_processes_task, database_objects_task, store_task)
				{
					event_sender
						.send(Err(tg::error!(!error, "failed to join the task")))
						.await
						.ok();
				}
			}
		});

		// Create the stream.
		let stream = ReceiverStream::new(event_receiver);
		let abort_handle = AbortOnDropHandle::new(task);
		let stream = stream
			.attach(abort_handle)
			.attach(complete_task_abort_handle)
			.attach(database_processes_task_abort_handle)
			.attach(database_objects_task_abort_handle)
			.attach(store_task_abort_handle);

		Ok(stream.right_stream())
	}

	async fn import_complete_task(
		&self,
		complete_receiver: tokio::sync::mpsc::Receiver<tg::export::Item>,
		event_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::import::Event>>,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(complete_receiver);
		let mut stream = pin!(stream);
		let mut join_set = JoinSet::new();
		while let Some(item) = stream.next().await {
			join_set.spawn({
				let server = self.clone();
				let event_sender = event_sender.clone();
				async move {
					match item {
						tg::export::Item::Process { id, .. } => {
							let Some(output) =
								server.try_get_process(&id).await.map_err(|source| {
									tg::error!(!source, "failed to get the process")
								})?
							else {
								return Ok(());
							};
							let Some(metadata) = output.metadata else {
								return Ok(());
							};
							if !(metadata.complete
								|| metadata.commands_complete
								|| metadata.logs_complete
								|| metadata.outputs_complete)
							{
								return Ok(());
							}
							let output = tg::import::ProcessOutput {
								id,
								output: tg::process::put::Output {
									commands_complete: metadata.commands_complete,
									complete: metadata.complete,
									logs_complete: metadata.logs_complete,
									outputs_complete: metadata.outputs_complete,
								},
							};
							let event = tg::import::Event::Complete(Either::Left(output));
							event_sender.send(Ok(event)).await.ok();
							Ok::<_, tg::Error>(())
						},
						tg::export::Item::Object { id, .. } => {
							let Some(metadata) = server
								.try_get_object_metadata_local(&id)
								.await
								.map_err(|source| {
									tg::error!(!source, "failed to get the object metadata")
								})?
							else {
								return Ok(());
							};
							if !metadata.complete {
								return Ok(());
							}
							let output = tg::import::ObjectOutput {
								id,
								output: tg::object::put::Output { complete: true },
							};
							let event = tg::import::Event::Complete(Either::Right(output));
							event_sender.send(Ok(event)).await.ok();
							Ok::<_, tg::Error>(())
						},
					}
				}
			});
		}
		Ok(())
	}

	async fn import_database_processes_task(
		&self,
		database_process_receiver: tokio::sync::mpsc::Receiver<tg::export::Item>,
		event_sender: &tokio::sync::mpsc::Sender<tg::Result<tg::import::Event>>,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(database_process_receiver);
		let mut stream = pin!(stream);
		while let Some(item) = stream.next().await {
			// Make sure the item is a process.
			let tg::export::Item::Process { id, data } = item else {
				return Err(tg::error!(?item, "expected a process item"));
			};

			// Put the process.
			let put_arg = tg::process::put::Arg {
				cacheable: data.cacheable,
				checksum: data.checksum,
				children: data.children,
				command: data.command.clone(),
				created_at: data.created_at,
				cwd: data.cwd,
				dequeued_at: data.dequeued_at,
				enqueued_at: data.enqueued_at,
				env: data.env,
				error: data.error,
				exit: data.exit,
				finished_at: data.finished_at,
				host: data.host,
				id: id.clone(),
				log: data.log.clone(),
				network: data.network,
				output: data.output.clone(),
				retry: data.retry,
				started_at: data.started_at,
				status: data.status,
			};
			let put_output = self
				.put_process(&id, put_arg)
				.await
				.map_err(|source| tg::error!(!source, "failed to put the process"))?;

			if put_output.complete
				|| put_output.commands_complete
				|| put_output.logs_complete
				|| put_output.outputs_complete
			{
				let event = tg::import::Event::Complete(Either::Left(tg::import::ProcessOutput {
					id,
					output: put_output,
				}));
				event_sender.send(Ok(event)).await.ok();
			}
		}
		Ok(())
	}

	async fn import_database_objects_task(
		&self,
		database_object_receiver: tokio::sync::mpsc::Receiver<tg::export::Item>,
	) -> tg::Result<()> {
		let stream = ReceiverStream::new(database_object_receiver);
		match &self.database {
			Either::Left(database) => {
				self.import_objects_sqlite(stream, database)
					.await
					.inspect_err(|error| eprintln!("failed to insert: {error}"))?;
			},
			Either::Right(database) => {
				self.import_objects_postgres(stream, database).await?;
			},
		}
		Ok(())
	}

	async fn import_objects_sqlite(
		&self,
		stream: impl Stream<Item = tg::export::Item> + Send + 'static,
		database: &db::sqlite::Database,
	) -> tg::Result<()> {
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let stream = stream.chunks(1024);
		let mut stream = pin!(stream);
		while let Some(chunk) = stream.next().await {
			let chunk: Vec<_> = chunk
				.into_iter()
				.map(|item| {
					// Make sure the item is an object.
					let tg::export::Item::Object { id, bytes } = item else {
						return Err(tg::error!(?item, "expected an object item"));
					};

					// Get the children.
					let children = tg::object::Data::deserialize(id.kind(), &bytes)?.children();

					Ok::<_, tg::Error>((id, bytes, children))
				})
				.try_collect()?;

			connection
				.with(move |connection| {
					// Begin a transaction for the batch.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Prepare a statement for the object children
					let children_statement = indoc!(
						"
							insert into object_children (object, child)
							values (?1, ?2)
							on conflict (object, child) do nothing;
						"
					);
					let mut children_statement = transaction
						.prepare_cached(children_statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

					// Prepare a statement for the objects.
					let objects_statement = indoc!(
						"
							insert into objects (id, bytes, size, touched_at)
							values (?1, ?2, ?3, ?4)
							on conflict (id) do update set touched_at = ?4;
						"
					);
					let mut objects_statement = transaction
						.prepare_cached(objects_statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

					let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();

					// Execute inserts for each member of the batch.
					for (id, bytes, children) in chunk {
						// Insert the childre.
						for child in children {
							let child = child.to_string();
							let params = rusqlite::params![&id.to_string(), &child];
							children_statement.execute(params).map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
						}

						// Insert the object.
						let size = bytes.len().to_u64().unwrap();
						let bytes = bytes.as_ref();
						let params = rusqlite::params![&id.to_string(), bytes, size, now];
						objects_statement.execute(params).map_err(|source| {
							tg::error!(!source, "failed to execute the statement")
						})?;
					}
					drop(children_statement);
					drop(objects_statement);

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

	async fn import_objects_postgres(
		&self,
		stream: impl Stream<Item = tg::export::Item> + Send + 'static,
		database: &db::postgres::Database,
	) -> tg::Result<()> {
		let mut connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let stream = stream.chunks(128);
		let mut stream = pin!(stream);
		while let Some(chunk) = stream.next().await {
			let chunk: Vec<_> = chunk
				.into_iter()
				.map(|item| {
					// Ensure the item is an object.
					let tg::export::Item::Object { id, bytes } = item else {
						return Err(tg::error!(?item, "expected an object"));
					};

					// Get the children.
					let children = tg::object::Data::deserialize(id.kind(), &bytes)?.children();
					Ok::<_, tg::Error>((id, bytes, children))
				})
				.try_collect()?;

			let transaction = connection
				.client_mut()
				.transaction()
				.await
				.map_err(|source| tg::error!(!source, "failed to create a transaction"))?;

			let statement = indoc!(
				"
					with inserted_object_children as (
						insert into object_children (object, child)
						select ($1::text[])[object_index], child
						from unnest($3::int8[], $2::text[]) as c (object_index, child)
						on conflict (object, child) do nothing
					),
					inserted_objects as (
						insert into objects (id, bytes, size, touched_at)
						select id, bytes, size, $6
						from unnest($1::text[], $4::bytea[], $5::int8[]) as t (id, bytes, size)
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
			transaction
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

			// Set reference counts and incomplete children.
			let statement = indoc!(
				"
				 update objects
					set incomplete_children = (
						select count(*)
						from object_children
						left join objects child_objects on child_objects.id = object_children.child
						where object_children.object = t.id and (child_objects.complete is null or child_objects.complete = 0)
					),
					reference_count = (
						(select count(*) from object_children where child = t.id) +
						(select count(*) from process_objects where object = t.id) +
						(select count(*) from tags where item = t.id)
					)
					from unnest($1::text[]) as t (id)
					where objects.id = t.id;
				"
			);

			transaction
				.execute(statement, &[&ids.as_slice()])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			transaction
				.commit()
				.await
				.map_err(|source| tg::error!(!source, "failed to commit transaction"))?;
		}

		Ok(())
	}

	async fn import_store_task(
		&self,
		mut store_receiver: tokio::sync::mpsc::Receiver<tg::export::Item>,
	) -> tg::Result<()> {
		let mut join_set = JoinSet::new();
		loop {
			let Some(item) = store_receiver.recv().await else {
				break;
			};
			if let tg::export::Item::Object { id, bytes, .. } = item {
				join_set.spawn({
					let server = self.clone();
					async move {
						if let Some(store) = &server.store {
							store.put(id, bytes).await?;
						}
						Ok::<_, tg::Error>(())
					}
				});
				while let Some(result) = join_set.try_join_next() {
					result.map_err(|source| tg::error!(!source, "a store task panicked"))??;
				}
			}
		}
		while let Some(result) = join_set.join_next().await {
			result.map_err(|source| tg::error!(!source, "a store task panicked"))??;
		}
		Ok(())
	}

	pub(crate) async fn handle_import_request<H>(
		handle: &H,
		request: http::Request<Body>,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		// Parse the arg.
		let arg = request
			.query_params::<tg::import::QueryArg>()
			.transpose()?
			.ok_or_else(|| tg::error!("query parameters required"))?
			.into();

		// Get the accept header.
		let accept = request
			.parse_header::<mime::Mime, _>(http::header::ACCEPT)
			.transpose()?;

		// Create the incoming stream.
		let body = request.reader();
		let stream = stream::try_unfold(body, |mut reader| async move {
			let Some(item) = tg::export::Item::from_reader(&mut reader).await? else {
				return Ok(None);
			};
			Ok(Some((item, reader)))
		})
		.boxed();

		// Create the outgoing stream.
		let stream = handle.import(arg, stream).await?;

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
