use crate::Server;
use futures::StreamExt as _;
use indoc::{formatdoc, indoc};
use num::ToPrimitive as _;
use std::{collections::BTreeSet, pin::pin, sync::Arc, time::Duration};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use tangram_messenger::Messenger;
use time::format_description::well_known::Rfc3339;

#[derive(Clone)]
pub(crate) struct Message {
	pub(crate) acker: Option<Arc<async_nats::jetstream::message::Acker>>,
	pub(crate) id: tg::object::Id,
	pub(crate) size: u64,
	pub(crate) children: BTreeSet<tg::object::Id>,
}

impl Server {
	pub async fn index(&self) -> tg::Result<()> {
		let config = self.config.indexer.clone().unwrap_or_default();
		loop {
			let n = self.indexer_task_inner(&config, vec![]).await?;
			if n == 0 {
				break;
			}
		}
		Ok(())
	}

	pub(crate) async fn indexer_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		let (object_insert_sender, mut object_insert_receiver) =
			tokio::sync::mpsc::channel::<Message>(config.batch_size);

		// Create a task to listen for messages.
		let object_insert_task = tokio::spawn({
			let server = self.clone();
			let object_insert_sender = object_insert_sender.clone();
			async move {
				let result = server
					.indexer_message_consumer_task(&object_insert_sender)
					.await;
				if let Err(error) = result {
					tracing::error!(?error, "the message listener task failed");
				}
			}
		});
		let object_insert_task_abort_handle = object_insert_task.abort_handle();
		let _guard = scopeguard::guard(object_insert_task_abort_handle, |handle| {
			handle.abort();
		});

		loop {
			// Get a batch of messages.
			let mut batch = Vec::with_capacity(config.batch_size);
			while batch.len() < config.batch_size {
				match object_insert_receiver.try_recv() {
					Ok(message) => batch.push(message),
					Err(tokio::sync::mpsc::error::TryRecvError::Empty) => break,
					Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
						panic!("the sender was dropped");
					},
				}
			}
			let result = self.indexer_task_inner(config, batch).await;
			match result {
				Ok(0) => {
					tokio::time::sleep(Duration::from_millis(100)).await;
				},
				Ok(_) => (),
				Err(error) => {
					tracing::error!(?error, "failed to index the objects");
					tokio::time::sleep(Duration::from_secs(1)).await;
				},
			}
		}
	}

	pub(crate) async fn indexer_task_inner(
		&self,
		config: &crate::config::Indexer,
		messages: Vec<Message>,
	) -> tg::Result<u64> {
		// Insert objects.
		let inserted = self.indexer_task_insert_objects(messages).await?;

		// Update complete objects.
		let updated = self.indexer_task_update_complete_objects(config).await?;

		Ok(inserted + updated)
	}

	async fn indexer_message_consumer_task(
		&self,
		object_insert_sender: &tokio::sync::mpsc::Sender<Message>,
	) -> tg::Result<()> {
		match &self.messenger {
			Either::Left(memory) => {
				let stream = memory
					.subscribe("index".to_string(), None)
					.await
					.map_err(|source| tg::error!(!source, "failed to subscribe to objects stream"))
					.unwrap();

				let mut stream = pin!(stream);
				while let Some(message) = stream.next().await {
					let payload = serde_json::from_slice(&message.payload).map_err(|source| {
						tg::error!(!source, "failed to deserialize message payload")
					});
					let payload: crate::import::Message = match payload {
						Ok(payload) => payload,
						Err(error) => {
							tracing::error!("{error}");
							continue;
						},
					};
					let message = Message {
						acker: None,
						id: payload.id,
						size: payload.size,
						children: payload.children,
					};
					object_insert_sender
						.send(message)
						.await
						.expect("failed to send");
				}
			},
			Either::Right(nats) => {
				let stream = nats
					.jetstream
					.get_or_create_stream(async_nats::jetstream::stream::Config {
						name: "index".to_string(),
						max_messages: i64::MAX,
						..Default::default()
					})
					.await
					.map_err(|source| tg::error!(!source, "could not get object stream"))?;

				let consumer = stream
					.get_or_create_consumer(
						"index_consumer",
						async_nats::jetstream::consumer::pull::Config {
							durable_name: Some("index_consumer".to_string()),
							..Default::default()
						},
					)
					.await
					.map_err(|source| tg::error!(!source, "failed to create jetstream consumer"))?;

				// Create messages stream.
				let mut messages = consumer
					.messages()
					.await
					.map_err(|source| tg::error!(!source, "failed to get consumer messages"))?;

				while let Some(Ok(message)) = messages.next().await {
					let (message, acker) = message.split();
					let payload = serde_json::from_slice(&message.payload).map_err(|source| {
						tg::error!(!source, "failed to deserialize message payload")
					});
					let payload: crate::import::Message = match payload {
						Ok(payload) => payload,
						Err(error) => {
							tracing::error!("{error}");
							continue;
						},
					};
					let message = Message {
						acker: Some(Arc::new(acker)),
						id: payload.id,
						size: payload.size,
						children: payload.children,
					};
					object_insert_sender
						.send(message)
						.await
						.expect("failed to send");
				}

				tracing::warn!("the listener hung up");
			},
		}
		Ok(())
	}

	async fn indexer_task_insert_objects(&self, messages: Vec<Message>) -> tg::Result<u64> {
		if messages.is_empty() {
			return Ok(0);
		}
		let n = messages.len().to_u64().unwrap();
		match &self.database {
			Either::Left(database) => {
				self.indexer_insert_objects_sqlite(messages, database)
					.await?;
			},
			Either::Right(database) => {
				self.indexer_insert_objects_postgres(messages, database)
					.await?;
			},
		}
		Ok(n)
	}

	async fn indexer_insert_objects_sqlite(
		&self,
		messages: Vec<Message>,
		database: &db::sqlite::Database,
	) -> tg::Result<()> {
		// Split the acker from the payload.
		let chunk = messages
			.iter()
			.cloned()
			.map(|message| {
				let payload = crate::import::Message {
					id: message.id,
					size: message.size,
					children: message.children,
				};
				(message.acker, payload)
			})
			.collect::<Vec<_>>();

		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let payloads = chunk
			.iter()
			.map(|(_, payload)| payload.clone())
			.collect::<Vec<_>>();
		connection
			.with({
				move |connection| {
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
								insert into objects (id, size, touched_at)
								values (?1, ?2, ?3)
								on conflict (id) do update set touched_at = ?3;
							"
					);
					let mut objects_statement = transaction
						.prepare_cached(objects_statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;

					let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();

					// Execute inserts for each member of the batch.
					for payload in payloads {
						let id = payload.id;
						let size = payload.size;
						let children = payload.children;
						// Insert the children.
						for child in children {
							let child = child.to_string();
							let params = rusqlite::params![&id.to_string(), &child];
							children_statement.execute(params).map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
						}

						// Insert the object.
						let params = rusqlite::params![&id.to_string(), size, now];
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
				}
			})
			.await?;

		// Acknowledge the messages.
		let ackers = chunk
			.iter()
			.filter_map(|(acker, _)| acker.as_ref())
			.collect::<Vec<_>>();
		let result =
			futures::future::try_join_all(ackers.into_iter().map(async |acker| acker.ack().await))
				.await;
		if let Err(error) = result {
			tracing::error!("{error}");
		}

		Ok(())
	}

	async fn indexer_insert_objects_postgres(
		&self,
		messages: Vec<Message>,
		database: &db::postgres::Database,
	) -> tg::Result<()> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let mut connection = database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		for chunk in messages.chunks(128) {
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
						insert into objects (id, size, touched_at)
						select id, size, $5
						from unnest($1::text[], $4::int8[]) as t (id, size)
						on conflict (id) do update set touched_at = $5
					)
					select 1;
				"
			);
			let ids = chunk
				.iter()
				.map(|message| message.id.to_string())
				.collect::<Vec<_>>();
			let children = chunk
				.iter()
				.flat_map(|message| message.children.iter().map(ToString::to_string))
				.collect::<Vec<_>>();
			let parent_indices = chunk
				.iter()
				.enumerate()
				.flat_map(|(index, message)| {
					std::iter::repeat_n((index + 1).to_i64().unwrap(), message.children.len())
				})
				.collect::<Vec<_>>();
			let size = chunk
				.iter()
				.map(|message| message.size.to_i64().unwrap())
				.collect::<Vec<_>>();
			let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
			transaction
				.execute(
					statement,
					&[
						&ids.as_slice(),
						&children.as_slice(),
						&parent_indices.as_slice(),
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

			// Acknowledge all messages.
			let ackers = chunk
				.iter()
				.filter_map(|message| message.acker.as_ref())
				.collect::<Vec<_>>();
			let result = futures::future::try_join_all(
				ackers.into_iter().map(async |acker| acker.ack().await),
			)
			.await;
			if let Err(error) = result {
				tracing::error!("{error}");
			}
		}

		Ok(())
	}

	async fn indexer_task_update_complete_objects(
		&self,
		config: &crate::config::Indexer,
	) -> tg::Result<u64> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};
		let connection = self
			.database
			.connection_with_options(options)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		let p = connection.p();
		let locked = match connection {
			Either::Left(_) => "",
			Either::Right(_) => "for update skip locked",
		};
		let statement = formatdoc!(
			"
				with update_objects as (
					select id, size
					from objects
					where objects.complete = 0 and objects.incomplete_children = 0
					{locked}
				),
				updates as (
					select
						update_objects.id as id,
						coalesce(min(child_objects.complete), 1) as complete,
						1 + coalesce(sum(child_objects.count), 0) as count,
						1 + coalesce(max(child_objects.depth), 0) as depth,
						update_objects.size + coalesce(sum(child_objects.weight), 0) as weight
					from update_objects
					left join object_children on object_children.object = update_objects.id
					left join objects child_objects on child_objects.id = object_children.child
					group by update_objects.id, update_objects.size
					limit {p}1
				)
				update objects
				set
					complete = updates.complete,
					count = updates.count,
					depth = updates.depth,
					weight = updates.weight
				from updates
				where objects.id = updates.id;
			"
		);
		let batch_size = config.batch_size;
		let params = db::params![batch_size];
		let n = connection
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		Ok(n)
	}
}
