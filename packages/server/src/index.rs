use crate::Server;
use futures::{Stream, StreamExt as _};
use indoc::{formatdoc, indoc};
use std::{pin::pin, time::Duration};
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};
use tangram_either::Either;
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn index(&self) -> tg::Result<()> {
		let config = self.config.indexer.clone().unwrap_or_default();
		loop {
			let n = self.indexer_task_inner(&config).await?;
			if n == 0 {
				break;
			}
		}
		Ok(())
	}

	pub(crate) async fn indexer_task(&self, config: &crate::config::Indexer) -> tg::Result<()> {
		loop {
			let result = self.indexer_task_inner(config).await;
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
	) -> tg::Result<u64> {
		let options = db::ConnectionOptions {
			kind: db::ConnectionKind::Write,
			priority: db::Priority::Low,
		};

		// Assert any pending objects if applicable.
		self.indexer_insert_pending_objects(config).await?;

		// Run the indexing.
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

	async fn indexer_insert_pending_objects(
		&self,
		config: &crate::config::Indexer,
	) -> tg::Result<()> {
		// If we are using nats, check if there are objects to insert.
		if let Either::Right(messenger) = &self.messenger {
			let jetstream = async_nats::jetstream::new(messenger.client.clone());
			let stream = jetstream
				.get_or_create_stream(async_nats::jetstream::stream::Config {
					name: "objects".to_string(),
					..Default::default()
				})
				.await
				.map_err(|source| tg::error!(!source, "could not get object stream"))?;

			let consumer = stream
				.get_or_create_consumer(
					"object_consumer",
					async_nats::jetstream::consumer::pull::Config {
						durable_name: Some("object_consumer".to_string()),
						..Default::default()
					},
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to create jetstream consumer"))?;

			// Create a batch of messages.
			let messages = consumer
				.batch()
				.max_messages(config.batch_size)
				.messages()
				.await
				.map_err(|source| tg::error!(!source, "failed to get consumer messages"))?;

			// Collect the ids, sizes, and children for each message in the batch.
			let objects = messages.then(async |message| {
				let message =
					message.map_err(|source| tg::error!(!source, "failed to receive message"))?;
				message.ack().await?;
				let insert_message: crate::import::InsertMessage =
					serde_json::from_slice(&message.payload)
						.map_err(|source| tg::error!(!source, "failed to deserialize message"))?;
				Ok::<_, tg::Error>(insert_message)
			});

			// Insert the objects.
			match &self.database {
				Either::Left(database) => {
					self.indexer_insert_objects_sqlite(stream, database)
						.await
						.inspect_err(|error| eprintln!("failed to insert: {error}"))?;
				},
				Either::Right(database) => {
					self.indexer_insert_objects_postgres(stream, database)
						.await?;
				},
			}
		}
		Ok(())
	}

	async fn indexer_insert_objects_sqlite(
		&self,
		stream: impl Stream<Item = crate::import::InsertMessage> + Send + 'static,
		database: &db::sqlite::Database,
	) -> tg::Result<()> {
		todo!();
	}

	async fn indexer_insert_objects_postgres(
		&self,
		stream: impl Stream<Item = crate::import::InsertMessage> + Send + 'static,
		database: &db::postgres::Database,
	) -> tg::Result<()> {
		let mut connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let stream = stream.chunks(128);
		let mut stream = pin!(stream);
		while let Some(chunk) = stream.next().await {
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
					if self.store.is_none() {
						Some(bytes.as_ref())
					} else {
						None
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
}
