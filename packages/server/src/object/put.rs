use crate::Server;
use futures::FutureExt as _;
use indoc::indoc;
use itertools::Itertools as _;
use num::ToPrimitive;
use tangram_client as tg;
use tangram_database::prelude::*;
use tangram_either::Either;
use tangram_http::{Body, request::Ext as _, response::builder::Ext as _};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<()> {
		self.put_object_inner(id, arg).boxed().await
	}

	async fn put_object_inner(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<()> {
		// Get the children.
		let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
		let children = data.children();

		// Create the database future.
		let database_future = async {
			match &self.database {
				Either::Left(database) => {
					// Get a database connection.
					let connection = database.write_connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

					let id = id.to_string();
					let size = arg.bytes.len().to_u64().unwrap();
					connection
						.with(move |connection| {
							// Begin a transaction.
							let transaction = connection.transaction().map_err(|source| {
								tg::error!(!source, "failed to begin a transaction")
							})?;

							// Insert the object children.
							let statement = indoc!(
								"
									insert into object_children (object, child)
									values (?1, ?2)
									on conflict (object, child) do nothing;
								"
							);
							let mut statement =
								transaction.prepare_cached(statement).map_err(|source| {
									tg::error!(!source, "failed to prepare the statement")
								})?;
							for child in children {
								let child = child.to_string();
								let params = rusqlite::params![&id, &child];
								statement.execute(params).map_err(|source| {
									tg::error!(!source, "failed to execute the statement")
								})?;
							}
							drop(statement);

							// Insert the object.
							let statement = indoc!(
								"
									insert into objects (id, size, touched_at)
									values (?1, ?2, ?3)
									on conflict (id) do update set touched_at = ?3;
								"
							);
							let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
							let params = rusqlite::params![id, size, now];
							let mut statement =
								transaction.prepare_cached(statement).map_err(|source| {
									tg::error!(!source, "failed to prepare the statement")
								})?;
							statement.execute(params).map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
							drop(statement);

							// Commit the transaction.
							transaction.commit().map_err(|source| {
								tg::error!(!source, "failed to commit the transaction")
							})?;

							Ok::<_, tg::Error>(())
						})
						.await?;
				},

				Either::Right(database) => {
					let connection = database.write_connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;
					let statement = indoc!(
						"
							with inserted_object_children as (
								insert into object_children (object, child)
								select $1, unnest($2::text[])
								on conflict (object, child) do nothing
								returning child
							),
							inserted_objects as (
								insert into objects (id, size, touched_at)
								values ($1, $3, $4)
								on conflict (id) do update set touched_at = $4
								returning id, complete
							)
							select 1;
						"
					);
					let id = id.to_string();
					let children = children.iter().map(ToString::to_string).collect_vec();
					let size = arg.bytes.len().to_i64().unwrap();
					let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
					connection
						.client()
						.execute(statement, &[&id, &children, &size, &now])
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				},
			}
			Ok::<_, tg::Error>(())
		};

		// Create the store future.
		let store = async {
			if matches!(&*self.store, crate::Store::Lmdb(_)) {
				let (sender, receiver) =
					tokio::sync::oneshot::channel::<crate::store::lmdb::Message>();

				let put_task = tokio::task::spawn_blocking({
					let store = self.store.clone();
					move || {
						let crate::Store::Lmdb(lmdb) = &*store else {
							panic!("expected LMDB");
						};
						let mut transaction = lmdb.env.write_txn().map_err(|source| {
							tg::error!(!source, "failed to begin a write transaction")
						})?;

						let message = receiver
							.blocking_recv()
							.map_err(|source| tg::error!(!source, "recv error"))?;

						lmdb.db
							.put(
								&mut transaction,
								message.id.to_string().as_bytes(),
								&message.bytes,
							)
							.map_err(|source| tg::error!(!source, "failed to put object"))?;

						transaction.commit().map_err(|source| {
							tg::error!(!source, "failed to commit the transaction")
						})?;

						Ok::<_, tg::Error>(())
					}
				});
				sender
					.send(crate::store::lmdb::Message {
						id: id.clone(),
						bytes: arg.bytes.clone(),
					})
					.map_err(|_| tg::error!("failed to send"))?;

				put_task
					.await
					.map_err(|source| tg::error!(!source, "blocking task failed"))??;
			} else {
				self.store.put(id.clone(), arg.bytes.clone()).await?;
			}

			Ok::<_, tg::Error>(())
		};

		// Await the futures.
		futures::try_join!(database_future, store)?;

		Ok(())
	}
}

impl Server {
	pub(crate) async fn handle_put_object_request<H>(
		handle: &H,
		request: http::Request<Body>,
		id: &str,
	) -> tg::Result<http::Response<Body>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let bytes = request.bytes().await?;
		let arg = tg::object::put::Arg { bytes };
		handle.put_object(&id, arg).await?;
		let response = http::Response::builder().empty().unwrap();
		Ok(response)
	}
}
