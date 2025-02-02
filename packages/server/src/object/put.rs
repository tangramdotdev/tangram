use crate::Server;
use futures::FutureExt as _;
use indoc::indoc;
use itertools::Itertools as _;
use num::ToPrimitive;
use tangram_client as tg;
use tangram_database::prelude::*;
use tangram_either::Either;
use tangram_http::{incoming::request::Ext as _, outgoing::response::Ext as _, Incoming, Outgoing};
use time::format_description::well_known::Rfc3339;

impl Server {
	pub async fn put_object(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		self.put_object_inner(id, arg).boxed().await
	}

	async fn put_object_inner(
		&self,
		id: &tg::object::Id,
		arg: tg::object::put::Arg,
	) -> tg::Result<tg::object::put::Output> {
		// Get the children.
		let data = tg::object::Data::deserialize(id.kind(), &arg.bytes)
			.map_err(|source| tg::error!(!source, "failed to deserialize the data"))?;
		let children = data.children();

		// Create the store future.
		let store = async {
			if let Some(store) = &self.store {
				store.put(id.clone(), arg.bytes.clone()).await?;
			}
			Ok::<_, tg::Error>(())
		};

		// Create the database future.
		let database = async {
			let complete = match &self.database {
				Either::Left(database) => {
					// Get a database connection.
					let connection = database.write_connection().await.map_err(|source| {
						tg::error!(!source, "failed to get a database connection")
					})?;

					let id = id.to_string();
					let bytes = if self.store.is_none() {
						Some(arg.bytes.clone())
					} else {
						None
					};
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
									insert into objects (id, bytes, size, touched_at)
									values (?1, ?2, ?3, ?4)
									on conflict (id) do update set touched_at = ?4
									returning complete;
								"
							);
							let bytes = bytes.as_ref().map(AsRef::as_ref);
							let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
							let params = rusqlite::params![id, bytes, size, now];
							let mut statement =
								transaction.prepare_cached(statement).map_err(|source| {
									tg::error!(!source, "failed to prepare the statement")
								})?;
							let complete = statement
								.query_row(params, |row| Ok(row.get::<_, i64>(0)? != 0))
								.map_err(|source| {
									tg::error!(!source, "failed to execute the statement")
								})?;
							drop(statement);

							// Commit the transaction.
							transaction.commit().map_err(|source| {
								tg::error!(!source, "failed to commit the transaction")
							})?;

							Ok::<_, tg::Error>(complete)
						})
						.await?
				},

				Either::Right(database) => {
					let mut connection = database.write_connection().await.map_err(|source| {
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
								insert into objects (id, bytes, size, touched_at)
								values ($1, $3, $4, $5)
								on conflict (id) do update set touched_at = $5
								returning id, complete
							)
							select complete
							from inserted_objects;
						"
					);
					let id = id.to_string();
					let children = children.iter().map(ToString::to_string).collect_vec();
					let bytes = if self.store.is_none() {
						Some(arg.bytes.as_ref())
					} else {
						None
					};
					let size = arg.bytes.len().to_i64().unwrap();
					let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
					connection
						.client()
						.query_one(statement, &[&id, &children, &bytes, &size, &now])
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
						.get::<_, i64>(0) != 0
				},
			};
			Ok::<_, tg::Error>(complete)
		};

		// Await the futures.
		let ((), complete) = futures::try_join!(store, database)?;

		// Create the output.
		let output = tg::object::put::Output { complete };

		Ok(output)
	}
}

impl Server {
	pub(crate) async fn handle_put_object_request<H>(
		handle: &H,
		request: http::Request<Incoming>,
		id: &str,
	) -> tg::Result<http::Response<Outgoing>>
	where
		H: tg::Handle,
	{
		let id = id.parse()?;
		let bytes = request.bytes().await?;
		let arg = tg::object::put::Arg { bytes };
		let output = handle.put_object(&id, arg).await?;
		let response = http::Response::builder().json(output).unwrap();
		Ok(response)
	}
}
