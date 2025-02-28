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
									on conflict (id) do update set touched_at = ?4;
								"
							);
							let bytes = bytes.as_ref().map(AsRef::as_ref);
							let now = time::OffsetDateTime::now_utc().format(&Rfc3339).unwrap();
							let params = rusqlite::params![id, bytes, size, now];
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
								insert into objects (id, bytes, size, touched_at)
								values ($1, $3, $4, $5)
								on conflict (id) do update set touched_at = $5
								returning id, complete
							)
							select 1;
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
						.execute(statement, &[&id, &children, &bytes, &size, &now])
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				},
			}
			Ok::<_, tg::Error>(())
		};

		// Create the store future.
		let store = async {
			if let Some(store) = &self.store {
				store.put(id, arg.bytes.clone()).await?;
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
