use {
	super::{InnerOutput, Server},
	indoc::indoc,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	#[cfg(feature = "postgres")]
	pub(super) async fn cleaner_task_inner_postgres(
		&self,
		database: &db::postgres::Database,
		max_touched_at: i64,
		n: usize,
	) -> tg::Result<InnerOutput> {
		let mut connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Clean.
		let statement = "call clean($1, $2, null, null, null);";
		let row = transaction
			.inner()
			.query_one(statement, &[&max_touched_at, &n.to_i64().unwrap()])
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to call clean_cache_entries procedure")
			})?;
		let cache_entries = row
			.get::<_, Vec<Vec<u8>>>(0)
			.into_iter()
			.map(|bytes| tg::artifact::Id::from_slice(&bytes))
			.collect::<Result<Vec<_>, _>>()
			.map_err(|source| tg::error!(!source, "failed to parse artifact ids"))?;
		let objects = row
			.get::<_, Vec<Vec<u8>>>(1)
			.into_iter()
			.map(|bytes| tg::object::Id::from_slice(&bytes))
			.collect::<Result<Vec<_>, _>>()
			.map_err(|source| tg::error!(!source, "failed to parse objects ids"))?;
		let processes = row
			.get::<_, Vec<Vec<u8>>>(2)
			.into_iter()
			.map(|bytes| tg::process::Id::from_slice(&bytes))
			.collect::<Result<Vec<_>, _>>()
			.map_err(|source| tg::error!(!source, "failed to parse process ids"))?;

		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		drop(connection);

		let output = InnerOutput {
			cache_entries,
			objects,
			processes,
		};

		Ok(output)
	}
}
