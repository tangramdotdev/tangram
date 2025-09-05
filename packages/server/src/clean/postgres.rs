use super::{Count, InnerOutput, Server};
use indoc::indoc;
use num::ToPrimitive as _;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	#[cfg(feature = "postgres")]
	pub(super) async fn clean_count_items_postgres(
		&self,
		database: &db::postgres::Database,
	) -> tg::Result<Count> {
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get an index connection"))?;
		let statement = indoc!(
			"
				select
					(select count(*) from cache_entries) as cache_entries,
					(select count(*) from objects) as objects,
					(select count(*) from processes) as processes;
				;
			"
		);
		let params = db::params![];
		let count = connection
			.query_one_into::<db::row::Serde<Count>>(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.0;
		Ok(count)
	}

	#[cfg(feature = "postgres")]
	pub(super) async fn cleaner_task_inner_postgres(
		&self,
		database: &db::postgres::Database,
		now: i64,
		ttl: std::time::Duration,
		n: usize,
	) -> tg::Result<InnerOutput> {
		let max_touched_at = now - ttl.as_secs().to_i64().unwrap();

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
