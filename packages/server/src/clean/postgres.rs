use {
	super::{InnerOutput, Server},
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
		#[allow(clippy::struct_field_names)]
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::postgres::value::TryFrom<i64>")]
			deleted_bytes: u64,
			#[tangram_database(as = "Vec<db::postgres::value::TryFrom<Vec<u8>>>")]
			deleted_cache_entries: Vec<tg::artifact::Id>,
			#[tangram_database(as = "Vec<db::postgres::value::TryFrom<Vec<u8>>>")]
			deleted_objects: Vec<tg::object::Id>,
			#[tangram_database(as = "Vec<db::postgres::value::TryFrom<Vec<u8>>>")]
			deleted_processes: Vec<tg::process::Id>,
		}
		let statement = "call clean($1, $2, null, null, null, null);";
		let row = transaction
			.inner()
			.query_one(statement, &[&max_touched_at, &n.to_i64().unwrap()])
			.await
			.map_err(|source| {
				tg::error!(!source, "failed to call clean_cache_entries procedure")
			})?;
		let row = <Row as db::postgres::row::Deserialize>::deserialize(&row)
			.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;

		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		drop(connection);

		let output = InnerOutput {
			bytes: row.deleted_bytes,
			cache_entries: row.deleted_cache_entries,
			objects: row.deleted_objects,
			processes: row.deleted_processes,
		};

		Ok(output)
	}
}
