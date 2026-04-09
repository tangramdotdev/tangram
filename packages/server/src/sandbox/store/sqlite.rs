use {
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

pub async fn migrate(database: &db::sqlite::Database) -> tg::Result<()> {
	let schema_version = 1;

	let connection = database
		.connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
	let version = connection
		.with(|connection, _cache| {
			connection
				.pragma_query_value(None, "user_version", |row| {
					Ok(row.get_unwrap::<_, i64>(0).to_usize().unwrap())
				})
				.map_err(|source| tg::error!(!source, "failed to get the version"))
		})
		.await?;
	drop(connection);

	if version > schema_version {
		return Err(tg::error!(
			r"The sandbox store has run migrations from a newer version of Tangram. Please run `tg self update` to update to the latest version of Tangram."
		));
	}

	if version == 0 {
		migration_0000(database)
			.await
			.map_err(|source| tg::error!(!source, "failed to create the sandbox store schema"))?;
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
		connection
			.with(move |connection, _cache| {
				connection
					.pragma_update(None, "user_version", schema_version.to_i64().unwrap())
					.map_err(|source| tg::error!(!source, "failed to set the version"))
			})
			.await?;
	}

	Ok(())
}

async fn migration_0000(database: &db::sqlite::Database) -> tg::Result<()> {
	let sql = include_str!("./sqlite.sql");
	let connection = database
		.write_connection()
		.await
		.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;
	connection
		.with(move |connection, _cache| {
			connection
				.execute_batch(sql)
				.map_err(|source| tg::error!(!source, "failed to execute the statements"))?;
			Ok::<_, tg::Error>(())
		})
		.await?;
	Ok(())
}
