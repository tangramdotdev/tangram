use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn clean_remote_tags_sqlite(
		&self,
		database: &db::sqlite::Database,
		max_cached_at: i64,
		batch_size: usize,
	) -> tg::Result<u64> {
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let batch_size = batch_size.to_i64().unwrap();
		connection
			.with(move |connection, cache| {
				Self::clean_remote_tags_sqlite_sync(connection, cache, max_cached_at, batch_size)
			})
			.await
	}

	fn clean_remote_tags_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
		max_cached_at: i64,
		batch_size: i64,
	) -> tg::Result<u64> {
		// Find expired remote leaf tags.
		let statement = indoc!(
			"
				select id
				from tags
				where remote is not null
					and child_count = 0
					and (cached_at is null or cached_at <= ?1)
				limit ?2;
			"
		);
		let mut statement = cache
			.get(connection, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![max_cached_at, batch_size];
		let mut rows = statement
			.query(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let mut ids = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to get the next row"))?
		{
			let id: i64 = row
				.get(0)
				.map_err(|source| tg::error!(!source, "failed to get the id"))?;
			ids.push(id);
		}
		drop(rows);
		drop(statement);

		let count = ids.len().to_u64().unwrap();

		for id in ids {
			// Find the parent.
			let statement = indoc!(
				"
					select tag from tag_children where child = ?1;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![id];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let parent_id: Option<i64> = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to get the next row"))?
				.map(|row| {
					row.get(0)
						.map_err(|source| tg::error!(!source, "failed to get the parent id"))
				})
				.transpose()?;
			drop(rows);
			drop(statement);

			// Delete the tag_children entry.
			let statement = indoc!(
				"
					delete from tag_children where child = ?1;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![id];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Update the parent: decrement child_count and clear cached_at.
			if let Some(parent_id) = parent_id {
				let statement = indoc!(
					"
						update tags set cached_at = null, child_count = child_count - 1
						where id = ?1;
					"
				);
				let mut statement = cache
					.get(connection, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![parent_id];
				statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Delete the tag.
			let statement = indoc!(
				"
					delete from tags where id = ?1;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![id];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(count)
	}
}
