use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn clean_remote_tags_postgres(
		&self,
		database: &db::postgres::Database,
		max_cached_at: i64,
		batch_size: usize,
	) -> tg::Result<u64> {
		let mut connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let batch_size = batch_size.to_i64().unwrap();

		// Find expired remote leaf tags.
		let statement = indoc!(
			"
				select id
				from tags
				where remote is not null
					and child_count = 0
					and (cached_at is null or cached_at <= $1)
				limit $2;
			"
		);
		let rows = transaction
			.inner()
			.query(statement, &[&max_cached_at, &batch_size])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let ids: Vec<i64> = rows.iter().map(|row| row.get(0)).collect();

		let count = ids.len().to_u64().unwrap();

		for id in ids {
			// Find the parent.
			let statement = indoc!(
				"
					select tag from tag_children where child = $1;
				"
			);
			let rows = transaction
				.inner()
				.query(statement, &[&id])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let parent_id: Option<i64> = rows.first().map(|row| row.get(0));

			// Delete the tag_children entry.
			let statement = indoc!(
				"
					delete from tag_children where child = $1;
				"
			);
			transaction
				.inner()
				.execute(statement, &[&id])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			// Update the parent: decrement child_count and clear cached_at.
			if let Some(parent_id) = parent_id {
				let statement = indoc!(
					"
						update tags set cached_at = null, child_count = child_count - 1
						where id = $1;
					"
				);
				transaction
					.inner()
					.execute(statement, &[&parent_id])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			}

			// Delete the tag.
			let statement = indoc!(
				"
					delete from tags where id = $1;
				"
			);
			transaction
				.inner()
				.execute(statement, &[&id])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(count)
	}
}
