use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn put_tag_sqlite(
		database: &db::sqlite::Database,
		tag: &tg::Tag,
		arg: &tg::tag::put::Arg,
	) -> tg::Result<()> {
		// Get a database connection.
		let connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		connection
			.with({
				let tag = tag.clone();
				let arg = arg.clone();
				move |connection, cache| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Insert the tag.
					Self::put_tag_sqlite_sync(&transaction, cache, &tag, &arg)?;

					// Commit the transaction.
					transaction.commit().map_err(|source| {
						tg::error!(!source, "failed to commit the transaction")
					})?;

					Ok::<_, tg::Error>(())
				}
			})
			.await?;

		Ok(())
	}

	pub(crate) fn put_tag_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		tag: &tg::Tag,
		arg: &tg::tag::put::Arg,
	) -> tg::Result<()> {
		if tag.is_empty() {
			return Err(tg::error!("cannot put an empty tag"));
		}

		// Create the branches.
		let mut parent = 0;
		let mut ancestor = tg::Tag::empty();
		for component in tag.components().take(tag.components().count() - 1) {
			ancestor.push(component);

			// Check if this branch already exists.
			#[derive(db::sqlite::row::Deserialize)]
			struct Row {
				#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
				id: u64,
				item: Option<String>,
			}
			let statement = indoc!(
				"
					select tags.id, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = ?1 and tags.component = ?2;
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![parent.to_i64().unwrap(), component.to_string()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to get the next row"))?
			{
				let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				if row.item.is_some() {
					return Err(tg::error!(%ancestor, "found existing tag"));
				}
				parent = row.id;
			} else {
				drop(rows);

				// Insert the branch node.
				let statement = indoc!(
					"
						insert into tags (component)
						values (?1);
					"
				);
				let mut statement = cache
					.get(transaction, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![component.to_string()];
				statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let new_id = transaction.last_insert_rowid();

				// Insert the parent-child relationship.
				let statement = indoc!(
					"
						insert into tag_children (tag, child)
						values (?1, ?2);
					"
				);
				let mut statement = cache
					.get(transaction, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![parent.to_i64().unwrap(), new_id];
				statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				parent = new_id.to_u64().unwrap();
			}
		}

		// Ensure there is no branch for the leaf.
		let statement = indoc!(
			"
				select tags.id
				from tag_children
				join tags on tag_children.child = tags.id
				where tag_children.tag = ?1 and tags.component = ?2 and tags.item is null;
			"
		);
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![
			parent.to_i64().unwrap(),
			tag.components().last().unwrap().to_string(),
		];
		let exists = statement
			.query(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the query"))?
			.is_some();
		if exists {
			return Err(tg::error!("found existing branch"));
		}

		// Check if the leaf already exists.
		let statement = indoc!(
			"
				select tags.id
				from tag_children
				join tags on tag_children.child = tags.id
				where tag_children.tag = ?1 and tags.component = ?2;
			"
		);
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![
			parent.to_i64().unwrap(),
			tag.components().last().unwrap().to_string(),
		];
		let mut rows = statement
			.query(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		if let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to get the next row"))?
		{
			// Update the existing leaf.
			let id: i64 = row
				.get(0)
				.map_err(|source| tg::error!(!source, "failed to get the id"))?;
			drop(rows);
			let statement = indoc!(
				"
					update tags set item = ?1
					where id = ?2;
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![arg.item.to_string(), id];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		} else {
			drop(rows);

			// Insert the leaf node.
			let statement = indoc!(
				"
					insert into tags (component, item)
					values (?1, ?2);
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![
				tag.components().last().unwrap().to_string(),
				arg.item.to_string(),
			];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let new_id = transaction.last_insert_rowid();

			// Insert the parent-child relationship.
			let statement = indoc!(
				"
					insert into tag_children (tag, child)
					values (?1, ?2);
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![parent.to_i64().unwrap(), new_id];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}
}
