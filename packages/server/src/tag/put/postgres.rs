use {
	crate::Server,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn put_tag_postgres(
		database: &db::postgres::Database,
		tag: &tg::Tag,
		arg: &tg::tag::put::Arg,
	) -> tg::Result<()> {
		// Get a database connection.
		let mut connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let mut transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Perform the transaction.
		Self::put_tag_postgres_inner(&mut transaction, tag, arg)
			.await
			.map_err(|source| tg::error!(!source, "failed to perform the transaction"))?;

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}

	pub(crate) async fn put_tag_postgres_inner(
		transaction: &mut db::postgres::Transaction<'_>,
		tag: &tg::Tag,
		arg: &tg::tag::put::Arg,
	) -> tg::Result<()> {
		if tag.is_empty() {
			return Err(tg::error!("cannot put an empty tag"));
		}

		// Create the branches.
		let mut parent: i64 = 0;
		let mut ancestor = tg::Tag::empty();
		for component in tag.components().take(tag.components().count() - 1) {
			ancestor.push(component);

			// Check if this branch already exists.
			let statement = indoc!(
				"
					select tags.id, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = $1 and tags.component = $2;
				"
			);
			let rows = transaction
				.inner()
				.query(statement, &[&parent, &component.to_string()])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if let Some(row) = rows.first() {
				#[derive(db::postgres::row::Deserialize)]
				struct Row {
					id: i64,
					item: Option<String>,
				}
				let row = <Row as db::postgres::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				if row.item.is_some() {
					return Err(tg::error!(%ancestor, "found existing tag"));
				}
				parent = row.id;
			} else {
				// Insert the branch node.
				let statement = indoc!(
					"
						insert into tags (component)
						values ($1)
						returning id;
					"
				);
				let rows = transaction
					.inner()
					.query(statement, &[&component.to_string()])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let new_id: i64 = rows.first().unwrap().get(0);

				// Insert the parent-child relationship.
				let statement = indoc!(
					"
						insert into tag_children (tag, child)
						values ($1, $2);
					"
				);
				transaction
					.inner()
					.execute(statement, &[&parent, &new_id])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				parent = new_id;
			}
		}

		// Ensure there is no branch for the leaf.
		let statement = indoc!(
			"
				select tags.id
				from tag_children
				join tags on tag_children.child = tags.id
				where tag_children.tag = $1 and tags.component = $2 and tags.item is null;
			"
		);
		let params = db::params![parent, tag.components().last().unwrap().to_string(),];
		let exists = transaction
			.query_optional(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
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
				where tag_children.tag = $1 and tags.component = $2;
			"
		);
		let rows = transaction
			.inner()
			.query(
				statement,
				&[&parent, &tag.components().last().unwrap().to_string()],
			)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		if let Some(row) = rows.first() {
			// Update the existing leaf.
			let id: i64 = row.get(0);
			let statement = indoc!(
				"
					update tags set item = $1
					where id = $2;
				"
			);
			transaction
				.inner()
				.execute(statement, &[&arg.item.to_string(), &id])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		} else {
			// Insert the leaf node.
			let statement = indoc!(
				"
					insert into tags (component, item)
					values ($1, $2)
					returning id;
				"
			);
			let rows = transaction
				.inner()
				.query(
					statement,
					&[
						&tag.components().last().unwrap().to_string(),
						&arg.item.to_string(),
					],
				)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let new_id: i64 = rows.first().unwrap().get(0);

			// Insert the parent-child relationship.
			let statement = indoc!(
				"
					insert into tag_children (tag, child)
					values ($1, $2);
				"
			);
			transaction
				.inner()
				.execute(statement, &[&parent, &new_id])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}
		Ok(())
	}
}
