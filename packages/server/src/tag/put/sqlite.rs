use {
	crate::Server,
	indoc::indoc,
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
				move |connection| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// Insert the tag.
					Self::put_tag_sqlite_sync(&transaction, &tag, &arg)?;

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
			let statement = indoc!(
				"
					insert into tags (parent, component)
					values (?1, ?2)
					on conflict (parent, component) do nothing;
				"
			);
			let mut statement = transaction
				.prepare_cached(statement)
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![parent, component.to_string()];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			#[derive(db::sqlite::row::Deserialize)]
			struct Row {
				id: u64,
				item: Option<String>,
			}
			let statement = indoc!(
				"
					select id, item
					from tags
					where parent = ?1 and component = ?2;
				"
			);
			let mut statement = transaction
				.prepare_cached(statement)
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![parent, component.to_string()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let row = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to execute the query"))?
				.ok_or_else(|| tg::error!("expected a row"))?;
			let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
				.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
			if row.item.is_some() {
				return Err(tg::error!(%ancestor, "found existing tag"));
			}
			parent = row.id;
		}

		// Ensure there is no branch for the leaf.
		let statement = indoc!(
			"
				select 1
				from tags
				where
					parent = ?1 and
					component = ?2 and
					item is null;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![parent, tag.components().last().unwrap().to_string(),];
		let exists = statement
			.query(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.next()
			.map_err(|source| tg::error!(!source, "failed to execute the query"))?
			.is_some();
		if exists {
			return Err(tg::error!("found existing branch"));
		}

		// Create the leaf.
		let statement = indoc!(
			"
				insert into tags (parent, component, item)
				values (?1, ?2, ?3)
				on conflict (parent, component) do update set item = ?3;
			"
		);
		let mut statement = transaction
			.prepare_cached(statement)
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![
			parent,
			tag.components().last().unwrap().to_string(),
			arg.item.to_string(),
		];
		statement
			.execute(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		Ok(())
	}
}
