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
		let mut parent = 0;
		let mut ancestor = tg::Tag::empty();
		for component in tag.components().take(tag.components().count() - 1) {
			ancestor.push(component);
			let statement = indoc!(
				"
					insert into tags (parent, component)
					values ($1, $2)
					on conflict (parent, component) do nothing;
				"
			);
			let params = db::params![parent, component.to_string()];
			transaction
				.execute(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			#[derive(db::row::Deserialize)]
			struct Row {
				id: u64,
				item: Option<String>,
			}
			let statement = indoc!(
				"
					select id, item
					from tags
					where parent = $1 and component = $2;
				"
			);
			let params = db::params![parent, component.to_string()];
			let row = transaction
				.query_one_into::<Row>(statement.into(), params)
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
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
					parent = $1 and
					component = $2 and
					item is null;
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

		// Create the leaf.
		let statement = indoc!(
			"
				insert into tags (parent, component, item)
				values ($1, $2, $3)
				on conflict (parent, component) do update set item = $3;
			"
		);
		let params = db::params![
			parent,
			tag.components().last().unwrap().to_string(),
			arg.item.to_string(),
		];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}
}
