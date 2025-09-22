use crate::Server;
use indoc::indoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

impl Server {
	pub(crate) async fn put_tag_postgres(
		database: &db::postgres::Database,
		tag: &tg::Tag,
		arg: &tg::tag::put::Arg,
	) -> tg::Result<()> {
		if tag.is_empty() {
			return Err(tg::error!("cannot put an empty tag"));
		}

		// Get a database connection.
		let mut connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Create the branches.
		let mut parent = 0;
		let mut ancestor = tg::Tag::empty();
		for component in tag.components().iter().take(tag.components().len() - 1) {
			let mut components = ancestor.components().clone();
			components.push(component.clone());
			ancestor = tg::Tag::with_components(components);
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
			#[derive(serde::Deserialize)]
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
				.query_one_into::<db::row::Serde<Row>>(statement.into(), params)
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

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}
}
