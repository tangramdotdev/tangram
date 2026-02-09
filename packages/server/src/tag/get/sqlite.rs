use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(super) async fn try_get_tag_sqlite(
		&self,
		database: &db::sqlite::Database,
		tag: &tg::Tag,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		if tag.is_empty() {
			return Ok(None);
		}

		// Get a database connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let tag = tag.clone();
		connection
			.with(move |connection, cache| {
				#[derive(db::sqlite::row::Deserialize)]
				struct Row {
					#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
					id: u64,
					#[tangram_database(as = "Option<db::sqlite::value::TryFrom<String>>")]
					item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
				}

				let transaction = connection
					.transaction()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

				let mut parent: i64 = 0;
				let mut last_row = None;
				for component in tag.components() {
					let statement = indoc!(
						"
							select id, item
							from tags
							where parent = ?1 and component = ?2;
						"
					);
					let mut statement = cache
						.get(&transaction, statement.into())
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![parent, component.to_string()];
					let mut rows = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					let Some(row) = rows
						.next()
						.map_err(|source| tg::error!(!source, "failed to get the next row"))?
					else {
						return Ok(None);
					};
					let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
						.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
					parent = row.id.to_i64().unwrap();
					last_row = Some(row);
				}

				// Return the output.
				let Some(row) = last_row else {
					return Ok(None);
				};

				let output = if let Some(item) = row.item {
					// This is a leaf tag with an item.
					tg::tag::get::Output {
						children: None,
						item: Some(item),
						remote: None,
						tag: tag.clone(),
					}
				} else {
					// This is a branch tag. Query the children.
					let statement = indoc!(
						"
							select component
							from tags
							where parent = ?1;
						"
					);
					let mut statement = cache
						.get(&transaction, statement.into())
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![parent];
					let mut rows = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					let mut children = Vec::new();
					while let Some(row) = rows
						.next()
						.map_err(|source| tg::error!(!source, "failed to get the next row"))?
					{
						let component: String = row
							.get(0)
							.map_err(|source| tg::error!(!source, "failed to get the component"))?;
						children.push(component);
					}
					tg::tag::get::Output {
						children: Some(children),
						item: None,
						remote: None,
						tag: tag.clone(),
					}
				};

				Ok(Some(output))
			})
			.await
	}
}
