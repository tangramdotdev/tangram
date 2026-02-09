use {
	crate::Server,
	indoc::indoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tracing::Instrument as _,
};

impl Server {
	pub(super) async fn try_get_tag_postgres(
		&self,
		database: &db::postgres::Database,
		tag: &tg::Tag,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		if tag.is_empty() {
			return Ok(None);
		}

		// Get a database connection.
		let mut connection = database
			.connection()
			.instrument(tracing::trace_span!("connection"))
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.instrument(tracing::trace_span!("begin_transaction"))
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Walk the tag components through the tree.
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::postgres::value::TryFrom<i64>")]
			id: u64,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<String>>")]
			item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
		}

		let mut parent: i64 = 0;
		let mut last_row = None;
		for component in tag.components() {
			let statement = indoc!(
				"
					select id, item
					from tags
					where parent = $1 and component = $2;
				"
			);
			let rows = async {
				transaction
					.inner()
					.query(statement, &[&parent, &component.to_string()])
					.await
			}
			.instrument(tracing::trace_span!("query_component"))
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let Some(row) = rows.first() else {
				return Ok(None);
			};
			let row = <Row as db::postgres::row::Deserialize>::deserialize(row)
				.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
			parent = row.id.try_into().unwrap();
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
					where parent = $1;
				"
			);
			let rows = async { transaction.inner().query(statement, &[&parent]).await }
				.instrument(tracing::trace_span!("query_branch_children"))
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let children = rows
				.iter()
				.map(|row| {
					let component: String = row.get(0);
					component
				})
				.collect();
			tg::tag::get::Output {
				children: Some(children),
				item: None,
				remote: None,
				tag: tag.clone(),
			}
		};

		Ok(Some(output))
	}
}
