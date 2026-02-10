use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
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
		self.try_get_tag_postgres_with_remote(database, tag, None)
			.await
	}

	pub(crate) async fn try_get_tag_postgres_with_remote(
		&self,
		database: &db::postgres::Database,
		tag: &tg::Tag,
		remote: Option<&str>,
	) -> tg::Result<Option<tg::tag::get::Output>> {
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

		// Handle the empty tag by returning root-level children.
		if tag.is_empty() {
			#[derive(db::postgres::row::Deserialize)]
			struct ChildRow {
				component: String,
				#[tangram_database(as = "Option<db::postgres::value::TryFrom<String>>")]
				item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
			}
			let remote_str = remote.map(ToOwned::to_owned);
			let statement = indoc!(
				"
					select tags.component, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = 0 and tags.remote is not distinct from $1;
				"
			);
			let rows = async { transaction.inner().query(statement, &[&remote_str]).await }
				.instrument(tracing::trace_span!("query_root_children"))
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let children: Vec<tg::tag::get::Child> = rows
				.iter()
				.map(|row| {
					let row = <ChildRow as db::postgres::row::Deserialize>::deserialize(row)
						.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
					Ok(tg::tag::get::Child {
						component: row.component,
						item: row.item,
					})
				})
				.collect::<tg::Result<Vec<_>>>()?;
			if children.is_empty() {
				return Ok(None);
			}
			return Ok(Some(tg::tag::get::Output {
				children: Some(children),
				item: None,
				remote: None,
				tag: tg::Tag::empty(),
			}));
		}

		// Walk the tag components through the tree.
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::postgres::value::TryFrom<i64>")]
			id: u64,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<String>>")]
			item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
		}

		let remote_str = remote.map(ToOwned::to_owned);
		let mut parent: i64 = 0;
		let mut last_row = None;
		for component in tag.components() {
			let statement = indoc!(
				"
					select tags.id, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = $1 and tags.component = $2 and tags.remote is not distinct from $3;
				"
			);
			let rows = async {
				transaction
					.inner()
					.query(statement, &[&parent, &component.to_string(), &remote_str])
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
			#[derive(db::postgres::row::Deserialize)]
			struct ChildRow {
				component: String,
				#[tangram_database(as = "Option<db::postgres::value::TryFrom<String>>")]
				item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
			}
			let statement = indoc!(
				"
					select tags.component, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = $1;
				"
			);
			let rows = async { transaction.inner().query(statement, &[&parent]).await }
				.instrument(tracing::trace_span!("query_branch_children"))
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let children = rows
				.iter()
				.map(|row| {
					let row = <ChildRow as db::postgres::row::Deserialize>::deserialize(row)
						.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
					Ok(tg::tag::get::Child {
						component: row.component,
						item: row.item,
					})
				})
				.collect::<tg::Result<Vec<_>>>()?;
			tg::tag::get::Output {
				children: Some(children),
				item: None,
				remote: None,
				tag: tag.clone(),
			}
		};

		Ok(Some(output))
	}

	pub(crate) async fn try_get_cached_tag_postgres(
		&self,
		database: &db::postgres::Database,
		tag: &tg::Tag,
		remote: &str,
		ttl: u64,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		if tag.is_empty() {
			return Ok(None);
		}

		let now = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_secs()
			.to_i64()
			.unwrap();
		let ttl = ttl.to_i64().unwrap();

		// Get a database connection.
		let mut connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::postgres::value::TryFrom<i64>")]
			id: u64,
			#[tangram_database(as = "Option<db::postgres::value::TryFrom<String>>")]
			item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
			cached_at: Option<i64>,
		}

		let remote_str = remote.to_owned();
		let mut parent: i64 = 0;
		let mut last_row = None;
		for component in tag.components() {
			let statement = indoc!(
				"
					select tags.id, tags.item, tags.cached_at
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = $1 and tags.component = $2 and tags.remote = $3;
				"
			);
			let rows = transaction
				.inner()
				.query(statement, &[&parent, &component.to_string(), &remote_str])
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

		let Some(row) = last_row else {
			return Ok(None);
		};

		// Check the TTL.
		let cached_at = row.cached_at.unwrap_or(0);
		if cached_at + ttl < now {
			return Ok(None);
		}

		let output = if let Some(item) = row.item {
			tg::tag::get::Output {
				children: None,
				item: Some(item),
				remote: Some(remote.to_owned()),
				tag: tag.clone(),
			}
		} else {
			// This is a branch tag. Query the children.
			#[derive(db::postgres::row::Deserialize)]
			struct ChildRow {
				component: String,
				#[tangram_database(as = "Option<db::postgres::value::TryFrom<String>>")]
				item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
			}
			let statement = indoc!(
				"
					select tags.component, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = $1;
				"
			);
			let rows = transaction
				.inner()
				.query(statement, &[&parent])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let children = rows
				.iter()
				.map(|row| {
					let row = <ChildRow as db::postgres::row::Deserialize>::deserialize(row)
						.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
					Ok(tg::tag::get::Child {
						component: row.component,
						item: row.item,
					})
				})
				.collect::<tg::Result<Vec<_>>>()?;
			tg::tag::get::Output {
				children: Some(children),
				item: None,
				remote: Some(remote.to_owned()),
				tag: tag.clone(),
			}
		};

		Ok(Some(output))
	}

	pub(crate) async fn cache_remote_tag_postgres(
		&self,
		database: &db::postgres::Database,
		remote: &str,
		tag: &tg::Tag,
		output: &tg::tag::get::Output,
	) -> tg::Result<()> {
		let now = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_secs()
			.to_i64()
			.unwrap();

		let mut connection = database
			.write_connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		let remote_str = remote.to_owned();

		// Walk or create the tree path for this remote.
		let mut parent: i64 = 0;
		let component_count = tag.components().count();
		for (i, component) in tag.components().enumerate() {
			let statement = indoc!(
				"
					select tags.id
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = $1 and tags.component = $2 and tags.remote = $3;
				"
			);
			let rows = transaction
				.inner()
				.query(statement, &[&parent, &component.to_string(), &remote_str])
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if let Some(row) = rows.first() {
				let id: i64 = row.get(0);
				parent = id;
			} else {
				// Insert a new tag node.
				let item_str = if i + 1 == component_count {
					output.item.as_ref().map(ToString::to_string)
				} else {
					None
				};
				let statement = indoc!(
					"
						insert into tags (cached_at, component, item, remote)
						values ($1, $2, $3, $4)
						returning id;
					"
				);
				let rows = transaction
					.inner()
					.query(
						statement,
						&[&now, &component.to_string(), &item_str, &remote_str],
					)
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let new_id: i64 = rows.first().unwrap().get(0);

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

		// Update the leaf node's item and cached_at.
		let item_str = output.item.as_ref().map(ToString::to_string);
		let statement = indoc!(
			"
				update tags set item = $1, cached_at = $2
				where id = $3;
			"
		);
		transaction
			.inner()
			.execute(statement, &[&item_str, &now, &parent])
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// If the output has children, store them as child nodes.
		if let Some(children) = &output.children {
			for child in children {
				let statement = indoc!(
					"
						select tags.id
						from tag_children
						join tags on tag_children.child = tags.id
						where tag_children.tag = $1 and tags.component = $2 and tags.remote = $3;
					"
				);
				let rows = transaction
					.inner()
					.query(statement, &[&parent, &child.component, &remote_str])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				if let Some(row) = rows.first() {
					let child_id: i64 = row.get(0);
					let item_str = child.item.as_ref().map(ToString::to_string);
					let statement = indoc!(
						"
							update tags set cached_at = $1, item = $2
							where id = $3;
						"
					);
					transaction
						.inner()
						.execute(statement, &[&now, &item_str, &child_id])
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				} else {
					let item_str = child.item.as_ref().map(ToString::to_string);
					let statement = indoc!(
						"
							insert into tags (cached_at, component, item, remote)
							values ($1, $2, $3, $4)
							returning id;
						"
					);
					let rows = transaction
						.inner()
						.query(statement, &[&now, &child.component, &item_str, &remote_str])
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					let new_id: i64 = rows.first().unwrap().get(0);

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
			}
		}

		// Commit the transaction.
		transaction
			.commit()
			.await
			.map_err(|source| tg::error!(!source, "failed to commit the transaction"))?;

		Ok(())
	}
}
