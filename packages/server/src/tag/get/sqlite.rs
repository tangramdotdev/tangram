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
		self.try_get_tag_sqlite_with_remote(database, tag, None)
			.await
	}

	pub(crate) async fn try_get_tag_sqlite_with_remote(
		&self,
		database: &db::sqlite::Database,
		tag: &tg::Tag,
		remote: Option<&str>,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		// Get a database connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let tag = tag.clone();
		let remote = remote.map(ToOwned::to_owned);
		connection
			.with(move |connection, cache| {
				Self::try_get_tag_sqlite_sync(connection, cache, &tag, remote.as_deref())
			})
			.await
	}

	pub(crate) fn try_get_tag_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
		tag: &tg::Tag,
		remote: Option<&str>,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		// Handle the empty tag by returning root-level children.
		if tag.is_empty() {
			#[derive(db::sqlite::row::Deserialize)]
			struct ChildRow {
				component: String,
				#[tangram_database(as = "Option<db::sqlite::value::TryFrom<String>>")]
				item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
			}
			let statement = indoc!(
				"
					select tags.component, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = 0 and tags.remote is ?1;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![remote];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let mut children = Vec::new();
			while let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to get the next row"))?
			{
				let row = <ChildRow as db::sqlite::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				children.push(tg::tag::get::Child {
					component: row.component,
					item: row.item,
				});
			}
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

		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
			id: u64,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<String>>")]
			item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
		}

		let mut parent: i64 = 0;
		let mut last_row = None;
		for component in tag.components() {
			let statement = indoc!(
				"
					select tags.id, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = ?1 and tags.component = ?2 and tags.remote is ?3;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![parent, component.to_string(), remote];
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
			#[derive(db::sqlite::row::Deserialize)]
			struct ChildRow {
				component: String,
				#[tangram_database(as = "Option<db::sqlite::value::TryFrom<String>>")]
				item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
			}
			let statement = indoc!(
				"
					select tags.component, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = ?1;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
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
				let row = <ChildRow as db::sqlite::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				children.push(tg::tag::get::Child {
					component: row.component,
					item: row.item,
				});
			}
			tg::tag::get::Output {
				children: Some(children),
				item: None,
				remote: None,
				tag: tag.clone(),
			}
		};

		Ok(Some(output))
	}

	pub(crate) fn try_get_cached_tag_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
		tag: &tg::Tag,
		remote: &str,
		ttl: u64,
	) -> tg::Result<Option<tg::tag::get::Output>> {
		if tag.is_empty() {
			return Ok(None);
		}

		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
			id: u64,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<String>>")]
			item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
			cached_at: Option<i64>,
		}

		let now = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap()
			.as_secs()
			.to_i64()
			.unwrap();
		let ttl = ttl.to_i64().unwrap();

		let mut parent: i64 = 0;
		let mut last_row = None;
		for component in tag.components() {
			let statement = indoc!(
				"
					select tags.id, tags.item, tags.cached_at
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = ?1 and tags.component = ?2 and tags.remote = ?3;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![parent, component.to_string(), remote];
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

		// Check the TTL.
		let cached_at = row.cached_at.unwrap_or(0);
		if cached_at + ttl <= now {
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
			#[derive(db::sqlite::row::Deserialize)]
			struct ChildRow {
				component: String,
				#[tangram_database(as = "Option<db::sqlite::value::TryFrom<String>>")]
				item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
			}
			let statement = indoc!(
				"
					select tags.component, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = ?1;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
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
				let row = <ChildRow as db::sqlite::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				children.push(tg::tag::get::Child {
					component: row.component,
					item: row.item,
				});
			}
			tg::tag::get::Output {
				children: Some(children),
				item: None,
				remote: Some(remote.to_owned()),
				tag: tag.clone(),
			}
		};

		Ok(Some(output))
	}

	pub(crate) fn cache_remote_tag_sqlite_sync(
		connection: &sqlite::Connection,
		cache: &db::sqlite::Cache,
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

		// Walk or create the tree path for this remote.
		let mut parent: i64 = 0;
		let component_count = tag.components().count();
		for (i, component) in tag.components().enumerate() {
			// Check if the child already exists.
			let statement = indoc!(
				"
					select tags.id
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = ?1 and tags.component = ?2 and tags.remote = ?3;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![parent, component.to_string(), remote];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

			if let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to get the next row"))?
			{
				let id: i64 = row
					.get(0)
					.map_err(|source| tg::error!(!source, "failed to get the id"))?;
				parent = id;
			} else {
				drop(rows);

				// Insert a new tag node. Only set cached_at for the leaf node.
				let is_leaf = i + 1 == component_count;
				let item_str = if is_leaf {
					output.item.as_ref().map(ToString::to_string)
				} else {
					None
				};
				let cached_at: Option<i64> = if is_leaf { Some(now) } else { None };
				let statement = indoc!(
					"
						insert into tags (cached_at, component, item, remote)
						values (?1, ?2, ?3, ?4);
					"
				);
				let mut statement = cache
					.get(connection, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![cached_at, component.to_string(), item_str, remote];
				statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				let new_id = connection.last_insert_rowid();

				// Insert the parent-child relationship.
				let statement = indoc!(
					"
						insert into tag_children (tag, child)
						values (?1, ?2);
					"
				);
				let mut statement = cache
					.get(connection, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![parent, new_id];
				statement
					.execute(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				parent = new_id;
			}
		}

		// Update the leaf node's item and cached_at.
		let item_str = output.item.as_ref().map(ToString::to_string);
		let statement = indoc!(
			"
				update tags set item = ?1, cached_at = ?2
				where id = ?3;
			"
		);
		let mut statement = cache
			.get(connection, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![item_str, now, parent];
		statement
			.execute(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// If the output has children, store them as child nodes.
		if let Some(children) = &output.children {
			for child in children {
				// Check if this child already exists.
				let statement = indoc!(
					"
						select tags.id
						from tag_children
						join tags on tag_children.child = tags.id
						where tag_children.tag = ?1 and tags.component = ?2 and tags.remote = ?3;
					"
				);
				let mut statement = cache
					.get(connection, statement.into())
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![parent, &child.component, remote];
				let mut rows = statement
					.query(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

				if let Some(row) = rows
					.next()
					.map_err(|source| tg::error!(!source, "failed to get the next row"))?
				{
					// Update the existing child. Only update cached_at for leaf children.
					let child_id: i64 = row
						.get(0)
						.map_err(|source| tg::error!(!source, "failed to get the id"))?;
					drop(rows);
					let item_str = child.item.as_ref().map(ToString::to_string);
					if child.item.is_some() {
						let statement = indoc!(
							"
								update tags set cached_at = ?1, item = ?2
								where id = ?3;
							"
						);
						let mut statement =
							cache.get(connection, statement.into()).map_err(|source| {
								tg::error!(!source, "failed to prepare the statement")
							})?;
						let params = sqlite::params![now, item_str, child_id];
						statement.execute(params).map_err(|source| {
							tg::error!(!source, "failed to execute the statement")
						})?;
					} else {
						let statement = indoc!(
							"
								update tags set item = ?1
								where id = ?2;
							"
						);
						let mut statement =
							cache.get(connection, statement.into()).map_err(|source| {
								tg::error!(!source, "failed to prepare the statement")
							})?;
						let params = sqlite::params![item_str, child_id];
						statement.execute(params).map_err(|source| {
							tg::error!(!source, "failed to execute the statement")
						})?;
					}
				} else {
					drop(rows);

					// Insert a new child node. Only set cached_at for leaf children.
					let item_str = child.item.as_ref().map(ToString::to_string);
					let cached_at: Option<i64> = if child.item.is_some() {
						Some(now)
					} else {
						None
					};
					let statement = indoc!(
						"
							insert into tags (cached_at, component, item, remote)
							values (?1, ?2, ?3, ?4);
						"
					);
					let mut statement = cache
						.get(connection, statement.into())
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![cached_at, &child.component, item_str, remote];
					statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					let new_id = connection.last_insert_rowid();

					let statement = indoc!(
						"
							insert into tag_children (tag, child)
							values (?1, ?2);
						"
					);
					let mut statement = cache
						.get(connection, statement.into())
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![parent, new_id];
					statement
						.execute(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				}
			}
		}

		// Delete stale cached children that are no longer present in the remote response.
		let valid_components: std::collections::HashSet<&str> = output
			.children
			.as_ref()
			.map(|children| children.iter().map(|c| c.component.as_str()).collect())
			.unwrap_or_default();
		let statement = indoc!(
			"
				select tags.id, tags.component
				from tag_children
				join tags on tag_children.child = tags.id
				where tag_children.tag = ?1 and tags.remote = ?2;
			"
		);
		let mut statement = cache
			.get(connection, statement.into())
			.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
		let params = sqlite::params![parent, remote];
		let mut rows = statement
			.query(params)
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		let mut stale_ids = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|source| tg::error!(!source, "failed to get the next row"))?
		{
			let id: i64 = row
				.get(0)
				.map_err(|source| tg::error!(!source, "failed to get the id"))?;
			let component: String = row
				.get(1)
				.map_err(|source| tg::error!(!source, "failed to get the component"))?;
			if !valid_components.contains(component.as_str()) {
				stale_ids.push(id);
			}
		}
		drop(rows);
		for stale_id in stale_ids {
			let statement = indoc!(
				"
					delete from tag_children where tag = ?1 and child = ?2;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![parent, stale_id];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let statement = indoc!(
				"
					delete from tags where id = ?1;
				"
			);
			let mut statement = cache
				.get(connection, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![stale_id];
			statement
				.execute(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		}

		Ok(())
	}
}
