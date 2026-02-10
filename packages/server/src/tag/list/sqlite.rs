use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

#[derive(Clone, Debug)]
pub struct Match {
	pub id: u64,
	pub tag: tg::Tag,
	pub item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
}

impl Server {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn list_tags_sqlite(
		&self,
		database: &db::sqlite::Database,
		arg: tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		// Get a database connection.
		let connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		let output = connection
			.with({
				let arg = arg.clone();
				move |connection, cache| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// List the tags.
					let output = Self::list_tags_sqlite_sync(&transaction, cache, &arg)?;

					Ok::<_, tg::Error>(output)
				}
			})
			.await?;

		Ok(output)
	}

	#[tracing::instrument(level = "trace", skip_all)]
	fn list_tags_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		arg: &tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		// Get all tags matching the pattern.
		let matches =
			Self::match_tags_sqlite_sync(transaction, cache, &arg.pattern, arg.recursive)?;

		let mut output = matches;

		// Expand the directory if necessary.
		if !arg.recursive
			&& !arg.pattern.as_str().contains(['*', '=', '>', '<', '^'])
			&& !arg.pattern.is_empty()
			&& output.len() == 1
			&& let Some(m) = output.first()
			&& m.item.is_none()
		{
			let _span = tracing::trace_span!("query_branch_children").entered();
			// This is a branch tag, get its children.
			#[derive(db::sqlite::row::Deserialize)]
			struct Row {
				#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
				id: u64,
				component: String,
				#[tangram_database(as = "Option<db::sqlite::value::TryFrom<String>>")]
				item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
			}
			let statement = indoc!(
				"
					select tags.id, tags.component, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = ?1 and tags.remote is null;
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let params = sqlite::params![m.id.to_i64().unwrap()];
			let mut rows = statement
				.query(params)
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let mut expanded = Vec::new();
			while let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to get the next row"))?
			{
				let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				let mut tag = m.tag.clone();
				tag.push(&row.component);
				let m = Match {
					id: row.id,
					tag,
					item: row.item,
				};
				expanded.push(m);
			}
			output = expanded;
		}

		// Sort the matches.
		output.sort_by(|a, b| a.tag.cmp(&b.tag));

		// Reverse if requested.
		if arg.reverse {
			output.reverse();
		}

		// Limit.
		if let Some(length) = arg.length {
			output.truncate(length.to_usize().unwrap());
		}

		// Create the output.
		let data = output
			.into_iter()
			.map(|m| tg::tag::get::Output {
				children: None,
				item: m.item,
				remote: None,
				tag: m.tag,
			})
			.collect();
		let output = tg::tag::list::Output { data };

		Ok(output)
	}

	#[tracing::instrument(level = "trace", skip_all, fields(pattern = %pattern, recursive))]
	pub fn match_tags_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::tag::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<Match>> {
		#[derive(db::sqlite::row::Deserialize)]
		struct TagRow {
			#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
			id: u64,
			component: String,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<String>>")]
			item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
		}

		#[derive(db::sqlite::row::Deserialize)]
		struct TagRowNoComponent {
			#[tangram_database(as = "db::sqlite::value::TryFrom<i64>")]
			id: u64,
			#[tangram_database(as = "Option<db::sqlite::value::TryFrom<String>>")]
			item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
		}

		// If the pattern is empty, return all root-level local tags.
		if pattern.is_empty() {
			let _span = tracing::trace_span!("query_root_tags").entered();
			let statement = indoc!(
				"
					select tags.id, tags.component, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = 0 and tags.remote is null;
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
			let mut rows = statement
				.query([])
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
			let mut matches = Vec::new();
			while let Some(row) = rows
				.next()
				.map_err(|source| tg::error!(!source, "failed to get the next row"))?
			{
				let row = <TagRow as db::sqlite::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))?;
				let mut tag = tg::Tag::empty();
				tag.push(&row.component);
				let m = Match {
					id: row.id,
					tag,
					item: row.item,
				};
				matches.push(m);
			}
			return Ok(matches);
		}

		let mut matches: Vec<Option<Match>> = vec![None];
		for pattern in pattern.components() {
			let mut new = Vec::new();
			for m in matches {
				if pattern == "*" {
					let _span = tracing::trace_span!("query_wildcard_children").entered();
					let statement = indoc!(
						"
							select tags.id, tags.component, tags.item
							from tag_children
							join tags on tag_children.child = tags.id
							where tag_children.tag = ?1 and tags.remote is null;
						"
					);
					let mut statement = cache
						.get(transaction, statement.into())
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![m.as_ref().map_or(0, |m| m.id.to_i64().unwrap())];
					let mut rows = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					while let Some(row) = rows
						.next()
						.map_err(|source| tg::error!(!source, "failed to get the next row"))?
					{
						let row = <TagRow as db::sqlite::row::Deserialize>::deserialize(row)
							.map_err(|source| {
								tg::error!(!source, "failed to deserialize the row")
							})?;
						let mut tag = m.as_ref().map_or_else(tg::Tag::empty, |m| m.tag.clone());
						tag.push(&row.component);
						let m = Match {
							id: row.id,
							tag,
							item: row.item,
						};
						new.push(Some(m));
					}
				} else if pattern.contains(['=', '>', '<', '^']) {
					let _span = tracing::trace_span!("query_version_children").entered();
					let statement = indoc!(
						"
							select tags.id, tags.component, tags.item
							from tag_children
							join tags on tag_children.child = tags.id
							where tag_children.tag = ?1 and tags.remote is null;
						"
					);
					let mut statement = cache
						.get(transaction, statement.into())
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![m.as_ref().map_or(0, |m| m.id.to_i64().unwrap())];
					let mut rows = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					while let Some(row) = rows
						.next()
						.map_err(|source| tg::error!(!source, "failed to get the next row"))?
					{
						let row = <TagRow as db::sqlite::row::Deserialize>::deserialize(row)
							.map_err(|source| {
								tg::error!(!source, "failed to deserialize the row")
							})?;
						if tg::tag::pattern::matches(&row.component, pattern) {
							let mut tag = m.as_ref().map_or_else(tg::Tag::empty, |m| m.tag.clone());
							tag.push(&row.component);
							let m = Match {
								id: row.id,
								tag,
								item: row.item,
							};
							new.push(Some(m));
						}
					}
				} else {
					let _span = tracing::trace_span!("query_exact_match").entered();
					let statement = indoc!(
						"
							select tags.id, tags.item
							from tag_children
							join tags on tag_children.child = tags.id
							where tag_children.tag = ?1 and tags.component = ?2 and tags.remote is null;
						"
					);
					let mut statement = cache
						.get(transaction, statement.into())
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![
						m.as_ref().map_or(0, |m| m.id.to_i64().unwrap()),
						pattern.to_string()
					];
					let mut rows = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					while let Some(row) = rows
						.next()
						.map_err(|source| tg::error!(!source, "failed to get the next row"))?
					{
						let row =
							<TagRowNoComponent as db::sqlite::row::Deserialize>::deserialize(row)
								.map_err(|source| {
								tg::error!(!source, "failed to deserialize the row")
							})?;
						let mut tag = m.as_ref().map_or_else(tg::Tag::empty, |m| m.tag.clone());
						tag.push(pattern);
						let m = Match {
							id: row.id,
							tag,
							item: row.item,
						};
						new.push(Some(m));
					}
				}
			}
			matches = new;
		}

		let mut output = matches.into_iter().flatten().collect::<Vec<_>>();

		// Recursively expand branches if requested.
		if recursive {
			let mut to_explore: Vec<Match> = output.clone();

			while let Some(m) = to_explore.pop() {
				if m.item.is_none() {
					let _span = tracing::trace_span!("query_recursive_children").entered();
					// This is a branch tag, get its children.
					let statement = indoc!(
						"
							select tags.id, tags.component, tags.item
							from tag_children
							join tags on tag_children.child = tags.id
							where tag_children.tag = ?1 and tags.remote is null;
						"
					);
					let mut statement = cache
						.get(transaction, statement.into())
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![m.id.to_i64().unwrap()];
					let mut rows = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

					while let Some(row) = rows
						.next()
						.map_err(|source| tg::error!(!source, "failed to get the next row"))?
					{
						let row = <TagRow as db::sqlite::row::Deserialize>::deserialize(row)
							.map_err(|source| {
								tg::error!(!source, "failed to deserialize the row")
							})?;
						let mut tag = m.tag.clone();
						tag.push(&row.component);
						let child = Match {
							id: row.id,
							tag,
							item: row.item,
						};
						output.push(child.clone());
						to_explore.push(child);
					}
				}
			}
		}

		Ok(output)
	}
}
