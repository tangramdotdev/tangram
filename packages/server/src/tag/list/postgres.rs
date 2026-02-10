use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tracing::Instrument as _,
};

#[derive(Clone, Debug)]
pub struct Match {
	pub id: u64,
	pub tag: tg::Tag,
	pub item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
}

#[derive(db::postgres::row::Deserialize)]
struct RowWithComponent {
	#[tangram_database(as = "db::postgres::value::TryFrom<i64>")]
	id: u64,
	component: String,
	#[tangram_database(as = "Option<db::postgres::value::TryFrom<String>>")]
	item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
}

#[derive(db::postgres::row::Deserialize)]
struct RowWithoutComponent {
	#[tangram_database(as = "db::postgres::value::TryFrom<i64>")]
	id: u64,
	#[tangram_database(as = "Option<db::postgres::value::TryFrom<String>>")]
	item: Option<tg::Either<tg::object::Id, tg::process::Id>>,
}

impl Server {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn list_tags_postgres(
		&self,
		database: &tangram_database::postgres::Database,
		arg: tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
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

		// Get all tags matching the pattern.
		let matches = Self::match_tags_postgres(&transaction, &arg.pattern, arg.recursive).await?;

		let mut output = matches;

		// Expand the directory if necessary.
		if !arg.recursive
			&& !arg.pattern.as_str().contains(['*', '=', '>', '<', '^'])
			&& !arg.pattern.is_empty()
			&& output.len() == 1
			&& let Some(m) = output.first()
			&& m.item.is_none()
		{
			// This is a branch tag, get its children.
			let statement = indoc!(
				"
					select tags.id, tags.component, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = $1 and tags.remote is null;
				"
			);
			let rows = async {
				transaction
					.inner()
					.query(statement, &[&m.id.to_i64().unwrap()])
					.await
			}
			.instrument(tracing::trace_span!("query_branch_children"))
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
			.iter()
			.map(|row| {
				<RowWithComponent as db::postgres::row::Deserialize>::deserialize(row)
					.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
			})
			.collect::<tg::Result<Vec<_>>>()?;
			output = rows
				.into_iter()
				.map(|row| {
					let mut tag = m.tag.clone();
					tag.push(&row.component);
					Match {
						id: row.id,
						tag,
						item: row.item,
					}
				})
				.collect();
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
	pub async fn match_tags_postgres(
		transaction: &tangram_database::postgres::Transaction<'_>,
		pattern: &tg::tag::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<Match>> {
		// If the pattern is empty, return all root-level local tags.
		if pattern.is_empty() {
			let statement = indoc!(
				"
					select tags.id, tags.component, tags.item
					from tag_children
					join tags on tag_children.child = tags.id
					where tag_children.tag = 0 and tags.remote is null;
				"
			);
			let rows = async { transaction.inner().query(statement, &[]).await }
				.instrument(tracing::trace_span!("query_root_tags"))
				.await
				.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
				.iter()
				.map(|row| {
					<RowWithComponent as db::postgres::row::Deserialize>::deserialize(row)
						.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
				})
				.collect::<tg::Result<Vec<_>>>()?;
			let matches = rows
				.into_iter()
				.map(|row| {
					let mut tag = tg::Tag::empty();
					tag.push(&row.component);
					Match {
						id: row.id,
						tag,
						item: row.item,
					}
				})
				.collect();
			return Ok(matches);
		}

		let mut matches: Vec<Option<Match>> = vec![None];
		for pattern in pattern.components() {
			let mut new = Vec::new();
			for m in matches {
				if pattern == "*" {
					let statement = indoc!(
						"
							select tags.id, tags.component, tags.item
							from tag_children
							join tags on tag_children.child = tags.id
							where tag_children.tag = $1 and tags.remote is null;
						"
					);
					let rows = async {
						transaction
							.inner()
							.query(
								statement,
								&[&m.as_ref().map_or(0, |m| m.id.to_i64().unwrap())],
							)
							.await
					}
					.instrument(tracing::trace_span!("query_wildcard_children"))
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
					.iter()
					.map(|row| {
						<RowWithComponent as db::postgres::row::Deserialize>::deserialize(row)
							.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
					})
					.collect::<tg::Result<Vec<_>>>()?;
					new.extend(rows.into_iter().map(|row| {
						let mut tag = m.as_ref().map_or_else(tg::Tag::empty, |m| m.tag.clone());
						tag.push(&row.component);
						Some(Match {
							id: row.id,
							tag,
							item: row.item,
						})
					}));
				} else if pattern.contains(['=', '>', '<', '^']) {
					let statement = indoc!(
						"
							select tags.id, tags.component, tags.item
							from tag_children
							join tags on tag_children.child = tags.id
							where tag_children.tag = $1 and tags.remote is null;
						"
					);
					let rows = async {
						transaction
							.inner()
							.query(
								statement,
								&[&m.as_ref().map_or(0, |m| m.id.to_i64().unwrap())],
							)
							.await
					}
					.instrument(tracing::trace_span!("query_version_children"))
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
					.iter()
					.map(|row| {
						<RowWithComponent as db::postgres::row::Deserialize>::deserialize(row)
							.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
					})
					.collect::<tg::Result<Vec<_>>>()?;
					new.extend(
						rows.into_iter()
							.filter(|row| tg::tag::pattern::matches(&row.component, pattern))
							.map(|row| {
								let mut tag =
									m.as_ref().map_or_else(tg::Tag::empty, |m| m.tag.clone());
								tag.push(&row.component);
								Some(Match {
									id: row.id,
									tag,
									item: row.item,
								})
							}),
					);
				} else {
					let statement = indoc!(
						"
							select tags.id, tags.item
							from tag_children
							join tags on tag_children.child = tags.id
							where tag_children.tag = $1 and tags.component = $2 and tags.remote is null;
						"
					);
					let rows = async {
						transaction
							.inner()
							.query(
								statement,
								&[
									&m.as_ref().map_or(0, |m| m.id.to_i64().unwrap()),
									&pattern.to_string(),
								],
							)
							.await
					}
					.instrument(tracing::trace_span!("query_exact_match"))
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
					.iter()
					.map(|row| {
						<RowWithoutComponent as db::postgres::row::Deserialize>::deserialize(row)
							.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
					})
					.collect::<tg::Result<Vec<_>>>()?;
					new.extend(rows.into_iter().map(|row| {
						let mut tag = m.as_ref().map_or_else(tg::Tag::empty, |m| m.tag.clone());
						tag.push(pattern);
						Some(Match {
							id: row.id,
							tag,
							item: row.item,
						})
					}));
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
					// This is a branch tag, get its children.
					let statement = indoc!(
						"
							select tags.id, tags.component, tags.item
							from tag_children
							join tags on tag_children.child = tags.id
							where tag_children.tag = $1 and tags.remote is null;
						"
					);
					let rows = async {
						transaction
							.inner()
							.query(statement, &[&m.id.to_i64().unwrap()])
							.await
					}
					.instrument(tracing::trace_span!("query_recursive_children"))
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?
					.iter()
					.map(|row| {
						<RowWithComponent as db::postgres::row::Deserialize>::deserialize(row)
							.map_err(|source| tg::error!(!source, "failed to deserialize the row"))
					})
					.collect::<tg::Result<Vec<_>>>()?;
					for row in rows {
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
