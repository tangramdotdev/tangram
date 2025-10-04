use {
	crate::Server, indoc::indoc, num::ToPrimitive as _, tangram_client as tg,
	tangram_database::prelude::*, tangram_either::Either,
};

#[derive(Clone, Debug)]
pub(in crate::tag) struct Match {
	pub id: u64,
	pub tag: tg::Tag,
	pub item: Option<Either<tg::process::Id, tg::object::Id>>,
}

impl Server {
	pub(super) async fn list_tags_postgres(
		&self,
		database: &tangram_database::postgres::Database,
		arg: tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		let mut connection = database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Begin a transaction.
		let transaction = connection
			.transaction()
			.await
			.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

		// Get all tags matching the pattern.
		let matches = Self::match_tags_postgres(&transaction, &arg.pattern).await?;

		// Auto-expand all directories (like ls on directories).
		let mut expanded = Vec::new();
		for m in matches {
			if m.item.is_none() {
				// This is a branch tag, get its children.
				let statement = indoc!(
					"
						select id, component, item
						from tags
						where parent = $1;
					"
				);
				let rows = transaction
					.inner()
					.query(statement, &[&m.id.to_i64().unwrap()])
					.await
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				for row in rows {
					let id = row
						.try_get::<_, i64>(0)
						.map_err(|source| tg::error!(!source, "failed to get the id"))?
						.to_u64()
						.unwrap();
					let component = row
						.try_get::<_, String>(1)
						.map_err(|source| tg::error!(!source, "failed to get the id"))?;
					let item = row
						.try_get::<_, Option<String>>(2)
						.map_err(|source| tg::error!(!source, "failed to get the item"))?
						.map(|s| s.parse())
						.transpose()
						.map_err(|source| tg::error!(!source, "failed to parse the item"))?;
					let mut tag = m.tag.clone();
					tag.push(&component);
					let m = Match { id, tag, item };
					expanded.push(m);
				}
			} else {
				// This is a leaf tag, keep it as is.
				expanded.push(m);
			}
		}

		let mut output = expanded;

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
				tag: m.tag,
				item: m.item,
				remote: None,
			})
			.collect();
		let output = tg::tag::list::Output { data };

		Ok(output)
	}

	pub(in crate::tag) async fn match_tags_postgres(
		transaction: &tangram_database::postgres::Transaction<'_>,
		pattern: &tg::tag::Pattern,
	) -> tg::Result<Vec<Match>> {
		let mut matches: Vec<Option<Match>> = vec![None];
		for pattern in pattern.components() {
			let mut new = Vec::new();
			for m in matches {
				if pattern == "*" {
					let statement = indoc!(
						"
							select id, component, item
							from tags
							where parent = $1;
						"
					);
					let rows = transaction
						.inner()
						.query(
							statement,
							&[&m.as_ref().map_or(0, |m| m.id.to_i64().unwrap())],
						)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					for row in rows {
						let id = row
							.try_get::<_, i64>(0)
							.map_err(|source| tg::error!(!source, "failed to get the id"))?
							.to_u64()
							.unwrap();
						let component = row
							.try_get::<_, String>(1)
							.map_err(|source| tg::error!(!source, "failed to get the id"))?;
						let item = row
							.try_get::<_, Option<String>>(2)
							.map_err(|source| tg::error!(!source, "failed to get the item"))?
							.map(|s| s.parse())
							.transpose()
							.map_err(|source| tg::error!(!source, "failed to parse the item"))?;
						let mut tag = m.as_ref().map_or_else(tg::Tag::empty, |m| m.tag.clone());
						tag.push(&component);
						let m = Match { id, tag, item };
						new.push(Some(m));
					}
				} else if pattern.contains(['=', '>', '<', '^']) {
					let statement = indoc!(
						"
							select id, component, item
							from tags
							where parent = $1;
						"
					);
					let rows = transaction
						.inner()
						.query(
							statement,
							&[&m.as_ref().map_or(0, |m| m.id.to_i64().unwrap())],
						)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					for row in rows {
						let id = row
							.try_get::<_, i64>(0)
							.map_err(|source| tg::error!(!source, "failed to get the id"))?
							.to_u64()
							.unwrap();
						let component = row
							.try_get::<_, String>(1)
							.map_err(|source| tg::error!(!source, "failed to get the id"))?;
						let item = row
							.try_get::<_, Option<String>>(2)
							.map_err(|source| tg::error!(!source, "failed to get the item"))?
							.map(|s| s.parse())
							.transpose()
							.map_err(|source| tg::error!(!source, "failed to parse the item"))?;
						if tg::tag::pattern::matches(&component, pattern) {
							let mut tag = m.as_ref().map_or_else(tg::Tag::empty, |m| m.tag.clone());
							tag.push(&component);
							let m = Match { id, tag, item };
							new.push(Some(m));
						}
					}
				} else {
					let statement = indoc!(
						"
							select id, item
							from tags
							where parent = $1 and component = $2;
						"
					);
					let rows = transaction
						.inner()
						.query(
							statement,
							&[
								&m.as_ref().map_or(0, |m| m.id.to_i64().unwrap()),
								&pattern.to_string(),
							],
						)
						.await
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					for row in rows {
						let id = row
							.try_get::<_, i64>(0)
							.map_err(|source| tg::error!(!source, "failed to get the id"))?
							.to_u64()
							.unwrap();
						let item = row
							.try_get::<_, Option<String>>(1)
							.map_err(|source| tg::error!(!source, "failed to get the item"))?
							.map(|s| s.parse())
							.transpose()
							.map_err(|source| tg::error!(!source, "failed to parse the item"))?;
						let mut tag = m.as_ref().map_or_else(tg::Tag::empty, |m| m.tag.clone());
						tag.push(pattern);
						let m = Match { id, tag, item };
						new.push(Some(m));
					}
				}
			}
			matches = new;
		}

		let output = matches.into_iter().flatten().collect::<Vec<_>>();
		Ok(output)
	}
}
