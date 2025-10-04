use {
	crate::Server,
	indoc::indoc,
	num::ToPrimitive as _,
	rusqlite as sqlite, tangram_client as tg,
	tangram_database::{self as db, prelude::*},
	tangram_either::Either,
};

#[derive(Clone, Debug)]
pub struct Match {
	pub id: u64,
	pub tag: tg::Tag,
	pub item: Option<Either<tg::process::Id, tg::object::Id>>,
}

impl Server {
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
				move |connection| {
					// Begin a transaction.
					let transaction = connection
						.transaction()
						.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;

					// List the tags.
					let output = Self::list_tags_sqlite_sync(&transaction, &arg)?;

					Ok::<_, tg::Error>(output)
				}
			})
			.await?;

		Ok(output)
	}

	fn list_tags_sqlite_sync(
		transaction: &sqlite::Transaction,
		arg: &tg::tag::list::Arg,
	) -> tg::Result<tg::tag::list::Output> {
		// Get all tags matching the pattern.
		let matches = Self::match_tags_sqlite_sync(transaction, &arg.pattern)?;

		// Expand.
		let mut expanded = Vec::new();
		for m in matches {
			if m.item.is_none() {
				// This is a branch tag, get its children.
				let statement = indoc!(
					"
						select id, component, item
						from tags
						where parent = ?1;
					"
				);
				let mut statement = transaction
					.prepare_cached(statement)
					.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
				let params = sqlite::params![m.id];
				let mut rows = statement
					.query(params)
					.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
				while let Some(row) = rows
					.next()
					.map_err(|source| tg::error!(!source, "failed to get the next row"))?
				{
					let id = row
						.get::<_, u64>(0)
						.map_err(|source| tg::error!(!source, "failed to get the id"))?;
					let component = row
						.get::<_, String>(1)
						.map_err(|source| tg::error!(!source, "failed to get the id"))?;
					let item = row
						.get::<_, Option<String>>(2)
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

	pub fn match_tags_sqlite_sync(
		transaction: &sqlite::Transaction,
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
							where parent = ?1;
						"
					);
					let mut statement = transaction
						.prepare_cached(statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![m.as_ref().map_or(0, |m| m.id)];
					let mut rows = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					while let Some(row) = rows
						.next()
						.map_err(|source| tg::error!(!source, "failed to get the next row"))?
					{
						let id = row
							.get::<_, u64>(0)
							.map_err(|source| tg::error!(!source, "failed to get the id"))?;
						let component = row
							.get::<_, String>(1)
							.map_err(|source| tg::error!(!source, "failed to get the id"))?;
						let item = row
							.get::<_, Option<String>>(2)
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
							where parent = ?1;
						"
					);
					let mut statement = transaction
						.prepare_cached(statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params = sqlite::params![m.as_ref().map_or(0, |m| m.id)];
					let mut rows = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					while let Some(row) = rows
						.next()
						.map_err(|source| tg::error!(!source, "failed to get the next row"))?
					{
						let id = row
							.get::<_, u64>(0)
							.map_err(|source| tg::error!(!source, "failed to get the id"))?;
						let component = row
							.get::<_, String>(1)
							.map_err(|source| tg::error!(!source, "failed to get the id"))?;
						let item = row
							.get::<_, Option<String>>(2)
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
							where parent = ?1 and component = ?2;
						"
					);
					let mut statement = transaction
						.prepare_cached(statement)
						.map_err(|source| tg::error!(!source, "failed to prepare the statement"))?;
					let params =
						sqlite::params![m.as_ref().map_or(0, |m| m.id), pattern.to_string()];
					let mut rows = statement
						.query(params)
						.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
					while let Some(row) = rows
						.next()
						.map_err(|source| tg::error!(!source, "failed to get the next row"))?
					{
						let id = row
							.get::<_, u64>(0)
							.map_err(|source| tg::error!(!source, "failed to get the id"))?;
						let item = row
							.get::<_, Option<String>>(1)
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
