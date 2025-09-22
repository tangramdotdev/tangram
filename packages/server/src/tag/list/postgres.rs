use crate::Server;
use indoc::indoc;
use num::ToPrimitive as _;
use tangram_client as tg;
use tangram_database::prelude::*;
use tangram_either::Either;

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

		#[derive(Clone, Debug)]
		struct Match {
			id: u64,
			tag: tg::Tag,
			item: Option<Either<tg::process::Id, tg::object::Id>>,
		}
		let mut matches: Vec<Option<Match>> = vec![None];
		for component in arg.pattern.components() {
			let mut new = Vec::new();
			for m in matches {
				match component {
					tg::tag::pattern::Component::Normal(component) => {
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
									&component.to_string(),
								],
							)
							.await
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
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
								.map_err(|source| {
									tg::error!(!source, "failed to parse the item")
								})?;
							let mut components = m
								.as_ref()
								.map_or_else(Vec::new, |m| m.tag.components().clone());
							components.push(component.clone());
							let tag = tg::Tag::with_components(components);
							let m = Match { id, tag, item };
							new.push(Some(m));
						}
					},
					tg::tag::pattern::Component::Version(pattern) => {
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
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
						for row in rows {
							let id = row
								.try_get::<_, i64>(0)
								.map_err(|source| tg::error!(!source, "failed to get the id"))?
								.to_u64()
								.unwrap();
							let component = row
								.try_get::<_, String>(1)
								.map_err(|source| tg::error!(!source, "failed to get the id"))?
								.parse()
								.map_err(|source| {
									tg::error!(!source, "failed to parse the component")
								})?;
							let item = row
								.try_get::<_, Option<String>>(2)
								.map_err(|source| tg::error!(!source, "failed to get the item"))?
								.map(|s| s.parse())
								.transpose()
								.map_err(|source| {
									tg::error!(!source, "failed to parse the item")
								})?;
							if let tg::tag::Component::Version(version) = &component
								&& pattern.matches(version)
							{
								let mut components = m
									.as_ref()
									.map_or_else(Vec::new, |m| m.tag.components().clone());
								components.push(component);
								let tag = tg::Tag::with_components(components);
								let m = Match { id, tag, item };
								new.push(Some(m));
							}
						}
					},
					tg::tag::pattern::Component::Wildcard => {
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
							.map_err(|source| {
								tg::error!(!source, "failed to execute the statement")
							})?;
						for row in rows {
							let id = row
								.try_get::<_, i64>(0)
								.map_err(|source| tg::error!(!source, "failed to get the id"))?
								.to_u64()
								.unwrap();
							let component = row
								.try_get::<_, String>(1)
								.map_err(|source| tg::error!(!source, "failed to get the id"))?
								.parse()
								.map_err(|source| {
									tg::error!(!source, "failed to parse the component")
								})?;
							let item = row
								.try_get::<_, Option<String>>(2)
								.map_err(|source| tg::error!(!source, "failed to get the item"))?
								.map(|s| s.parse())
								.transpose()
								.map_err(|source| {
									tg::error!(!source, "failed to parse the item")
								})?;
							let mut components = m
								.as_ref()
								.map_or_else(Vec::new, |m| m.tag.components().clone());
							components.push(component);
							let tag = tg::Tag::with_components(components);
							let m = Match { id, tag, item };
							new.push(Some(m));
						}
					},
				}
			}
			matches = new;
		}

		if matches.len() == 1
			&& let Some(m) = matches.first().cloned().unwrap()
			&& m.item.is_none()
		{
			let mut new = Vec::new();
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
					.map_err(|source| tg::error!(!source, "failed to get the id"))?
					.parse()
					.map_err(|source| tg::error!(!source, "failed to parse the component"))?;
				let item = row
					.try_get::<_, Option<String>>(2)
					.map_err(|source| tg::error!(!source, "failed to get the item"))?
					.map(|s| s.parse())
					.transpose()
					.map_err(|source| tg::error!(!source, "failed to parse the item"))?;
				let mut components = m.tag.components().clone();
				components.push(component);
				let tag = tg::Tag::with_components(components);
				let m = Match { id, tag, item };
				new.push(Some(m));
			}
			matches = new;
		}

		let mut output = matches.into_iter().flatten().collect::<Vec<_>>();

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
}
