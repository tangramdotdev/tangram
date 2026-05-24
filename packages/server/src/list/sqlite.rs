use {
	crate::{Session, context::Authentication},
	indoc::indoc,
	rusqlite as sqlite,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

#[derive(Clone, Debug)]
pub(crate) struct Match {
	pub item: tg::Either<tg::object::Id, tg::process::Id>,
	pub tag: tg::Tag,
}

impl Session {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn list_sqlite(
		&self,
		database: &db::sqlite::Database,
		arg: tg::list::Arg,
	) -> tg::Result<tg::list::Output> {
		let connection = database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let authentication = self.context.authentication.clone();
		connection
			.with(move |connection, cache| {
				let transaction = connection
					.transaction()
					.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
				Self::list_sqlite_sync(&transaction, cache, authentication.as_ref(), &arg)
			})
			.await
	}

	fn list_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		authentication: Option<&Authentication>,
		arg: &tg::list::Arg,
	) -> tg::Result<tg::list::Output> {
		let mut data = Vec::new();
		if arg.namespaces {
			data.extend(Self::list_namespace_entries_sqlite_sync(
				transaction,
				cache,
				&arg.pattern,
				arg.recursive,
			)?);
		}
		if arg.tags {
			data.extend(Self::list_tag_entries_sqlite_sync(
				transaction,
				cache,
				&arg.pattern,
				arg.recursive,
			)?);
		}
		data =
			Self::filter_list_entries_by_visibility_sqlite_sync(transaction, authentication, data)?;
		Ok(tg::list::Output { data })
	}

	pub(super) async fn list_cache_get_sqlite(
		&self,
		database: &db::sqlite::Database,
		arg: &str,
	) -> tg::Result<Option<(String, i64)>> {
		let connection = database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let arg = arg.to_owned();
		connection
			.with(move |connection, cache| {
				let statement = indoc!(
					"
						select output, timestamp
						from list_cache
						where arg = ?1 ;
					"
				);
				let mut statement = cache
					.get(connection, statement.into())
					.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
				let mut rows = statement
					.query(sqlite::params![arg])
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				let Some(row) = rows
					.next()
					.map_err(|error| tg::error!(!error, "failed to get the next row"))?
				else {
					return Ok(None);
				};
				let output = row
					.get(0)
					.map_err(|error| tg::error!(!error, "failed to get the output column"))?;
				let timestamp = row
					.get(1)
					.map_err(|error| tg::error!(!error, "failed to get the timestamp column"))?;
				Ok(Some((output, timestamp)))
			})
			.await
	}

	pub(super) async fn list_cache_put_sqlite(
		&self,
		database: &db::sqlite::Database,
		arg: &str,
		output: &str,
		timestamp: i64,
	) -> tg::Result<()> {
		let connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let arg = arg.to_owned();
		let output = output.to_owned();
		connection
			.with(move |connection, cache| {
				let statement = indoc!(
					"
						insert into list_cache (arg, output, timestamp)
						values (?1, ?2, ?3)
						on conflict (arg) do update
						set output = excluded.output, timestamp = excluded.timestamp ;
					"
				);
				let mut statement = cache
					.get(connection, statement.into())
					.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
				statement
					.execute(sqlite::params![arg, output, timestamp])
					.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
				Ok(())
			})
			.await
	}

	fn list_namespace_entries_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::list::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let namespaces = if recursive {
			Self::query_namespace_subtree_sqlite_sync(transaction, cache, pattern)?
		} else {
			Self::query_namespace_children_sqlite_sync(transaction, cache, pattern)?
		};
		let entries = namespaces
			.into_iter()
			.map(|namespace| tg::list::Entry::Namespace {
				location: None,
				namespace,
			})
			.collect();
		Ok(entries)
	}

	fn query_namespace_children_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::list::Pattern,
	) -> tg::Result<Vec<tg::Namespace>> {
		let Some(parent) = Self::get_namespace_sqlite_sync(transaction, cache, &pattern.namespace)?
		else {
			return Ok(Vec::new());
		};
		if pattern.contains_operators() && pattern.name.as_str() != "*" {
			return Ok(Vec::new());
		}
		let mut namespaces = Vec::new();
		if pattern.name.as_str() == "*" || pattern.name.is_empty() {
			let statement = indoc!(
				"
					select name
					from namespaces
					where parent = ?1 ;
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
			let mut rows = statement
				.query(sqlite::params![parent])
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			while let Some(row) = rows
				.next()
				.map_err(|error| tg::error!(!error, "failed to get the next row"))?
			{
				let name: String = row
					.get(0)
					.map_err(|error| tg::error!(!error, "failed to get the name column"))?;
				namespaces.push(name.parse()?);
			}
		} else {
			let statement = indoc!(
				"
					select name
					from namespaces
					where parent = ?1 and component = ?2 ;
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
			let mut rows = statement
				.query(sqlite::params![parent, pattern.name.as_str()])
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			while let Some(row) = rows
				.next()
				.map_err(|error| tg::error!(!error, "failed to get the next row"))?
			{
				let name: String = row
					.get(0)
					.map_err(|error| tg::error!(!error, "failed to get the name column"))?;
				namespaces.push(name.parse()?);
			}
		}
		Ok(namespaces)
	}

	fn query_namespace_subtree_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::list::Pattern,
	) -> tg::Result<Vec<tg::Namespace>> {
		if pattern.contains_operators() && pattern.name.as_str() != "*" {
			return Ok(Vec::new());
		}
		let namespace = if pattern.name.as_str() == "*" || pattern.name.is_empty() {
			pattern.namespace.clone()
		} else {
			pattern.to_namespace()
		};
		let names = if namespace.is_root() {
			let statement = "select name from namespaces ;";
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
			let mut rows = statement
				.query(())
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			let mut names = Vec::new();
			while let Some(row) = rows
				.next()
				.map_err(|error| tg::error!(!error, "failed to get the next row"))?
			{
				names.push(
					row.get::<_, String>(0)
						.map_err(|error| tg::error!(!error, "failed to get the name column"))?,
				);
			}
			names
		} else {
			let prefix = namespace.to_string();
			let lower = format!("{prefix}/");
			let upper = format!("{prefix}0");
			let statement = indoc!(
				"
					select name
					from namespaces
					where name = ?1
						or (name >= ?2 and name < ?3) ;
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
			let mut rows = statement
				.query(sqlite::params![prefix, lower, upper])
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			let mut names = Vec::new();
			while let Some(row) = rows
				.next()
				.map_err(|error| tg::error!(!error, "failed to get the next row"))?
			{
				names.push(
					row.get::<_, String>(0)
						.map_err(|error| tg::error!(!error, "failed to get the name column"))?,
				);
			}
			names
		};
		names.into_iter().map(|name| name.parse()).collect()
	}

	fn list_tag_entries_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::list::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let matches = if recursive {
			Self::match_tags_for_list_sqlite_sync(transaction, cache, pattern)?
		} else {
			Self::list_tag_matches_for_list_sqlite_sync(transaction, cache, pattern)?
		};
		let entries = matches
			.into_iter()
			.map(|m| tg::list::Entry::Tag {
				item: m.item,
				location: None,
				tag: m.tag,
			})
			.collect();
		Ok(entries)
	}

	pub(crate) fn list_tag_matches_for_list_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::list::Pattern,
	) -> tg::Result<Vec<Match>> {
		if !pattern.is_empty() && !pattern.contains_operators() {
			return Ok(
				Self::get_tag_match_for_list_sqlite_sync(transaction, cache, pattern)?
					.into_iter()
					.collect(),
			);
		}
		let mut matches = Self::query_tags_in_namespace_for_list_sqlite_sync(
			transaction,
			cache,
			&pattern.namespace,
		)?;
		matches.retain(|m| pattern.matches(&m.tag));
		Ok(matches)
	}

	pub(crate) fn match_tags_for_list_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::list::Pattern,
	) -> tg::Result<Vec<Match>> {
		if pattern.is_empty() {
			return Self::query_tags_in_namespace_subtree_for_list_sqlite_sync(
				transaction,
				cache,
				&tg::Namespace::root(),
			);
		}
		let mut matches = Vec::new();
		let namespace = if pattern.contains_operators() {
			pattern.namespace.clone()
		} else {
			if let Some(m) = Self::get_tag_match_for_list_sqlite_sync(transaction, cache, pattern)?
			{
				matches.push(m);
			}
			pattern.to_namespace()
		};
		matches.extend(Self::query_tags_in_namespace_subtree_for_list_sqlite_sync(
			transaction,
			cache,
			&namespace,
		)?);
		if pattern.contains_operators() {
			matches.retain(|m| pattern.matches_in_namespace_subtree(&m.tag));
		}
		Ok(matches)
	}

	fn get_tag_match_for_list_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		pattern: &tg::list::Pattern,
	) -> tg::Result<Option<Match>> {
		let Some(namespace) =
			Self::get_namespace_sqlite_sync(transaction, cache, &pattern.namespace)?
		else {
			return Ok(None);
		};
		let statement = indoc!(
			"
				select item
				from tags
				where namespace = ?1 and name = ?2 ;
			"
		);
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params![namespace, pattern.name.as_str()])
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = rows
			.next()
			.map_err(|error| tg::error!(!error, "failed to get the next row"))?
		else {
			return Ok(None);
		};
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::sqlite::value::TryFrom<String>")]
			item: tg::Either<tg::object::Id, tg::process::Id>,
		}
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|error| tg::error!(!error, "failed to deserialize the row"))?;
		Ok(Some(Match {
			item: row.item,
			tag: tg::Tag::with_namespace_and_name(
				pattern.namespace.clone(),
				pattern.name.as_str().parse()?,
			),
		}))
	}

	fn query_tags_in_namespace_for_list_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<Match>> {
		let Some(namespace_id) = Self::get_namespace_sqlite_sync(transaction, cache, namespace)?
		else {
			return Ok(Vec::new());
		};
		let statement = indoc!(
			"
				select name, item
				from tags
				where namespace = ?1 ;
			"
		);
		let mut statement = cache
			.get(transaction, statement.into())
			.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
		let mut rows = statement
			.query(sqlite::params![namespace_id])
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut matches = Vec::new();
		while let Some(row) = rows
			.next()
			.map_err(|error| tg::error!(!error, "failed to get the next row"))?
		{
			#[derive(db::sqlite::row::Deserialize)]
			struct Row {
				name: String,
				#[tangram_database(as = "db::sqlite::value::TryFrom<String>")]
				item: tg::Either<tg::object::Id, tg::process::Id>,
			}
			let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
				.map_err(|error| tg::error!(!error, "failed to deserialize the row"))?;
			matches.push(Match {
				item: row.item,
				tag: tg::Tag::with_namespace_and_name(namespace.clone(), row.name.parse()?),
			});
		}
		Ok(matches)
	}

	fn query_tags_in_namespace_subtree_for_list_sqlite_sync(
		transaction: &sqlite::Transaction,
		cache: &db::sqlite::Cache,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<Match>> {
		let mut matches = Vec::new();
		if namespace.is_root() {
			let statement = indoc!(
				"
					select coalesce(namespaces.name, '') as namespace, tags.name, tags.item
					from tags
					left join namespaces on tags.namespace = namespaces.id ;
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
			let mut rows = statement
				.query(())
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			while let Some(row) = rows
				.next()
				.map_err(|error| tg::error!(!error, "failed to get the next row"))?
			{
				matches.push(Self::deserialize_tag_match_for_list_sqlite(row)?);
			}
		} else {
			let prefix = namespace.to_string();
			let lower = format!("{prefix}/");
			let upper = format!("{prefix}0");
			let statement = indoc!(
				"
					select namespaces.name as namespace, tags.name, tags.item
					from tags
					join namespaces on tags.namespace = namespaces.id
					where namespaces.name = ?1
						or (namespaces.name >= ?2 and namespaces.name < ?3) ;
				"
			);
			let mut statement = cache
				.get(transaction, statement.into())
				.map_err(|error| tg::error!(!error, "failed to prepare the statement"))?;
			let mut rows = statement
				.query(sqlite::params![prefix, lower, upper])
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			while let Some(row) = rows
				.next()
				.map_err(|error| tg::error!(!error, "failed to get the next row"))?
			{
				matches.push(Self::deserialize_tag_match_for_list_sqlite(row)?);
			}
		}
		Ok(matches)
	}

	fn deserialize_tag_match_for_list_sqlite(row: &sqlite::Row<'_>) -> tg::Result<Match> {
		#[derive(db::sqlite::row::Deserialize)]
		struct Row {
			namespace: String,
			name: String,
			#[tangram_database(as = "db::sqlite::value::TryFrom<String>")]
			item: tg::Either<tg::object::Id, tg::process::Id>,
		}
		let row = <Row as db::sqlite::row::Deserialize>::deserialize(row)
			.map_err(|error| tg::error!(!error, "failed to deserialize the row"))?;
		Ok(Match {
			item: row.item,
			tag: tg::Tag::with_namespace_and_name(row.namespace.parse()?, row.name.parse()?),
		})
	}
}
