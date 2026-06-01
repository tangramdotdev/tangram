use {
	crate::Session,
	indoc::indoc,
	std::ops::ControlFlow,
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
	pub(super) async fn list_turso(
		&self,
		database: &db::turso::Database,
		arg: tg::list::Arg,
	) -> tg::Result<tg::list::Output> {
		let mut connection = database
			.connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let authentication = self.context.authentication.clone();
		let mut data = Vec::new();
		if arg.namespaces {
			data.extend(
				Self::list_namespace_entries_turso_with_transaction(
					&transaction,
					&arg.pattern,
					arg.recursive,
				)
				.await?,
			);
		}
		if arg.tags {
			data.extend(
				Self::list_tag_entries_turso_with_transaction(
					&transaction,
					&arg.pattern,
					arg.recursive,
				)
				.await?,
			);
		}
		data = Self::filter_list_entries_by_visibility_turso_with_transaction(
			&transaction,
			authentication.as_ref(),
			data,
		)
		.await?;
		Ok(tg::list::Output { data })
	}

	pub(super) async fn list_cache_get_turso(
		&self,
		database: &db::turso::Database,
		arg: &str,
	) -> tg::Result<Option<(String, i64)>> {
		let connection = database
			.write_connection()
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let statement = indoc!(
			"
				select output, timestamp
				from list_cache
				where arg = ?1;
			"
		);
		#[derive(db::row::Deserialize)]
		struct Row {
			output: String,
			timestamp: i64,
		}
		let row = connection
			.query_optional_into::<Row>(statement.into(), db::params![arg])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		Ok(row.map(|row| (row.output, row.timestamp)))
	}

	pub(super) async fn list_cache_put_turso(
		&self,
		database: &db::turso::Database,
		arg: &str,
		output: &str,
		timestamp: i64,
	) -> tg::Result<()> {
		let arg = arg.to_owned();
		let output = output.to_owned();
		db::turso::run!(database, |transaction| {
			let statement = indoc!(
				"
					insert into list_cache (arg, output, timestamp)
					values (?1, ?2, ?3)
					on conflict (arg) do update
					set output = excluded.output, timestamp = excluded.timestamp;
				"
			);
			let result = transaction
				.execute(statement.into(), db::params![arg, output, timestamp])
				.await;
			crate::database::retry!(result, "failed to execute the statement");
			Ok(ControlFlow::Break(()))
		})
		.map_err(|error| tg::error!(!error, "failed to put the list cache"))
	}

	async fn list_namespace_entries_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		pattern: &tg::list::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let namespaces = if recursive {
			Self::query_namespace_subtree_turso_with_transaction(transaction, pattern).await?
		} else {
			Self::query_namespace_children_turso_with_transaction(transaction, pattern).await?
		};
		Ok(namespaces
			.into_iter()
			.map(|namespace| tg::list::Entry::Namespace {
				location: None,
				namespace,
			})
			.collect())
	}

	async fn query_namespace_children_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		pattern: &tg::list::Pattern,
	) -> tg::Result<Vec<tg::Namespace>> {
		let Some(parent) =
			Self::try_get_namespace_turso_with_transaction(transaction, &pattern.namespace).await?
		else {
			return Ok(Vec::new());
		};
		if pattern.contains_operators() && pattern.name.as_str() != "*" {
			return Ok(Vec::new());
		}
		let statement = if pattern.name.as_str() == "*" || pattern.name.is_empty() {
			indoc!(
				"
					select name
					from namespaces
					where parent = ?1;
				"
			)
		} else {
			indoc!(
				"
					select name
					from namespaces
					where parent = ?1 and component = ?2;
				"
			)
		};
		let rows = if pattern.name.as_str() == "*" || pattern.name.is_empty() {
			transaction
				.query_all_value_into::<String>(statement.into(), db::params![parent])
				.await
		} else {
			transaction
				.query_all_value_into::<String>(
					statement.into(),
					db::params![parent, pattern.name.as_str()],
				)
				.await
		}
		.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		rows.into_iter().map(|name| name.parse()).collect()
	}

	async fn query_namespace_subtree_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
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
			transaction
				.query_all_value_into::<String>(
					"select name from namespaces;".into(),
					db::params![],
				)
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		} else {
			let prefix = namespace.to_string();
			let lower = format!("{prefix}/");
			let upper = format!("{prefix}0");
			let statement = indoc!(
				"
					select name
					from namespaces
					where name = ?1
						or (name >= ?2 and name < ?3);
				"
			);
			transaction
				.query_all_value_into::<String>(statement.into(), db::params![prefix, lower, upper])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		};
		names.into_iter().map(|name| name.parse()).collect()
	}

	async fn list_tag_entries_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		pattern: &tg::list::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let matches = if recursive {
			Self::match_tags_for_list_turso_with_transaction(transaction, pattern).await?
		} else {
			Self::list_tag_matches_for_list_turso_with_transaction(transaction, pattern).await?
		};
		Ok(matches
			.into_iter()
			.map(|m| tg::list::Entry::Tag {
				item: m.item,
				location: None,
				tag: m.tag,
			})
			.collect())
	}

	pub(crate) async fn list_tag_matches_for_list_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		pattern: &tg::list::Pattern,
	) -> tg::Result<Vec<Match>> {
		if !pattern.is_empty() && !pattern.contains_operators() {
			return Ok(
				Self::get_tag_match_for_list_turso_with_transaction(transaction, pattern)
					.await?
					.into_iter()
					.collect(),
			);
		}
		let mut matches = Self::query_tags_in_namespace_for_list_turso_with_transaction(
			transaction,
			&pattern.namespace,
		)
		.await?;
		matches.retain(|m| pattern.matches(&m.tag));
		Ok(matches)
	}

	pub(crate) async fn match_tags_for_list_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		pattern: &tg::list::Pattern,
	) -> tg::Result<Vec<Match>> {
		if pattern.is_empty() {
			return Self::query_tags_in_namespace_subtree_for_list_turso_with_transaction(
				transaction,
				&tg::Namespace::root(),
			)
			.await;
		}
		let mut matches = Vec::new();
		let namespace = if pattern.contains_operators() {
			pattern.namespace.clone()
		} else {
			if let Some(m) =
				Self::get_tag_match_for_list_turso_with_transaction(transaction, pattern).await?
			{
				matches.push(m);
			}
			pattern.to_namespace()
		};
		matches.extend(
			Self::query_tags_in_namespace_subtree_for_list_turso_with_transaction(
				transaction,
				&namespace,
			)
			.await?,
		);
		if pattern.contains_operators() {
			matches.retain(|m| pattern.matches_in_namespace_subtree(&m.tag));
		}
		Ok(matches)
	}

	async fn get_tag_match_for_list_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		pattern: &tg::list::Pattern,
	) -> tg::Result<Option<Match>> {
		let Some(namespace) =
			Self::try_get_namespace_turso_with_transaction(transaction, &pattern.namespace).await?
		else {
			return Ok(None);
		};
		let statement = indoc!(
			"
				select item
				from tags
				where namespace = ?1 and name = ?2;
			"
		);
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			item: tg::Either<tg::object::Id, tg::process::Id>,
		}
		let Some(row) = transaction
			.query_optional_into::<Row>(
				statement.into(),
				db::params![namespace, pattern.name.as_str()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		else {
			return Ok(None);
		};
		Ok(Some(Match {
			item: row.item,
			tag: tg::Tag::with_namespace_and_name(
				pattern.namespace.clone(),
				pattern.name.as_str().parse()?,
			),
		}))
	}

	async fn query_tags_in_namespace_for_list_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<Match>> {
		let Some(namespace_id) =
			Self::try_get_namespace_turso_with_transaction(transaction, namespace).await?
		else {
			return Ok(Vec::new());
		};
		let statement = indoc!(
			"
				select name, item
				from tags
				where namespace = ?1;
			"
		);
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
			#[tangram_database(as = "db::value::FromStr")]
			item: tg::Either<tg::object::Id, tg::process::Id>,
		}
		let rows = transaction
			.query_all_into::<Row>(statement.into(), db::params![namespace_id])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		rows.into_iter()
			.map(|row| {
				Ok(Match {
					item: row.item,
					tag: tg::Tag::with_namespace_and_name(namespace.clone(), row.name.parse()?),
				})
			})
			.collect::<tg::Result<Vec<_>>>()
	}

	async fn query_tags_in_namespace_subtree_for_list_turso_with_transaction(
		transaction: &db::turso::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<Match>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			namespace: String,
			name: String,
			#[tangram_database(as = "db::value::FromStr")]
			item: tg::Either<tg::object::Id, tg::process::Id>,
		}
		let rows = if namespace.is_root() {
			let statement = indoc!(
				"
					select coalesce(namespaces.name, '') as namespace, tags.name, tags.item
					from tags
					left join namespaces on tags.namespace = namespaces.id;
				"
			);
			transaction
				.query_all_into::<Row>(statement.into(), db::params![])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
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
						or (namespaces.name >= ?2 and namespaces.name < ?3);
				"
			);
			transaction
				.query_all_into::<Row>(statement.into(), db::params![prefix, lower, upper])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		};
		rows.into_iter()
			.map(|row| {
				Ok(Match {
					item: row.item,
					tag: tg::Tag::with_namespace_and_name(
						row.namespace.parse()?,
						row.name.parse()?,
					),
				})
			})
			.collect()
	}
}
