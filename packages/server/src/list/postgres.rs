use {
	crate::{Session, context::Authentication},
	futures::FutureExt as _,
	indoc::indoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tracing::Instrument as _,
};

#[derive(Clone, Debug)]
pub(crate) struct Match {
	pub item: tg::Either<tg::object::Id, tg::process::Id>,
	pub tag: tg::Tag,
}

impl Session {
	#[tracing::instrument(level = "trace", skip_all)]
	pub(super) async fn list_postgres(
		&self,
		database: &db::postgres::Database,
		arg: tg::list::Arg,
	) -> tg::Result<tg::list::Output> {
		let mut connection = database
			.connection()
			.instrument(tracing::trace_span!("connection"))
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let transaction = connection
			.transaction()
			.instrument(tracing::trace_span!("begin_transaction"))
			.await
			.map_err(|error| tg::error!(!error, "failed to begin a transaction"))?;
		let authentication = self.context.authentication.clone();
		let mut data = Vec::new();
		if arg.recursive {
			if arg.namespaces {
				data.extend(
					self.list_namespace_entries_postgres(&transaction, &arg.pattern, true)
						.await?,
				);
			}
			if arg.tags {
				data.extend(
					self.list_tag_entries_postgres(&transaction, &arg.pattern, true)
						.await?,
				);
			}
			data = Self::filter_list_entries_by_visibility_postgres(
				&transaction,
				authentication.as_ref(),
				data,
			)
			.await?;
		} else {
			if arg.namespaces {
				data.extend(
					self.list_visible_namespace_entries_postgres(
						&transaction,
						authentication.as_ref(),
						&arg.pattern,
					)
					.await?,
				);
			}
			if arg.tags {
				data.extend(
					self.list_visible_tag_entries_postgres(
						&transaction,
						authentication.as_ref(),
						&arg.pattern,
					)
					.await?,
				);
			}
		}
		Ok(tg::list::Output { data })
	}

	pub(super) async fn list_cache_get_postgres(
		&self,
		database: &db::postgres::Database,
		arg: &str,
	) -> tg::Result<Option<(String, i64)>> {
		let connection = database
			.connection()
			.instrument(tracing::trace_span!("connection"))
			.await
			.map_err(|error| tg::error!(!error, "failed to get a database connection"))?;
		let statement = indoc!(
			"
				select output, timestamp
				from list_cache
				where arg = $1;
			"
		);
		let rows = connection
			.inner()
			.query(statement, &[&arg])
			.instrument(tracing::trace_span!("query_cache_get"))
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = rows.first() else {
			return Ok(None);
		};
		let output = row
			.try_get(0)
			.map_err(|error| tg::error!(!error, "failed to get the output column"))?;
		let timestamp = row
			.try_get(1)
			.map_err(|error| tg::error!(!error, "failed to get the timestamp column"))?;
		Ok(Some((output, timestamp)))
	}

	pub(super) async fn list_cache_put_postgres(
		&self,
		database: &db::postgres::Database,
		arg: &str,
		output: &str,
		timestamp: i64,
	) -> tg::Result<()> {
		let arg = arg.to_owned();
		let output = output.to_owned();
		database
			.run(|transaction| {
				let arg = arg.clone();
				let output = output.clone();
				async move {
					Self::list_cache_put_postgres_with_transaction(
						transaction,
						&arg,
						&output,
						timestamp,
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to put the list cache"))
	}

	async fn list_cache_put_postgres_with_transaction(
		transaction: &db::postgres::Transaction<'_>,
		arg: &str,
		output: &str,
		timestamp: i64,
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
		let statement = indoc!(
			"
				insert into list_cache (arg, output, timestamp)
				values ($1, $2, $3)
				on conflict (arg) do update
				set output = excluded.output, timestamp = excluded.timestamp;
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![arg.to_owned(), output.to_owned(), timestamp],
			)
			.instrument(tracing::trace_span!("query_cache_put"))
			.await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}

	async fn list_namespace_entries_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		pattern: &tg::specifier::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let namespaces = if recursive {
			Self::query_namespace_subtree_postgres(transaction, pattern).await?
		} else {
			Self::query_namespace_children_postgres(transaction, pattern).await?
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

	async fn list_visible_namespace_entries_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		authentication: Option<&Authentication>,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let Some(parent) =
			Self::try_get_namespace_postgres_with_transaction(transaction, &pattern.namespace)
				.await?
		else {
			return Ok(Vec::new());
		};
		if pattern.contains_operators() && pattern.name.as_str() != "*" {
			return Ok(Vec::new());
		}
		let rows = match authentication {
			Some(Authentication::Root) => {
				if pattern.name.as_str() == "*" || pattern.name.is_empty() {
					let statement = indoc!(
						"
							select name
							from namespaces
							where parent = $1;
						"
					);
					transaction
						.inner()
						.query(statement, &[&parent])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				} else {
					let statement = indoc!(
						"
							select name
							from namespaces
							where parent = $1 and component = $2;
						"
					);
					transaction
						.inner()
						.query(statement, &[&parent, &pattern.name.as_str()])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				}
			},
			None
			| Some(
				Authentication::Process(_) | Authentication::Runner | Authentication::Sandbox(_),
			) => {
				if pattern.name.as_str() == "*" || pattern.name.is_empty() {
					let statement = indoc!(
						r"
							select namespaces.name
							from namespaces
							where namespaces.parent = $1
								and exists (
									select 1
									from namespace_visibility
									where namespace_visibility.namespace = namespaces.id
										and namespace_visibility.principal = 'all'
								);
						"
					);
					transaction
						.inner()
						.query(statement, &[&parent])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				} else {
					let statement = indoc!(
						r"
							select namespaces.name
							from namespaces
							where namespaces.parent = $1
								and namespaces.component = $2
								and exists (
									select 1
									from namespace_visibility
									where namespace_visibility.namespace = namespaces.id
										and namespace_visibility.principal = 'all'
								);
						"
					);
					transaction
						.inner()
						.query(statement, &[&parent, &pattern.name.as_str()])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				}
			},
			Some(Authentication::User(user)) => {
				let user = user.id.to_string();
				if pattern.name.as_str() == "*" || pattern.name.is_empty() {
					let statement = indoc!(
						r#"
							with matching_principals(principal) as (
								values ($2::text), ('all'::text)
								union all
								select group_members."group"
								from group_members
								where group_members."user" = $2
							)
							select namespaces.name
							from namespaces
							where namespaces.parent = $1
								and exists (
									select 1
									from namespace_visibility
									where namespace_visibility.namespace = namespaces.id
										and exists (
											select 1
											from matching_principals
											where namespace_visibility.principal = matching_principals.principal
										)
								);
						"#
					);
					transaction
						.inner()
						.query(statement, &[&parent, &user])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				} else {
					let statement = indoc!(
						r#"
							with matching_principals(principal) as (
								values ($3::text), ('all'::text)
								union all
								select group_members."group"
								from group_members
								where group_members."user" = $3
							)
							select namespaces.name
							from namespaces
							where namespaces.parent = $1
								and namespaces.component = $2
								and exists (
									select 1
									from namespace_visibility
									where namespace_visibility.namespace = namespaces.id
										and exists (
											select 1
											from matching_principals
											where namespace_visibility.principal = matching_principals.principal
										)
								);
						"#
					);
					transaction
						.inner()
						.query(statement, &[&parent, &pattern.name.as_str(), &user])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				}
			},
		};
		rows.into_iter()
			.map(|row| {
				let name: String = row
					.try_get(0)
					.map_err(|error| tg::error!(!error, "failed to get the name column"))?;
				Ok(tg::list::Entry::Namespace {
					location: None,
					namespace: name.parse()?,
				})
			})
			.collect()
	}

	async fn query_namespace_children_postgres(
		transaction: &db::postgres::Transaction<'_>,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<Vec<tg::Namespace>> {
		let Some(parent) =
			Self::try_get_namespace_postgres_with_transaction(transaction, &pattern.namespace)
				.await?
		else {
			return Ok(Vec::new());
		};
		if pattern.contains_operators() && pattern.name.as_str() != "*" {
			return Ok(Vec::new());
		}
		let rows = if pattern.name.as_str() == "*" || pattern.name.is_empty() {
			let statement = indoc!(
				"
					select name
					from namespaces
					where parent = $1;
				"
			);
			async { transaction.inner().query(statement, &[&parent]).await }
				.instrument(tracing::trace_span!("query_namespace_children"))
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		} else {
			let statement = indoc!(
				"
					select name
					from namespaces
					where parent = $1 and component = $2;
				"
			);
			async {
				transaction
					.inner()
					.query(statement, &[&parent, &pattern.name.as_str()])
					.await
			}
			.instrument(tracing::trace_span!("query_namespace_child"))
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		};
		rows.into_iter()
			.map(|row| {
				let name: String = row
					.try_get(0)
					.map_err(|error| tg::error!(!error, "failed to get the name column"))?;
				name.parse()
			})
			.collect()
	}

	async fn query_namespace_subtree_postgres(
		transaction: &db::postgres::Transaction<'_>,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<Vec<tg::Namespace>> {
		if pattern.contains_operators() && pattern.name.as_str() != "*" {
			return Ok(Vec::new());
		}
		let namespace = if pattern.name.as_str() == "*" || pattern.name.is_empty() {
			pattern.namespace.clone()
		} else {
			pattern.to_namespace()
		};
		let rows = if namespace.is_root() {
			let statement = "select name from namespaces;";
			async { transaction.inner().query(statement, &[]).await }
				.instrument(tracing::trace_span!("query_namespaces"))
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
					where name = $1
						or (name >= $2 and name < $3);
				"
			);
			async {
				transaction
					.inner()
					.query(statement, &[&prefix, &lower, &upper])
					.await
			}
			.instrument(tracing::trace_span!("query_namespace_subtree"))
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		};
		rows.into_iter()
			.map(|row| {
				let name: String = row
					.try_get(0)
					.map_err(|error| tg::error!(!error, "failed to get the name column"))?;
				name.parse()
			})
			.collect()
	}

	async fn list_tag_entries_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		pattern: &tg::specifier::Pattern,
		recursive: bool,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let matches = if recursive {
			self.match_tags_for_list_postgres(transaction, pattern)
				.await?
		} else {
			self.list_tag_matches_for_list_postgres(transaction, pattern)
				.await?
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

	async fn list_visible_tag_entries_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		authentication: Option<&Authentication>,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<Vec<tg::list::Entry>> {
		let Some(namespace_id) =
			Self::try_get_namespace_postgres_with_transaction(transaction, &pattern.namespace)
				.await?
		else {
			return Ok(Vec::new());
		};
		let exact = (!pattern.is_empty() && !pattern.contains_operators())
			.then(|| pattern.name.as_str().to_owned());
		let ancestors = namespace_ancestor_names(&pattern.namespace);
		let rows = match authentication {
			Some(Authentication::Root) => {
				if let Some(name) = &exact {
					let statement = indoc!(
						"
							select name, item
							from tags
							where namespace = $1 and name = $2;
						"
					);
					transaction
						.inner()
						.query(statement, &[&namespace_id, name])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				} else {
					let statement = indoc!(
						"
							select name, item
							from tags
							where namespace = $1;
						"
					);
					transaction
						.inner()
						.query(statement, &[&namespace_id])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				}
			},
			None
			| Some(
				Authentication::Process(_) | Authentication::Runner | Authentication::Sandbox(_),
			) => {
				if let Some(name) = &exact {
					let statement = indoc!(
						r"
							with ancestor_ids(id) as (
								values (0::bigint)
								union
								select namespaces.id
								from namespaces
								where namespaces.name = any($3)
							),
							namespace_visible_by_permission(visible) as (
								select exists (
									select 1
									from namespace_grants
									where namespace_grants.namespace in (select id from ancestor_ids)
										and namespace_grants.principal = 'all'
										and namespace_grants.permission = 'read'
								)
							)
							select tags.name, tags.item
							from tags
							where tags.namespace = $1
								and tags.name = $2
								and (
									(select visible from namespace_visible_by_permission)
									or exists (
										select 1
										from tag_grants
										where tag_grants.namespace = tags.namespace
											and tag_grants.name = tags.name
											and tag_grants.principal = 'all'
											and tag_grants.permission = 'read'
									)
								);
						"
					);
					transaction
						.inner()
						.query(statement, &[&namespace_id, name, &ancestors])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				} else {
					let statement = indoc!(
						r"
							with ancestor_ids(id) as (
								values (0::bigint)
								union
								select namespaces.id
								from namespaces
								where namespaces.name = any($2)
							),
							namespace_visible_by_permission(visible) as (
								select exists (
									select 1
									from namespace_grants
									where namespace_grants.namespace in (select id from ancestor_ids)
										and namespace_grants.principal = 'all'
										and namespace_grants.permission = 'read'
								)
							)
							select tags.name, tags.item
							from tags
							where tags.namespace = $1
								and (
									(select visible from namespace_visible_by_permission)
									or exists (
										select 1
										from tag_grants
										where tag_grants.namespace = tags.namespace
											and tag_grants.name = tags.name
											and tag_grants.principal = 'all'
											and tag_grants.permission = 'read'
									)
								);
						"
					);
					transaction
						.inner()
						.query(statement, &[&namespace_id, &ancestors])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				}
			},
			Some(Authentication::User(user)) => {
				let user = user.id.to_string();
				if let Some(name) = &exact {
					let statement = indoc!(
						r#"
							with ancestor_ids(id) as (
								values (0::bigint)
								union
								select namespaces.id
								from namespaces
								where namespaces.name = any($4)
							),
							matching_principals(principal) as (
								values ($3::text), ('all'::text)
								union all
								select group_members."group"
								from group_members
								where group_members."user" = $3
							),
							namespace_visible_by_permission(visible) as (
								select exists (
									select 1
									from namespace_grants
									where namespace_grants.namespace in (select id from ancestor_ids)
											and exists (
												select 1
												from matching_principals
												where namespace_grants.principal = matching_principals.principal
											)
								)
							)
							select tags.name, tags.item
							from tags
							where tags.namespace = $1
								and tags.name = $2
								and (
									(select visible from namespace_visible_by_permission)
									or exists (
										select 1
										from tag_grants
										where tag_grants.namespace = tags.namespace
											and tag_grants.name = tags.name
												and exists (
													select 1
													from matching_principals
													where tag_grants.principal = matching_principals.principal
												)
									)
								);
						"#
					);
					transaction
						.inner()
						.query(statement, &[&namespace_id, name, &user, &ancestors])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				} else {
					let statement = indoc!(
						r#"
							with ancestor_ids(id) as (
								values (0::bigint)
								union
								select namespaces.id
								from namespaces
								where namespaces.name = any($3)
							),
							matching_principals(principal) as (
								values ($2::text), ('all'::text)
								union all
								select group_members."group"
								from group_members
								where group_members."user" = $2
							),
							namespace_visible_by_permission(visible) as (
								select exists (
									select 1
									from namespace_grants
									where namespace_grants.namespace in (select id from ancestor_ids)
											and exists (
												select 1
												from matching_principals
												where namespace_grants.principal = matching_principals.principal
											)
								)
							)
							select tags.name, tags.item
							from tags
							where tags.namespace = $1
								and (
									(select visible from namespace_visible_by_permission)
									or exists (
										select 1
										from tag_grants
										where tag_grants.namespace = tags.namespace
											and tag_grants.name = tags.name
												and exists (
													select 1
													from matching_principals
													where tag_grants.principal = matching_principals.principal
												)
									)
								);
						"#
					);
					transaction
						.inner()
						.query(statement, &[&namespace_id, &user, &ancestors])
						.await
						.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
				}
			},
		};
		let mut entries = Vec::new();
		for row in rows {
			#[derive(db::postgres::row::Deserialize)]
			struct Row {
				name: String,
				#[tangram_database(as = "db::postgres::value::TryFrom<String>")]
				item: tg::Either<tg::object::Id, tg::process::Id>,
			}
			let row = <Row as db::postgres::row::Deserialize>::deserialize(&row)
				.map_err(|error| tg::error!(!error, "failed to deserialize the row"))?;
			let tag =
				tg::Tag::with_namespace_and_name(pattern.namespace.clone(), row.name.parse()?);
			if pattern.matches(&tag) {
				entries.push(tg::list::Entry::Tag {
					item: row.item,
					location: None,
					tag,
				});
			}
		}
		Ok(entries)
	}

	pub(crate) async fn list_tag_matches_for_list_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<Vec<Match>> {
		if !pattern.is_empty() && !pattern.contains_operators() {
			return Ok(Self::get_tag_match_for_list_postgres(transaction, pattern)
				.await?
				.into_iter()
				.collect());
		}
		let mut matches =
			Self::query_tags_in_namespace_for_list_postgres(transaction, &pattern.namespace)
				.await?;
		matches.retain(|m| pattern.matches(&m.tag));
		Ok(matches)
	}

	pub(crate) async fn match_tags_for_list_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<Vec<Match>> {
		if pattern.is_empty() {
			return Self::query_tags_in_namespace_subtree_for_list_postgres(
				transaction,
				&tg::Namespace::root(),
			)
			.await;
		}
		let mut matches = Vec::new();
		let namespace = if pattern.contains_operators() {
			pattern.namespace.clone()
		} else {
			if let Some(m) = Self::get_tag_match_for_list_postgres(transaction, pattern).await? {
				matches.push(m);
			}
			pattern.to_namespace()
		};
		matches.extend(
			Self::query_tags_in_namespace_subtree_for_list_postgres(transaction, &namespace)
				.await?,
		);
		if pattern.contains_operators() {
			matches.retain(|m| pattern.matches_in_namespace_subtree(&m.tag));
		}
		Ok(matches)
	}

	async fn get_tag_match_for_list_postgres(
		transaction: &db::postgres::Transaction<'_>,
		pattern: &tg::specifier::Pattern,
	) -> tg::Result<Option<Match>> {
		let Some(namespace) =
			Self::try_get_namespace_postgres_with_transaction(transaction, &pattern.namespace)
				.await?
		else {
			return Ok(None);
		};
		let statement = indoc!(
			"
				select item
				from tags
				where namespace = $1 and name = $2;
			"
		);
		let rows = async {
			transaction
				.inner()
				.query(statement, &[&namespace, &pattern.name.as_str()])
				.await
		}
		.instrument(tracing::trace_span!("query_exact_tag"))
		.await
		.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let Some(row) = rows.first() else {
			return Ok(None);
		};
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::postgres::value::TryFrom<String>")]
			item: tg::Either<tg::object::Id, tg::process::Id>,
		}
		let row = <Row as db::postgres::row::Deserialize>::deserialize(row)
			.map_err(|error| tg::error!(!error, "failed to deserialize the row"))?;
		Ok(Some(Match {
			item: row.item,
			tag: tg::Tag::with_namespace_and_name(
				pattern.namespace.clone(),
				pattern.name.as_str().parse()?,
			),
		}))
	}

	async fn query_tags_in_namespace_for_list_postgres(
		transaction: &db::postgres::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<Match>> {
		let Some(namespace_id) =
			Self::try_get_namespace_postgres_with_transaction(transaction, namespace).await?
		else {
			return Ok(Vec::new());
		};
		let statement = indoc!(
			"
				select name, item
				from tags
				where namespace = $1;
			"
		);
		let rows = async { transaction.inner().query(statement, &[&namespace_id]).await }
			.instrument(tracing::trace_span!("query_tags"))
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let mut matches = Vec::new();
		for row in rows {
			#[derive(db::postgres::row::Deserialize)]
			struct Row {
				name: String,
				#[tangram_database(as = "db::postgres::value::TryFrom<String>")]
				item: tg::Either<tg::object::Id, tg::process::Id>,
			}
			let row = <Row as db::postgres::row::Deserialize>::deserialize(&row)
				.map_err(|error| tg::error!(!error, "failed to deserialize the row"))?;
			matches.push(Match {
				item: row.item,
				tag: tg::Tag::with_namespace_and_name(namespace.clone(), row.name.parse()?),
			});
		}
		Ok(matches)
	}

	async fn query_tags_in_namespace_subtree_for_list_postgres(
		transaction: &db::postgres::Transaction<'_>,
		namespace: &tg::Namespace,
	) -> tg::Result<Vec<Match>> {
		let rows = if namespace.is_root() {
			let statement = indoc!(
				"
					select coalesce(namespaces.name, '') as namespace, tags.name, tags.item
					from tags
					left join namespaces on tags.namespace = namespaces.id;
				"
			);
			async { transaction.inner().query(statement, &[]).await }
				.instrument(tracing::trace_span!("query_tags"))
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
					where namespaces.name = $1
						or (namespaces.name >= $2 and namespaces.name < $3);
				"
			);
			async {
				transaction
					.inner()
					.query(statement, &[&prefix, &lower, &upper])
					.await
			}
			.instrument(tracing::trace_span!("query_tags_in_namespace_subtree"))
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?
		};
		rows.into_iter()
			.map(|row| Self::deserialize_tag_match_for_list_postgres(&row))
			.collect()
	}

	fn deserialize_tag_match_for_list_postgres(row: &tokio_postgres::Row) -> tg::Result<Match> {
		#[derive(db::postgres::row::Deserialize)]
		struct Row {
			namespace: String,
			name: String,
			#[tangram_database(as = "db::postgres::value::TryFrom<String>")]
			item: tg::Either<tg::object::Id, tg::process::Id>,
		}
		let row = <Row as db::postgres::row::Deserialize>::deserialize(row)
			.map_err(|error| tg::error!(!error, "failed to deserialize the row"))?;
		Ok(Match {
			item: row.item,
			tag: tg::Tag::with_namespace_and_name(row.namespace.parse()?, row.name.parse()?),
		})
	}
}

fn namespace_ancestor_names(namespace: &tg::Namespace) -> Vec<String> {
	let mut names = Vec::new();
	let mut name = String::new();
	for component in namespace.components() {
		if !name.is_empty() {
			name.push('/');
		}
		name.push_str(component);
		names.push(name.clone());
	}
	names
}
