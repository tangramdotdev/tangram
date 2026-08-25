use {
	crate::{Session, database::Transaction},
	futures::FutureExt as _,
	indoc::formatdoc,
	std::{
		collections::{BTreeMap, BTreeSet},
		ops::ControlFlow,
	},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn try_resolve_named_node(
		&self,
		selector: &tg::Selector<tg::Id>,
	) -> tg::Result<Option<(tg::Id, tg::Specifier)>> {
		let selector = selector.clone();
		let output = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let selector = selector.clone();
				async move {
					Self::try_resolve_named_node_with_transaction(transaction, &selector).await
				}
				.boxed()
			})
			.await?;

		Ok(output)
	}

	async fn try_resolve_named_node_with_transaction(
		transaction: &Transaction<'_>,
		selector: &tg::Selector<tg::Id>,
	) -> tg::Result<ControlFlow<Option<(tg::Id, tg::Specifier)>, crate::database::Error>> {
		let output = match selector {
			tg::Selector::Id(id) => {
				let specifier =
					match Self::try_get_specifier_for_id_with_transaction(transaction, id).await? {
						ControlFlow::Break(specifier) => specifier,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
				specifier.map(|specifier| (id.clone(), specifier))
			},
			tg::Selector::Specifier(specifier) => {
				let id =
					match Self::try_get_id_for_specifier_with_transaction(transaction, specifier)
						.await?
					{
						ControlFlow::Break(id) => id,
						ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
					};
				id.map(|id| (id, specifier.clone()))
			},
		};

		Ok(ControlFlow::Break(output))
	}

	pub(crate) async fn try_get_ids_and_ancestors_for_specifiers(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<BTreeMap<tg::Specifier, Option<tg::Id>>> {
		let specifiers = specifiers
			.iter()
			.flat_map(|specifier| std::iter::once(specifier.clone()).chain(specifier.ancestors()))
			.collect::<BTreeSet<_>>()
			.into_iter()
			.collect::<Vec<_>>();
		let ids_by_specifier = self.try_get_ids_for_specifiers(&specifiers).await?;

		Ok(ids_by_specifier)
	}

	pub(crate) async fn try_get_ids_for_specifiers(
		&self,
		specifiers: &[tg::Specifier],
	) -> tg::Result<BTreeMap<tg::Specifier, Option<tg::Id>>> {
		let batch_size = self.server.config.sync.get.database.batch_size;
		let ids_by_specifier = self
			.server
			.database
			.run_with_options(db::ConnectionOptions::default(), |transaction| {
				let specifiers = specifiers.to_vec();
				async move {
					Self::try_get_ids_for_specifiers_with_transaction(
						transaction,
						&specifiers,
						batch_size,
					)
					.await
				}
				.boxed()
			})
			.await?;

		Ok(ids_by_specifier)
	}

	pub(crate) async fn try_get_ids_for_specifiers_with_transaction(
		transaction: &Transaction<'_>,
		specifiers: &[tg::Specifier],
		batch_size: usize,
	) -> tg::Result<ControlFlow<BTreeMap<tg::Specifier, Option<tg::Id>>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}

		let mut ids_by_specifier = specifiers
			.iter()
			.cloned()
			.map(|specifier| (specifier, None))
			.collect::<BTreeMap<_, _>>();
		for specifiers in specifiers.chunks(batch_size) {
			let p = transaction.p();
			let placeholders = (1..=specifiers.len())
				.map(|index| format!("{p}{index}"))
				.collect::<Vec<_>>()
				.join(", ");
			let statement = format!(
				"select id, specifier from specifiers where specifier in ({placeholders});"
			);
			let params = specifiers
				.iter()
				.map(ToString::to_string)
				.map(db::Value::from)
				.collect();
			let result = transaction
				.query_all_into::<Row>(statement.into(), params)
				.await;
			let rows = crate::database::retry!(result, "failed to read the named node ids");
			for row in rows {
				ids_by_specifier.insert(row.specifier, Some(row.id));
			}
		}

		Ok(ControlFlow::Break(ids_by_specifier))
	}

	pub(crate) async fn verify_ids_for_specifiers_with_transaction(
		transaction: &Transaction<'_>,
		ids_by_specifier: &BTreeMap<tg::Specifier, Option<tg::Id>>,
		batch_size: usize,
	) -> tg::Result<ControlFlow<bool, crate::database::Error>> {
		let specifiers = ids_by_specifier.keys().cloned().collect::<Vec<_>>();
		let actual = match Self::try_get_ids_for_specifiers_with_transaction(
			transaction,
			&specifiers,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(actual) => actual,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		};
		let matches = &actual == ids_by_specifier;

		Ok(ControlFlow::Break(matches))
	}

	pub(crate) fn delete_permission_for_named_node(
		id: &tg::Id,
	) -> tg::Result<tg::authorization::Permission> {
		match id.kind() {
			tg::id::Kind::Group => Ok(tg::authorization::Permission::Group(
				tg::authorization::permission::group::Permission::Admin,
			)),
			tg::id::Kind::Organization => Ok(tg::authorization::Permission::Organization(
				tg::authorization::permission::organization::Permission::Admin,
			)),
			tg::id::Kind::Tag => Ok(tg::authorization::Permission::Tag(
				tg::authorization::permission::tag::Permission::Write,
			)),
			tg::id::Kind::User => Ok(tg::authorization::Permission::User(
				tg::authorization::permission::user::Permission::Admin,
			)),
			_ => Err(tg::error!("invalid named node kind")),
		}
	}

	pub(crate) async fn collect_named_subtrees_with_transaction(
		transaction: &Transaction<'_>,
		roots: &[tg::Id],
		batch_size: usize,
	) -> tg::Result<ControlFlow<Vec<(tg::Id, tg::Specifier)>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}

		let mut ids_and_specifiers = BTreeMap::new();
		for roots in roots.chunks(batch_size) {
			let p = transaction.p();
			let placeholders = (1..=roots.len())
				.map(|index| format!("{p}{index}"))
				.collect::<Vec<_>>()
				.join(", ");
			let statement = formatdoc!(
				"
					with recursive subtree (id) as (
						select id
						from specifiers
						where id in ({placeholders})
						union
						select children.id
						from (
							select id, parent from groups
							union all
							select id, parent from tags
						) as children
						join subtree on children.parent = subtree.id
					)
					select specifiers.id, specifiers.specifier
					from subtree
					join specifiers on specifiers.id = subtree.id;
				"
			);
			let params = roots
				.iter()
				.map(ToString::to_string)
				.map(db::Value::from)
				.collect();
			let result = transaction
				.query_all_into::<Row>(statement.into(), params)
				.await;
			let rows = crate::database::retry!(result, "failed to collect the named subtrees");
			for row in rows {
				ids_and_specifiers.insert(row.id, row.specifier);
			}
		}
		let mut ids_and_specifiers = ids_and_specifiers.into_iter().collect::<Vec<_>>();
		ids_and_specifiers.sort_by(|(_, a), (_, b)| {
			b.components()
				.count()
				.cmp(&a.components().count())
				.then_with(|| a.cmp(b))
		});

		Ok(ControlFlow::Break(ids_and_specifiers))
	}

	pub(crate) async fn delete_named_nodes_with_transaction(
		&self,
		transaction: &Transaction<'_>,
		ids_and_specifiers: &[(tg::Id, tg::Specifier)],
		batch: &mut tangram_index::batch::Arg,
		batch_size: usize,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		if ids_and_specifiers.is_empty() {
			return Ok(ControlFlow::Break(()));
		}

		#[derive(db::row::Deserialize)]
		struct GroupMemberRow {
			#[tangram_database(as = "db::value::FromStr")]
			group: tg::group::Id,
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::group::Member,
		}

		#[derive(db::row::Deserialize)]
		struct OrganizationMemberRow {
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::organization::Member,
			#[tangram_database(as = "db::value::FromStr")]
			organization: tg::organization::Id,
		}

		// Delete the memberships and grants that reference the nodes.
		let ids = ids_and_specifiers
			.iter()
			.map(|(id, _)| id.clone())
			.collect::<Vec<_>>();
		for ids in ids.chunks(batch_size) {
			let p = transaction.p();
			let placeholders = (1..=ids.len())
				.map(|index| format!("{p}{index}"))
				.collect::<Vec<_>>()
				.join(", ");
			let params = ids
				.iter()
				.map(ToString::to_string)
				.map(db::Value::from)
				.collect::<Vec<_>>();

			let statement = format!(
				r#"select "group", member from group_members where "group" in ({placeholders}) or member in ({placeholders});"#
			);
			let result = transaction
				.query_all_into::<GroupMemberRow>(statement.into(), params.clone())
				.await;
			let rows = crate::database::retry!(result, "failed to list the group members");
			for row in rows {
				batch
					.items
					.push(tangram_index::batch::Item::DeleteGroupMember(
						tangram_index::group::member::delete::Arg {
							group: row.group,
							member: row.member,
						},
					));
			}

			let statement = format!(
				"select organization, member from organization_members where organization in ({placeholders}) or member in ({placeholders});"
			);
			let result = transaction
				.query_all_into::<OrganizationMemberRow>(statement.into(), params.clone())
				.await;
			let rows = crate::database::retry!(result, "failed to list the organization members");
			for row in rows {
				batch
					.items
					.push(tangram_index::batch::Item::DeleteOrganizationMember(
						tangram_index::organization::member::delete::Arg {
							member: row.member,
							organization: row.organization,
						},
					));
			}

			match self
				.delete_node_grants_batch_with_transaction(transaction, ids, batch)
				.await?
			{
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}

			for statement in [
				format!(
					r#"delete from group_members where "group" in ({placeholders}) or member in ({placeholders});"#
				),
				format!(
					"delete from organization_members where organization in ({placeholders}) or member in ({placeholders});"
				),
			] {
				let result = transaction.execute(statement.into(), params.clone()).await;
				crate::database::retry!(result, "failed to delete the named node memberships");
			}

			let statement =
				format!("update runners set owner = null where owner in ({placeholders});");
			let result = transaction.execute(statement.into(), params.clone()).await;
			crate::database::retry!(result, "failed to clear the named node runners");
		}

		// Delete the data that references users.
		let users = ids
			.iter()
			.filter(|id| id.kind() == tg::id::Kind::User)
			.cloned()
			.collect::<Vec<_>>();
		for users in users.chunks(batch_size) {
			let p = transaction.p();
			let placeholders = (1..=users.len())
				.map(|index| format!("{p}{index}"))
				.collect::<Vec<_>>()
				.join(", ");
			let params = users
				.iter()
				.map(ToString::to_string)
				.map(db::Value::from)
				.collect::<Vec<_>>();
			for statement in [
				format!(r#"delete from github_identities where "user" in ({placeholders});"#),
				format!(r#"delete from user_emails where "user" in ({placeholders});"#),
				format!(r#"delete from user_identities where "user" in ({placeholders});"#),
				format!(r#"delete from user_tokens where "user" in ({placeholders});"#),
				format!(r#"update logins set "user" = null where "user" in ({placeholders});"#),
			] {
				let result = transaction.execute(statement.into(), params.clone()).await;
				crate::database::retry!(result, "failed to delete the named node user data");
			}
		}

		// Create the index deletions in descendant-first order.
		for (id, _) in ids_and_specifiers {
			let item = match id {
				id if id.kind() == tg::id::Kind::Group => {
					tangram_index::batch::Item::DeleteGroup(id.clone().try_into()?)
				},
				id if id.kind() == tg::id::Kind::Organization => {
					tangram_index::batch::Item::DeleteOrganization(id.clone().try_into()?)
				},
				id if id.kind() == tg::id::Kind::Tag => {
					tangram_index::batch::Item::DeleteTag(id.clone().try_into()?)
				},
				id if id.kind() == tg::id::Kind::User => {
					tangram_index::batch::Item::DeleteUser(id.clone().try_into()?)
				},
				_ => return Err(tg::error!("invalid named node kind")),
			};
			batch.items.push(item);
		}

		// Delete the nodes and their specifier entries.
		for (kind, table) in [
			(tg::id::Kind::Group, "groups"),
			(tg::id::Kind::Organization, "organizations"),
			(tg::id::Kind::Tag, "tags"),
			(tg::id::Kind::User, "users"),
		] {
			let ids = ids
				.iter()
				.filter(|id| id.kind() == kind)
				.cloned()
				.collect::<Vec<_>>();
			match Self::delete_named_node_rows_with_transaction(
				transaction,
				table,
				&ids,
				batch_size,
			)
			.await?
			{
				ControlFlow::Break(()) => (),
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			}
		}
		match Self::delete_named_node_rows_with_transaction(
			transaction,
			"specifiers",
			&ids,
			batch_size,
		)
		.await?
		{
			ControlFlow::Break(()) => (),
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}

		Ok(ControlFlow::Break(()))
	}

	async fn delete_named_node_rows_with_transaction(
		transaction: &Transaction<'_>,
		table: &str,
		ids: &[tg::Id],
		batch_size: usize,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		for ids in ids.chunks(batch_size) {
			let p = transaction.p();
			let placeholders = (1..=ids.len())
				.map(|index| format!("{p}{index}"))
				.collect::<Vec<_>>()
				.join(", ");
			let statement = format!("delete from {table} where id in ({placeholders});");
			let params = ids
				.iter()
				.map(ToString::to_string)
				.map(db::Value::from)
				.collect();
			let result = transaction.execute(statement.into(), params).await;
			crate::database::retry!(result, "failed to delete the named node rows");
		}

		Ok(ControlFlow::Break(()))
	}
}
