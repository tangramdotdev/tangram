use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub async fn try_resolve_named_node_with_transaction(
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

	pub async fn try_get_ids_for_specifiers_with_transaction(
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

	pub async fn verify_ids_for_specifiers_with_transaction(
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

	pub fn delete_permission_for_named_node(
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

	pub async fn collect_named_subtrees_with_transaction(
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

	pub async fn delete_named_node_rows_with_transaction(
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
