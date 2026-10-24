use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub async fn insert_specifier_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
		specifier: &tg::Specifier,
	) -> tg::Result<ControlFlow<(), crate::database::Error>> {
		if specifier.components().next().is_none() {
			return Err(tg::error!("invalid specifier"));
		}
		let p = transaction.p();
		let statement = formatdoc!(
			"
				insert into specifiers (id, specifier)
				values ({p}1, {p}2);
			"
		);
		let result = transaction
			.execute(
				statement.into(),
				db::params![id.to_string(), specifier.to_string()],
			)
			.await;
		crate::database::retry!(result, "failed to execute the statement");

		Ok(ControlFlow::Break(()))
	}

	pub async fn try_get_specifier_for_id_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
	) -> tg::Result<ControlFlow<Option<tg::Specifier>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			specifier: tg::Specifier,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			"
				select specifier
				from specifiers
				where id = {p}1;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let specifier = row.map(|row| row.specifier);

		Ok(ControlFlow::Break(specifier))
	}

	pub async fn try_get_id_for_specifier_with_transaction(
		transaction: &Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<ControlFlow<Option<tg::Id>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::Id,
		}

		let p = transaction.p();
		let statement = formatdoc!(
			"
				select id
				from specifiers
				where specifier = {p}1;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![specifier.to_string()])
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let id = row.map(|row| row.id);

		Ok(ControlFlow::Break(id))
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
}
