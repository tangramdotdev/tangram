use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(crate) async fn insert_specifier_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
		specifier: &tg::Specifier,
	) -> tg::Result<()> {
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
		transaction
			.execute(
				statement.into(),
				db::params![id.to_string(), specifier.to_string()],
			)
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;

		Ok(())
	}

	pub(crate) async fn try_get_specifier_for_id_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::Id,
	) -> tg::Result<Option<tg::Specifier>> {
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
		let row = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let specifier = row.map(|row| row.specifier);

		Ok(specifier)
	}

	pub(crate) async fn try_get_id_for_specifier_with_transaction(
		transaction: &Transaction<'_>,
		specifier: &tg::Specifier,
	) -> tg::Result<Option<tg::Id>> {
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
		let row = transaction
			.query_optional_into::<Row>(statement.into(), db::params![specifier.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let id = row.map(|row| row.id);

		Ok(id)
	}
}
