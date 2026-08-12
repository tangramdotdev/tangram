use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

pub mod billing;
pub mod create;
pub mod delete;
pub mod get;
pub mod members;
pub mod usage;

impl Session {
	pub(crate) async fn try_get_organization_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		id: &tg::organization::Id,
	) -> tg::Result<Option<tg::Organization>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
		}

		let Some(specifier) =
			Self::try_get_specifier_for_id_with_transaction(transaction, &id.clone().into())
				.await?
		else {
			return Ok(None);
		};
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select name
				from organizations
				where id = {p}1;
			"
		);
		let row = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await
			.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
		let organization = row.map(|row| tg::Organization {
			id: id.clone(),
			location: Some(tg::Location::Local(tg::location::Local::default())),
			name: row.name,
			specifier,
			tokens: tg::grant::Tokens::default(),
		});

		Ok(organization)
	}
}
