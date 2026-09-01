use {
	crate::Session,
	indoc::formatdoc,
	std::ops::ControlFlow,
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
	) -> tg::Result<ControlFlow<Option<tg::organization::Data>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
		}

		let specifier =
			match Self::try_get_specifier_for_id_with_transaction(transaction, &id.clone().into())
				.await?
			{
				ControlFlow::Break(specifier) => specifier,
				ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
			};
		let Some(specifier) = specifier else {
			return Ok(ControlFlow::Break(None));
		};
		let p = transaction.p();
		let statement = formatdoc!(
			"
				select name
				from organizations
				where id = {p}1;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let organization = row.map(|row| tg::organization::Data {
			id: id.clone(),
			name: row.name,
			specifier,
		});

		Ok(ControlFlow::Break(organization))
	}
}
