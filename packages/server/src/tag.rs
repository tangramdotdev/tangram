pub mod batch;
pub mod delete;
pub mod get;
pub mod pull;
pub mod put;

use {
	crate::{Session, database::Transaction},
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client as tg,
	tangram_database::{self as db, prelude::*},
};

pub(crate) fn version() -> String {
	tg::id::ENCODING.encode(uuid::Uuid::now_v7().as_bytes())
}

impl Session {
	pub(crate) async fn get_tag_data_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::tag::Id,
	) -> tg::Result<ControlFlow<tg::tag::Data, crate::database::Error>> {
		let data = match Self::try_get_tag_data_with_transaction(transaction, id).await? {
			ControlFlow::Break(data) => data,
			ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
		}
		.ok_or_else(|| tg::error!("failed to find the tag"))?;

		Ok(ControlFlow::Break(data))
	}

	pub(crate) async fn try_get_tag_data_with_transaction(
		transaction: &Transaction<'_>,
		id: &tg::tag::Id,
	) -> tg::Result<ControlFlow<Option<tg::tag::Data>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
			target: String,
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
				select target, name, parent
				from tags
				where id = {p}1;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let Some(row) = row else {
			return Ok(ControlFlow::Break(None));
		};
		let target = Self::parse_tag_target(&row.target)?;
		let data = tg::tag::Data {
			id: id.clone(),
			name: row.name,
			parent: row.parent,
			specifier,
			target,
		};

		Ok(ControlFlow::Break(Some(data)))
	}

	pub(crate) fn parse_tag_target(target: &str) -> tg::Result<tg::tag::data::Target> {
		target
			.parse::<tg::Either<tg::object::Id, tg::process::Id>>()
			.map(Into::into)
			.map_err(|error| tg::error!(!error, "failed to parse the tag target"))
	}

	pub(crate) fn tag_target_to_string(target: &tg::tag::data::Target) -> String {
		match target {
			tg::tag::data::Target::Object(id) => id.to_string(),
			tg::tag::data::Target::Process(id) => id.to_string(),
		}
	}
}
