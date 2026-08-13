use {
	crate::Session,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

pub mod create;
pub mod delete;
pub mod get;
pub mod members;

impl Session {
	pub(crate) async fn try_get_group_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		id: &tg::group::Id,
	) -> tg::Result<ControlFlow<Option<tg::Group>, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			name: String,
			#[tangram_database(as = "Option<db::value::FromStr>")]
			parent: Option<tg::Id>,
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
				select name, parent
				from groups
				where id = {p}1;
			"
		);
		let result = transaction
			.query_optional_into::<Row>(statement.into(), db::params![id.to_string()])
			.await;
		let row = crate::database::retry!(result, "failed to execute the statement");
		let group = row.map(|row| tg::Group {
			id: id.clone(),
			location: Some(tg::Location::Local(tg::location::Local::default())),
			name: row.name,
			parent: row.parent,
			specifier,
			tokens: tg::authorization::Tokens::default(),
		});

		Ok(ControlFlow::Break(group))
	}

	pub(crate) async fn group_contains_group_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		group: &tg::Id,
		member: &tg::Id,
	) -> tg::Result<ControlFlow<bool, crate::database::Error>> {
		#[derive(db::row::Deserialize)]
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			member: tg::Id,
		}
		let mut stack = vec![group.clone()];
		let mut visited = std::collections::BTreeSet::new();
		while let Some(group) = stack.pop() {
			if !visited.insert(group.clone()) {
				continue;
			}
			if &group == member {
				return Ok(ControlFlow::Break(true));
			}
			let p = transaction.p();
			let statement = formatdoc!(
				r#"
					select member
					from group_members
					where "group" = {p}1;
				"#
			);
			let result = transaction
				.query_all_into::<Row>(statement.into(), db::params![group.to_string()])
				.await;
			let rows = crate::database::retry!(result, "failed to execute the statement");
			stack.extend(rows.into_iter().map(|row| row.member));
		}

		Ok(ControlFlow::Break(false))
	}
}
