use {
	crate::Session,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

pub mod create;
pub mod delete;
pub mod get;
pub mod list;
pub mod members;

impl Session {
	pub(crate) async fn group_contains_group_with_transaction(
		transaction: &crate::database::Transaction<'_>,
		group: &tg::Id,
		member: &tg::Id,
	) -> tg::Result<bool> {
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
				return Ok(true);
			}
			let p = transaction.p();
			let statement = formatdoc!(
				r#"
					select member
					from group_members
					where "group" = {p}1;
				"#
			);
			let rows = transaction
				.query_all_into::<Row>(statement.into(), db::params![group.to_string()])
				.await
				.map_err(|error| tg::error!(!error, "failed to execute the statement"))?;
			stack.extend(rows.into_iter().map(|row| row.member));
		}
		Ok(false)
	}
}
