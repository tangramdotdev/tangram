use {
	crate::Session,
	indoc::formatdoc,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Session {
	pub(super) async fn update_parent_depths_postgres(
		&self,
		transaction: &db::postgres::Transaction<'_>,
		child_id: String,
	) -> tg::Result<ControlFlow<(), db::postgres::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				call update_parent_depths(array[{p}1]::text[]);
			"
		);
		let params = db::params![child_id];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(()))
	}
}
