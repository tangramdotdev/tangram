use {
	crate::Server,
	indoc::formatdoc,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(super) async fn update_parent_depths_postgres(
		transaction: &db::postgres::Transaction<'_>,
		child_id: String,
	) -> tg::Result<()> {
		let p = transaction.p();
		let statement = formatdoc!(
			"
				call update_parent_depths(array[{p}1]::text[]);
			"
		);
		let params = db::params![child_id];
		transaction
			.execute(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;
		Ok(())
	}
}
