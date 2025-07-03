use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

#[derive(Clone, serde::Deserialize, serde::Serialize)]
pub struct Output {
	pub commands_complete: bool,
	pub complete: bool,
	pub outputs_complete: bool,
}

impl Server {
	pub(crate) async fn try_get_process_complete_local(
		&self,
		id: &tg::process::Id,
	) -> tg::Result<Option<Output>> {
		// Get an index connection.
		let connection = self
			.index
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Get the object metadata.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select commands_complete, complete, outputs_complete
				from processes
				where id = {p}1;
			",
		);
		let params = db::params![id];
		let output = connection
			.query_optional_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(output)
	}
}
