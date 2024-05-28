use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

mod children;
mod dequeue;
mod finish;
mod get;
mod heartbeat;
mod list;
mod log;
mod monitor;
mod outcome;
mod pull;
mod push;
mod put;
mod spawn;
mod start;
mod status;
mod touch;

impl Server {
	pub(crate) async fn get_build_exists_local(&self, id: &tg::build::Id) -> tg::Result<bool> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Check if the build exists.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count(*) != 0
				from builds
				where id = {p}1;
			"
		);
		let params = db::params![id];
		let exists = connection
			.query_one_value_into(statement, params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(exists)
	}
}
