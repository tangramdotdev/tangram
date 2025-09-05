use crate::Server;
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

pub(crate) mod cancel;
pub(crate) mod children;
pub(crate) mod complete;
pub(crate) mod dequeue;
pub(crate) mod finish;
pub(crate) mod get;
pub(crate) mod heartbeat;
pub(crate) mod log;
pub(crate) mod metadata;
pub(crate) mod put;
pub(crate) mod signal;
pub(crate) mod spawn;
pub(crate) mod start;
pub(crate) mod status;
pub(crate) mod touch;
pub(crate) mod wait;

impl Server {
	pub(crate) async fn get_process_exists_local(&self, id: &tg::process::Id) -> tg::Result<bool> {
		// Get a database connection.
		let connection = self
			.database
			.connection()
			.await
			.map_err(|source| tg::error!(!source, "failed to get a database connection"))?;

		// Check if the process exists.
		let p = connection.p();
		let statement = formatdoc!(
			"
				select count(*) != 0
				from processes
				where id = {p}1;
			"
		);
		let params = db::params![id.to_string()];
		let exists = connection
			.query_one_value_into(statement.into(), params)
			.await
			.map_err(|source| tg::error!(!source, "failed to execute the statement"))?;

		// Drop the database connection.
		drop(connection);

		Ok(exists)
	}
}
