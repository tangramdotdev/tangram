use crate::Server;
use futures::{stream::FuturesUnordered, TryStreamExt};
use indoc::formatdoc;
use tangram_client as tg;
use tangram_database::{self as db, prelude::*};

mod children;
mod dequeue;
mod finish;
mod get;
mod heartbeat;
mod index;
mod log;
mod outcome;
mod pull;
mod push;
mod put;
mod spawn;
mod start;
mod status;
mod touch;

impl Server {
	pub(crate) async fn try_get_remote_for_build(
		&self,
		id: &tg::build::Id,
	) -> tg::Result<Option<String>> {
		let name = self
			.options
			.remotes
			.iter()
			.filter(|(_name, remote)| remote.build)
			.map(|(name, remote)| async {
				let exists = remote.client.try_get_build(id).await?;
				Ok::<_, tg::Error>(exists.is_some().then(|| name.clone()))
			})
			.collect::<FuturesUnordered<_>>()
			.try_next()
			.await?
			.flatten();
		Ok(name)
	}

	pub(crate) async fn get_build_exists_local(&self, id: &tg::build::Id) -> tg::Result<bool> {
		// Get a database connection.
		let connection = self
			.database
			.connection(db::Priority::Low)
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
