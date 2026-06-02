use {
	crate::{Server, database},
	indoc::formatdoc,
	num::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
};

impl Server {
	pub(crate) async fn grant_process_with_transaction(
		&self,
		transaction: &database::Transaction<'_>,
		process: &tg::process::Id,
		principal: &tg::Principal,
		now: i64,
	) -> tg::Result<ControlFlow<(), database::Error>> {
		let p = transaction.p();
		let max_expires_at = match transaction {
			#[cfg(feature = "postgres")]
			database::Transaction::Postgres(_) => "greatest(process_grants.expires_at, excluded.expires_at)",
			#[cfg(feature = "sqlite")]
			database::Transaction::Sqlite(_) => "max(process_grants.expires_at, excluded.expires_at)",
			#[cfg(feature = "turso")]
			database::Transaction::Turso(_) => "max(process_grants.expires_at, excluded.expires_at)",
		};
		let grant_ttl = self.config.process.grant_time_to_live.as_secs();
		let expires_at = now + grant_ttl.to_i64().unwrap();
		let statement = formatdoc!(
			"
				insert into process_grants (
					process,
					principal,
					node,
					node_command,
					node_error,
					node_log,
					node_output,
					subtree,
					subtree_command,
					subtree_error,
					subtree_log,
					subtree_output,
					created_at,
					expires_at
				) values (
					{p}1,
					{p}2,
					true,
					true,
					true,
					true,
					true,
					true,
					true,
					true,
					true,
					true,
					{p}3,
					{p}4
				)
				on conflict (process, principal) do update set
					node = process_grants.node or excluded.node,
					node_command = process_grants.node_command or excluded.node_command,
					node_error = process_grants.node_error or excluded.node_error,
					node_log = process_grants.node_log or excluded.node_log,
					node_output = process_grants.node_output or excluded.node_output,
					subtree = process_grants.subtree or excluded.subtree,
					subtree_command = process_grants.subtree_command or excluded.subtree_command,
					subtree_error = process_grants.subtree_error or excluded.subtree_error,
					subtree_log = process_grants.subtree_log or excluded.subtree_log,
					subtree_output = process_grants.subtree_output or excluded.subtree_output,
					expires_at = {max_expires_at};
			"
		);
		let params = db::params![process.to_string(), principal.to_string(), now, expires_at];
		let result = transaction.execute(statement.into(), params).await;
		crate::database::retry!(result, "failed to grant the process");
		Ok(ControlFlow::Break(()))
	}
}
