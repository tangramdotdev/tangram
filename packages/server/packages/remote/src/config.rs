use {
	crate::Component,
	futures::FutureExt as _,
	indoc::{formatdoc, indoc},
	std::{collections::BTreeMap, ops::ControlFlow},
	tangram_client::prelude::*,
	tangram_database::{self as db, prelude::*},
	tangram_server_core::{
		Server,
		database::{Error, Transaction},
	},
};

impl Component {
	pub(crate) async fn set_config_remotes(server: &Server) -> tg::Result<()> {
		if !server.is_primary_region() {
			return Ok(());
		}
		let Some(remotes) = &server.config.remotes else {
			return Ok(());
		};
		let remotes = remotes.clone();
		server
			.database
			.run(|transaction| {
				let remotes = remotes.clone();
				async move { Self::set_config_remotes_with_transaction(transaction, &remotes).await }
					.boxed()
			})
			.await?;
		Ok(())
	}

	async fn set_config_remotes_with_transaction(
		transaction: &Transaction<'_>,
		remotes: &BTreeMap<String, tangram_server_core::config::Remote>,
	) -> tg::Result<ControlFlow<(), Error>> {
		#[derive(db::row::Deserialize)]
		struct RemoteTokenRow {
			name: String,
			token: Option<String>,
		}
		let statement = indoc!(
			r"
				select name, token
				from remotes
				where principal is null;
			",
		);
		let result = transaction
			.query_all_into::<RemoteTokenRow>(statement.into(), db::params![])
			.await;
		let tokens =
			tangram_server_core::database::retry!(result, "failed to execute the statement")
				.into_iter()
				.map(|row| (row.name, row.token))
				.collect::<BTreeMap<_, _>>();
		let statement = indoc!(
			r"
				delete from remotes
				where principal is null;
			",
		);
		let result = transaction.execute(statement.into(), db::params![]).await;
		tangram_server_core::database::retry!(result, "failed to delete the remotes");
		for (name, remote) in remotes {
			let p = transaction.p();
			let statement = formatdoc!(
				r"
					insert into remotes (name, principal, token, trusted, url)
					values ({p}1, null, {p}2, {p}3, {p}4);
				",
			);
			let token = remote
				.token
				.clone()
				.or_else(|| tokens.get(name).cloned().flatten());
			let params = db::params![name.clone(), token, remote.trusted, remote.url.to_string()];
			let result = transaction.execute(statement.into(), params).await;
			tangram_server_core::database::retry!(result, "failed to insert the remote");
		}
		Ok(ControlFlow::Break(()))
	}
}
