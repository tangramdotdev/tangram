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
		for name in remotes.keys() {
			crate::validate_name(name)?;
		}
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
		struct Row {
			#[tangram_database(as = "db::value::FromStr")]
			id: tg::remote::Id,

			name: String,

			token: Option<String>,
		}
		let statement = indoc!(
			r"
				select id, name, token
				from remotes
				where principal is null;
			",
		);
		let result = transaction
			.query_all_into::<Row>(statement.into(), db::params![])
			.await;
		let previous =
			tangram_server_core::database::retry!(result, "failed to execute the statement")
				.into_iter()
				.map(|row| (row.name, (row.id, row.token)))
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
					insert into remotes (id, name, principal, token, trusted, url)
					values ({p}1, {p}2, null, {p}3, {p}4, {p}5);
				",
			);
			let existing = previous.get(name);
			let id = existing.map_or_else(tg::remote::Id::new, |(id, _)| id.clone());
			let token = remote
				.token
				.clone()
				.or_else(|| existing.and_then(|(_, token)| token.clone()));
			let params = db::params![
				id.to_string(),
				name.clone(),
				token,
				remote.trusted,
				remote.url.to_string()
			];
			let result = transaction.execute(statement.into(), params).await;
			tangram_server_core::database::retry!(result, "failed to insert the remote");
		}
		Ok(ControlFlow::Break(()))
	}
}
