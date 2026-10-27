use {
	crate::Component, futures::FutureExt as _, indoc::formatdoc, std::ops::ControlFlow,
	tangram_client::prelude::*, tangram_database::prelude::*, tangram_server_core::Session,
};

impl Component {
	pub(crate) async fn set_remote_token(
		&self,
		session: &Session,
		remote: &str,
		token: Option<String>,
	) -> tg::Result<()> {
		Self::verify_request_can_mutate_remotes(session)?;
		let principal = self.resolve_remote_arg_principal(session, None).await?;
		let principal = principal.as_ref().map(ToString::to_string);
		let remote = remote.to_owned();
		let n = session
			.server
			.database
			.run(|transaction| {
				let principal = principal.clone();
				let remote = remote.clone();
				let token = token.clone();
				async move {
					Self::set_remote_token_with_transaction(
						transaction,
						&remote,
						principal.as_deref(),
						token.as_deref(),
					)
					.await
				}
				.boxed()
			})
			.await
			.map_err(|error| tg::error!(!error, "failed to set the remote token"))?;
		if n == 0 {
			return Err(tg::error!("failed to find the remote"));
		}
		Ok(())
	}

	async fn set_remote_token_with_transaction(
		transaction: &tangram_server_core::database::Transaction<'_>,
		remote: &str,
		principal: Option<&str>,
		token: Option<&str>,
	) -> tg::Result<ControlFlow<u64, tangram_server_core::database::Error>> {
		let p = transaction.p();
		let statement = formatdoc!(
			r"
				update remotes
				set token = {p}3
				where name = {p}1 and (
					(principal is null and cast({p}2 as text) is null) or
					principal = {p}2
				);
			",
		);
		let params = tangram_database::params![remote, principal, token];
		let result = transaction.execute(statement.into(), params).await;
		let n = tangram_server_core::database::retry!(result, "failed to execute the statement");
		Ok(ControlFlow::Break(n))
	}
}
