use {crate::Component, tangram_client::prelude::*, tangram_server_core::Session};

impl Component {
	pub(crate) async fn get_remote_session(
		&self,
		session: &Session,
		remote: &str,
	) -> tg::Result<tg::Session> {
		self.try_get_remote_session(session, remote)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))
	}

	pub(crate) async fn get_remote_session_for_process(
		&self,
		session: &Session,
		remote: &str,
	) -> tg::Result<tg::Session> {
		session.verify_request_can_access_remote_process()?;
		self.try_get_remote_session_inner(session, remote)
			.await?
			.ok_or_else(|| tg::error!("failed to find the remote"))
	}

	pub(crate) async fn try_get_remote_session(
		&self,
		session: &Session,
		remote: &str,
	) -> tg::Result<Option<tg::Session>> {
		session.verify_request_with_network_access()?;
		self.try_get_remote_session_inner(session, remote).await
	}

	async fn try_get_remote_session_inner(
		&self,
		session: &Session,
		remote: &str,
	) -> tg::Result<Option<tg::Session>> {
		let selector = tg::remote::Selector::Name(remote.to_owned());
		let Some(row) = self
			.try_get_remote_inner(session, &selector, None)
			.await
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote"))?
		else {
			return Ok(None);
		};
		let crate::get::Row {
			token,
			trusted,
			url,
			..
		} = row;
		let client = self
			.get_or_create_remote_client(&session.server, url)
			.map_err(|error| tg::error!(!error, %remote, "failed to get the remote client"))?;
		let mut context = client.context().clone();
		let principal_token = session
			.server
			.components
			.authentication
			.try_get_authenticated_principal_remote_token(session, remote)
			.await?;
		context.set_token(principal_token.token.or(token));
		context.set_process(principal_token.process);
		context.set_sandbox(principal_token.sandbox);
		context.set_trusted(trusted);
		let session = client.session(&context);
		Ok(Some(session))
	}
}
