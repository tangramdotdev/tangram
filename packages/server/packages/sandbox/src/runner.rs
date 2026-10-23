use {crate::Component, tangram_client::prelude::*, tangram_server_core::Session};

impl Component {
	pub(crate) async fn authorize_sandbox_runner(
		&self,
		session: &Session,
		id: &tg::sandbox::Id,
		tokens: &[tg::authorization::Token],
		permission: tg::authorization::permission::sandbox::Permission,
	) -> tg::Result<bool> {
		let resource = tg::Referent::with_node_and_local_tokens(id.clone(), tokens.to_vec());
		let permission = tg::authorization::Permission::Sandbox(permission);
		// Never use the sandbox's own authorization tokens to authorize a caller.
		let permissions = session
			.authorize(resource, permission)
			.await?
			.check_exhaustion()?;
		Ok(permissions.permissions.contains(permission))
	}
}
