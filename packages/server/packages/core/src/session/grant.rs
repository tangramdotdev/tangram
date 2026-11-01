use {crate::Session, tangram_client::prelude::*};

impl tg::instance::Grant for Session {
	async fn grant(&self, arg: tg::grant::grant::Arg) -> tg::Result<tg::grant::grant::Output> {
		self.server.components.grant.grant(self, arg).await
	}

	async fn revoke(&self, arg: tg::grant::revoke::Arg) -> tg::Result<()> {
		self.server.components.grant.revoke(self, arg).await
	}

	async fn try_delete_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.grant
			.try_delete_grant(self, grant, arg)
			.await
	}

	async fn try_get_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> tg::Result<Option<tg::grant::get::Output>> {
		self.server
			.components
			.grant
			.try_get_grant(self, grant, arg)
			.await
	}

	async fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		self.server.components.grant.list_grants(self, arg).await
	}
}
