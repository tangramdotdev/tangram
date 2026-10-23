use {crate::Session, tangram_client::prelude::*};

impl tg::instance::Grant for Session {
	async fn create_grant(
		&self,
		arg: tg::grant::create::Arg,
	) -> tg::Result<tg::grant::create::Output> {
		self.server.components.grant.create_grant(self, arg).await
	}

	async fn delete_grant(&self, arg: tg::grant::delete::Arg) -> tg::Result<Option<()>> {
		self.server.components.grant.delete_grant(self, arg).await
	}

	async fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		self.server.components.grant.list_grants(self, arg).await
	}
}
