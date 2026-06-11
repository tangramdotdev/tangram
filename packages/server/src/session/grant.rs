use {crate::Session, tangram_client::prelude::*};

impl tg::handle::Grant for Session {
	async fn create_grant(&self, arg: tg::grant::create::Arg) -> tg::Result<tg::Grant> {
		self.create_grant(arg).await
	}

	async fn delete_grant(&self, arg: tg::grant::delete::Arg) -> tg::Result<Option<()>> {
		self.delete_grant(arg).await
	}

	async fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> tg::Result<Option<tg::grant::list::Output>> {
		self.list_grants(arg).await
	}
}
