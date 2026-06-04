use {crate::Session, tangram_client::prelude::*};

impl tg::handle::Grant for Session {
	async fn create_grant(&self, arg: tg::grant::create::Arg) -> tg::Result<tg::Grant> {
		self.create_grant(arg).await
	}

	async fn delete_grant(&self, arg: tg::grant::delete::Arg) -> tg::Result<Option<()>> {
		self.delete_grant(arg).await
	}
}
