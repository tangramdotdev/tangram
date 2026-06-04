use {crate::Server, tangram_client::prelude::*};

impl tg::handle::Grant for Server {
	async fn create_grant(&self, arg: tg::grant::create::Arg) -> tg::Result<tg::Grant> {
		self.session(&self.context).create_grant(arg).await
	}

	async fn delete_grant(&self, arg: tg::grant::delete::Arg) -> tg::Result<Option<()>> {
		self.session(&self.context).delete_grant(arg).await
	}
}
