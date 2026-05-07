use {crate::Server, tangram_client::prelude::*};

impl tg::handle::User for Server {
	async fn get_user(&self, token: &str) -> tg::Result<Option<tg::User>> {
		self.session(&self.context).get_user(token).await
	}

	async fn login_user(&self, arg: tg::user::login::Arg) -> tg::Result<tg::user::login::Output> {
		self.session(&self.context).login_user(arg).await
	}
}
