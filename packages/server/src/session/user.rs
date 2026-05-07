use {crate::Session, tangram_client::prelude::*};

impl tg::handle::User for Session {
	async fn get_user(&self, token: &str) -> tg::Result<Option<tg::User>> {
		self.get_user(token).await
	}

	async fn login_user(&self, arg: tg::user::login::Arg) -> tg::Result<tg::user::login::Output> {
		self.login_user(arg).await
	}
}
