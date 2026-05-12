use {crate::Session, tangram_client::prelude::*};

impl tg::handle::User for Session {
	async fn get_current_user(&self, arg: tg::user::current::Arg) -> tg::Result<Option<tg::User>> {
		self.get_current_user(arg).await
	}

	async fn login_user(&self, arg: tg::user::login::Arg) -> tg::Result<tg::user::login::Output> {
		self.login_user(arg).await
	}
}
