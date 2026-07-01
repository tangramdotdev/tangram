use {crate::Server, tangram_client::prelude::*};

impl tg::handle::User for Server {
	async fn get_current_user(&self, arg: tg::user::current::Arg) -> tg::Result<Option<tg::User>> {
		self.session(&self.context).get_current_user(arg).await
	}

	async fn create_login(
		&self,
		arg: tg::user::login::create::Arg,
	) -> tg::Result<tg::user::login::create::Output> {
		self.session(&self.context).create_login(arg).await
	}

	async fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> tg::Result<Option<tg::User>> {
		self.session(&self.context).try_get_user(user, arg).await
	}

	async fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> tg::Result<tg::user::login::wait::Output> {
		self.session(&self.context).wait_login(arg).await
	}
}
