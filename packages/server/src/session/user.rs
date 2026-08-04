use {crate::Session, tangram_client::prelude::*};

impl tg::handle::User for Session {
	async fn get_current_user(&self, arg: tg::user::current::Arg) -> tg::Result<Option<tg::User>> {
		self.get_current_user(arg).await
	}

	async fn create_login(
		&self,
		arg: tg::user::login::create::Arg,
	) -> tg::Result<tg::user::login::create::Output> {
		self.create_login(arg).await
	}

	async fn logout(&self) -> tg::Result<()> {
		self.logout().await
	}

	async fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> tg::Result<Option<tg::user::get::Output>> {
		self.try_get_user(user, arg).await
	}

	async fn try_get_user_usage(
		&self,
		user: &tg::user::Selector,
	) -> tg::Result<Option<tg::usage::Output>> {
		self.try_get_user_usage(user).await
	}

	async fn manage_user_billing(
		&self,
		arg: tg::user::billing::manage::Arg,
	) -> tg::Result<tg::user::billing::manage::Output> {
		self.manage_user_billing(arg).await
	}

	async fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> tg::Result<tg::user::login::wait::Output> {
		self.wait_login(arg).await
	}
}
