use {crate::Session, tangram_client::prelude::*};

impl tg::instance::User for Session {
	async fn create_user_token(
		&self,
		arg: tg::user::token::create::Arg,
	) -> tg::Result<tg::user::token::create::Output> {
		self.server
			.components
			.user
			.create_user_token(self, arg)
			.await
	}

	async fn try_delete_user_token(
		&self,
		token: &tg::token::Id,
		arg: tg::user::token::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.user
			.try_delete_user_token(self, token, arg)
			.await
	}

	async fn try_get_user_token(
		&self,
		token: &tg::token::Id,
		arg: tg::user::token::get::Arg,
	) -> tg::Result<Option<tg::user::token::get::Output>> {
		self.server
			.components
			.user
			.try_get_user_token(self, token, arg)
			.await
	}

	async fn list_user_tokens(
		&self,
		arg: tg::user::token::list::Arg,
	) -> tg::Result<tg::user::token::list::Output> {
		self.server
			.components
			.user
			.list_user_tokens(self, arg)
			.await
	}

	async fn get_current_user(
		&self,
		arg: tg::user::current::Arg,
	) -> tg::Result<Option<tg::user::get::Output>> {
		self.server
			.components
			.user
			.get_current_user(self, arg)
			.await
	}

	async fn create_login(
		&self,
		arg: tg::user::login::create::Arg,
	) -> tg::Result<tg::user::login::create::Output> {
		self.server.components.user.create_login(self, arg).await
	}

	async fn logout(&self, arg: tg::user::logout::Arg) -> tg::Result<()> {
		self.server.components.user.logout(self, arg).await
	}

	async fn try_get_user(
		&self,
		user: &tg::user::Selector,
		arg: tg::user::get::Arg,
	) -> tg::Result<Option<tg::user::get::Output>> {
		self.server
			.components
			.user
			.try_get_user(self, user, arg)
			.await
	}

	async fn try_get_user_usage(
		&self,
		user: Option<&tg::user::Selector>,
		arg: tg::usage::Arg,
	) -> tg::Result<Option<tg::usage::Output>> {
		self.server
			.components
			.user
			.try_get_user_usage(self, user, arg)
			.await
	}

	async fn manage_user_billing(
		&self,
		arg: tg::user::billing::manage::Arg,
	) -> tg::Result<tg::user::billing::manage::Output> {
		self.server
			.components
			.user
			.manage_user_billing(self, arg)
			.await
	}

	async fn wait_login(
		&self,
		arg: tg::user::login::wait::Arg,
	) -> tg::Result<tg::user::login::wait::Output> {
		self.server.components.user.wait_login(self, arg).await
	}
}
