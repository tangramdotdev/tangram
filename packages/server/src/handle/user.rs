use {crate::Server, tangram_client::prelude::*};

impl tg::handle::User for Server {
	async fn get_current_user(&self, arg: tg::user::current::Arg) -> tg::Result<Option<tg::User>> {
		self.session(&self.context).get_current_user(arg).await
	}

	async fn login_user(&self, arg: tg::user::login::Arg) -> tg::Result<tg::user::login::Output> {
		self.session(&self.context).login_user(arg).await
	}

	async fn list_user_namespace_grants(
		&self,
		user: &str,
		arg: tg::user::grants::Arg,
	) -> tg::Result<Option<tg::user::grants::Output>> {
		self.session(&self.context)
			.list_user_namespace_grants(user, arg)
			.await
	}
}
