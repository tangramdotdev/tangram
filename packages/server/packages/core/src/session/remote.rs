use {crate::Session, tangram_client::prelude::*};

impl tg::instance::Remote for Session {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.server.components.remote.list_remotes(self, arg).await
	}

	async fn try_get_remote(
		&self,
		remote: &tg::remote::Selector,
		arg: tg::remote::get::Arg,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		self.server
			.components
			.remote
			.try_get_remote(self, remote, arg)
			.await
	}

	async fn put_remote(
		&self,
		remote: &tg::remote::Selector,
		arg: tg::remote::put::Arg,
	) -> tg::Result<()> {
		self.server
			.components
			.remote
			.put_remote(self, remote, arg)
			.await
	}

	async fn try_delete_remote(
		&self,
		remote: &tg::remote::Selector,
		arg: tg::remote::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.server
			.components
			.remote
			.try_delete_remote(self, remote, arg)
			.await
	}
}
