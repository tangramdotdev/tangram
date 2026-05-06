use {crate::Handle, tangram_client::prelude::*};

impl tg::handle::Remote for Handle {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.list_remotes(arg).await
	}

	async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		self.try_get_remote(name).await
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		self.put_remote(name, arg).await
	}

	async fn try_delete_remote(&self, name: &str) -> tg::Result<Option<()>> {
		self.try_delete_remote(name).await
	}
}
