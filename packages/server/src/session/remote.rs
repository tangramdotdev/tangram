use {crate::Session, tangram_client::prelude::*};

impl tg::handle::Remote for Session {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.list_remotes(arg).await
	}

	async fn try_get_remote(
		&self,
		name: &str,
		arg: tg::remote::get::Arg,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		self.try_get_remote(name, arg).await
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		self.put_remote(name, arg).await
	}

	async fn delete_remote(&self, name: &str, arg: tg::remote::delete::Arg) -> tg::Result<()> {
		self.delete_remote(name, arg).await
	}
}
