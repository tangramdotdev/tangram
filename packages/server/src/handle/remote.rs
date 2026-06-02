use {crate::Server, tangram_client::prelude::*};

impl tg::handle::Remote for Server {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		// self.session(&self.context).list_remotes(arg).await
		Err(tg::error!("todo"))
	}

	async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		// self.session(&self.context).try_get_remote(name).await
		Err(tg::error!("todo"))
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		// self.session(&self.context).put_remote(name, arg).await
		Err(tg::error!("todo"))
	}

	async fn try_delete_remote(&self, name: &str) -> tg::Result<Option<()>> {
		// self.session(&self.context).try_delete_remote(name).await
		Err(tg::error!("todo"))
	}
}
