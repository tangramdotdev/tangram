use {
	super::ServerWithContext,
	crate::{Context, Server, Shared},
	tangram_client::prelude::*,
};

impl tg::handle::Remote for Shared {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.0.list_remotes(arg).await
	}

	async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		self.0.try_get_remote(name).await
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		self.0.put_remote(name, arg).await
	}

	async fn delete_remote(&self, name: &str) -> tg::Result<()> {
		self.0.delete_remote(name).await
	}
}

impl tg::handle::Remote for Server {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.list_remotes_with_context(&Context::default(), arg)
			.await
	}

	async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		self.try_get_remote_with_context(&Context::default(), name)
			.await
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		self.put_remote_with_context(&Context::default(), name, arg)
			.await
	}

	async fn delete_remote(&self, name: &str) -> tg::Result<()> {
		self.delete_remote_with_context(&Context::default(), name)
			.await
	}
}

impl tg::handle::Remote for ServerWithContext {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.0.list_remotes_with_context(&self.1, arg).await
	}

	async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		self.0.try_get_remote_with_context(&self.1, name).await
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		self.0.put_remote_with_context(&self.1, name, arg).await
	}

	async fn delete_remote(&self, name: &str) -> tg::Result<()> {
		self.0.delete_remote_with_context(&self.1, name).await
	}
}
