use {
	crate::{Context, Server, Shared},
	tangram_client::prelude::*,
};

impl tg::handle::Sandbox for Shared {
	async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		self.0.create_sandbox(arg).await
	}

	async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		self.0.list_sandboxes(arg).await
	}

	async fn delete_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<()> {
		self.0.delete_sandbox(id).await
	}
}

impl tg::handle::Sandbox for Server {
	async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		self.create_sandbox_with_context(&Context::default(), arg)
			.await
	}

	async fn list_sandboxes(
		&self,
		arg: tg::sandbox::list::Arg,
	) -> tg::Result<tg::sandbox::list::Output> {
		self.list_sandboxes_with_context(&Context::default(), arg)
			.await
	}

	async fn delete_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<()> {
		self.delete_sandbox_with_context(&Context::default(), id)
			.await
	}
}
