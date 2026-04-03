use {
	super::ServerWithContext,
	crate::{Context, Server, Shared},
	tangram_client::prelude::*,
};

impl tg::handle::Sandbox for Shared {
	async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		self.0
			.create_sandbox_with_context(&Context::default(), arg)
			.await
	}

	async fn delete_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<()> {
		self.0
			.delete_sandbox_with_context(&Context::default(), id)
			.await
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

	async fn delete_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<()> {
		self.delete_sandbox_with_context(&Context::default(), id)
			.await
	}
}

impl tg::handle::Sandbox for ServerWithContext {
	async fn create_sandbox(
		&self,
		arg: tg::sandbox::create::Arg,
	) -> tg::Result<tg::sandbox::create::Output> {
		self.0.create_sandbox_with_context(&self.1, arg).await
	}

	async fn delete_sandbox(&self, id: &tg::sandbox::Id) -> tg::Result<()> {
		self.0.delete_sandbox_with_context(&self.1, id).await
	}
}
