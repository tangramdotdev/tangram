use {
	super::ServerWithContext,
	crate::{Context, Server, Shared},
	tangram_client::prelude::*,
};

impl tg::handle::Watch for Shared {
	async fn list_watches(&self, arg: tg::watch::list::Arg) -> tg::Result<tg::watch::list::Output> {
		self.0.list_watches(arg).await
	}

	async fn delete_watch(&self, arg: tg::watch::delete::Arg) -> tg::Result<()> {
		self.0.delete_watch(arg).await
	}

	async fn touch_watch(&self, arg: tg::watch::touch::Arg) -> tg::Result<()> {
		self.0.touch_watch(arg).await
	}
}

impl tg::handle::Watch for Server {
	async fn list_watches(&self, arg: tg::watch::list::Arg) -> tg::Result<tg::watch::list::Output> {
		self.list_watches_with_context(&Context::default(), arg)
			.await
	}

	async fn delete_watch(&self, arg: tg::watch::delete::Arg) -> tg::Result<()> {
		self.delete_watch_with_context(&Context::default(), arg)
			.await
	}

	async fn touch_watch(&self, arg: tg::watch::touch::Arg) -> tg::Result<()> {
		self.touch_watch_with_context(&Context::default(), arg)
			.await
	}
}

impl tg::handle::Watch for ServerWithContext {
	async fn list_watches(&self, arg: tg::watch::list::Arg) -> tg::Result<tg::watch::list::Output> {
		self.0.list_watches_with_context(&self.1, arg).await
	}

	async fn delete_watch(&self, arg: tg::watch::delete::Arg) -> tg::Result<()> {
		self.0.delete_watch_with_context(&self.1, arg).await
	}

	async fn touch_watch(&self, arg: tg::watch::touch::Arg) -> tg::Result<()> {
		self.0.touch_watch_with_context(&self.1, arg).await
	}
}
