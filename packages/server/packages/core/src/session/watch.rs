use {crate::Session, tangram_client::prelude::*};

impl tg::instance::Watch for Session {
	async fn list_watches(&self, arg: tg::watch::list::Arg) -> tg::Result<tg::watch::list::Output> {
		self.server.components.watch.list_watches(self, arg).await
	}

	async fn try_delete_watch(&self, arg: tg::watch::delete::Arg) -> tg::Result<Option<()>> {
		self.server
			.components
			.watch
			.try_delete_watch(self, arg)
			.await
	}

	async fn touch_watch(&self, arg: tg::watch::touch::Arg) -> tg::Result<()> {
		self.server.components.watch.touch_watch(self, arg).await
	}
}
