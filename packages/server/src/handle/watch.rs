use {crate::Server, tangram_client::prelude::*};

impl tg::handle::Watch for Server {
	async fn list_watches(&self, arg: tg::watch::list::Arg) -> tg::Result<tg::watch::list::Output> {
		self.session(&self.context).list_watches(arg).await
	}

	async fn try_delete_watch(&self, arg: tg::watch::delete::Arg) -> tg::Result<Option<()>> {
		self.session(&self.context).try_delete_watch(arg).await
	}

	async fn touch_watch(&self, arg: tg::watch::touch::Arg) -> tg::Result<()> {
		self.session(&self.context).touch_watch(arg).await
	}
}
