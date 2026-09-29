use crate::prelude::*;

pub trait Watch: Clone + Unpin + Send + Sync + 'static {
	/// Collect all pages, using the limit as the page size and the cursor as the starting point.
	fn list_all_watches(
		&self,
		mut arg: tg::watch::list::Arg,
	) -> impl Future<Output = tg::Result<tg::watch::list::Output>> + Send {
		async move {
			let mut output = self.list_watches(arg.clone()).await?;
			while let Some(cursor) = output.cursor.take() {
				arg.cursor = Some(cursor);
				let page = self.list_watches(arg.clone()).await?;
				output.data.extend(page.data);
				output.cursor = page.cursor;
			}
			Ok(output)
		}
	}

	fn list_watches(
		&self,
		arg: tg::watch::list::Arg,
	) -> impl Future<Output = tg::Result<tg::watch::list::Output>> + Send;

	fn delete_watch(
		&self,
		arg: tg::watch::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_delete_watch(arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the watch"))
		}
	}

	fn try_delete_watch(
		&self,
		arg: tg::watch::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn touch_watch(
		&self,
		arg: tg::watch::touch::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;
}

impl tg::instance::Watch for tg::Client {
	async fn list_watches(&self, arg: tg::watch::list::Arg) -> tg::Result<tg::watch::list::Output> {
		self.session(&self.context).list_watches(arg).await
	}

	async fn try_delete_watch(&self, arg: tg::watch::delete::Arg) -> tg::Result<Option<()>> {
		self.session(&self.context).try_delete_watch(arg).await
	}

	async fn touch_watch(&self, arg: crate::watch::touch::Arg) -> tg::Result<()> {
		self.session(&self.context).touch_watch(arg).await
	}
}
