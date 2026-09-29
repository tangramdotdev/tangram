use crate::prelude::*;

pub trait Remote: Clone + Unpin + Send + Sync + 'static {
	/// Collect all pages, using the limit as the page size and the cursor as the starting point.
	fn list_all_remotes(
		&self,
		mut arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> + Send {
		async move {
			let mut output = self.list_remotes(arg.clone()).await?;
			while let Some(cursor) = output.cursor.take() {
				arg.cursor = Some(cursor);
				let page = self.list_remotes(arg.clone()).await?;
				output.data.extend(page.data);
				output.cursor = page.cursor;
			}
			Ok(output)
		}
	}

	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> + Send;

	fn try_get_remote(
		&self,
		name: &str,
		arg: tg::remote::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> + Send;

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_remote(
		&self,
		name: &str,
		arg: tg::remote::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_delete_remote(name, arg)
				.await?
				.ok_or_else(|| tg::error!("failed to find the remote"))
		}
	}

	fn try_delete_remote(
		&self,
		name: &str,
		arg: tg::remote::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;
}

impl tg::handle::Remote for tg::Client {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.session(&self.context).list_remotes(arg).await
	}

	async fn try_get_remote(
		&self,
		name: &str,
		arg: tg::remote::get::Arg,
	) -> tg::Result<Option<tg::remote::get::Output>> {
		self.session(&self.context).try_get_remote(name, arg).await
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		self.session(&self.context).put_remote(name, arg).await
	}

	async fn try_delete_remote(
		&self,
		name: &str,
		arg: tg::remote::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_delete_remote(name, arg)
			.await
	}
}
