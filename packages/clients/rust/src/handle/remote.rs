use crate::prelude::*;

pub trait Remote: Clone + Unpin + Send + Sync + 'static {
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> + Send;

	fn try_get_remote(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> + Send;

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_delete_remote(name)
				.await?
				.ok_or_else(|| tg::error!("failed to find the remote"))
		}
	}

	fn try_delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<Option<()>>> + Send;
}

impl tg::handle::Remote for tg::Client {
	async fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> tg::Result<tg::remote::list::Output> {
		self.session(&self.context).list_remotes(arg).await
	}

	async fn try_get_remote(&self, name: &str) -> tg::Result<Option<tg::remote::get::Output>> {
		self.session(&self.context).try_get_remote(name).await
	}

	async fn put_remote(&self, name: &str, arg: tg::remote::put::Arg) -> tg::Result<()> {
		self.session(&self.context).put_remote(name, arg).await
	}

	async fn try_delete_remote(&self, name: &str) -> tg::Result<Option<()>> {
		self.session(&self.context).try_delete_remote(name).await
	}
}
