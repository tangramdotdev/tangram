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
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> {
		self.list_remotes(arg)
	}

	fn try_get_remote(
		&self,
		name: &str,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> {
		self.try_get_remote(name)
	}

	fn put_remote(
		&self,
		name: &str,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_remote(name, arg)
	}

	fn try_delete_remote(&self, name: &str) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_remote(name)
	}
}
