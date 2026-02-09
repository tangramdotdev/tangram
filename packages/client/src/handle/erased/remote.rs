use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Remote: Send + Sync + 'static {
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::remote::list::Output>>;

	fn try_get_remote<'a>(
		&'a self,
		name: &'a str,
	) -> BoxFuture<'a, tg::Result<Option<tg::remote::get::Output>>>;

	fn put_remote<'a>(
		&'a self,
		name: &'a str,
		arg: tg::remote::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn delete_remote<'a>(&'a self, name: &'a str) -> BoxFuture<'a, tg::Result<()>>;
}

impl<T> Remote for T
where
	T: tg::handle::Remote,
{
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::remote::list::Output>> {
		self.list_remotes(arg).boxed()
	}

	fn try_get_remote<'a>(
		&'a self,
		name: &'a str,
	) -> BoxFuture<'a, tg::Result<Option<tg::remote::get::Output>>> {
		self.try_get_remote(name).boxed()
	}

	fn put_remote<'a>(
		&'a self,
		name: &'a str,
		arg: tg::remote::put::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.put_remote(name, arg).boxed()
	}

	fn delete_remote<'a>(&'a self, name: &'a str) -> BoxFuture<'a, tg::Result<()>> {
		self.delete_remote(name).boxed()
	}
}
