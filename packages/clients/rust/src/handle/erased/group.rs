use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Group: Send + Sync + 'static {
	fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::group::create::Output>>;

	fn list_groups(
		&self,
		arg: tg::group::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::group::list::Output>>;

	fn try_get_group<'a>(&'a self, group: &'a str) -> BoxFuture<'a, tg::Result<Option<tg::Group>>>;

	fn try_delete_group<'a>(&'a self, group: &'a str) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_group_namespace_grants<'a>(
		&'a self,
		group: &'a str,
	) -> BoxFuture<'a, tg::Result<Option<tg::group::grants::Output>>>;

	fn list_group_members<'a>(
		&'a self,
		group: &'a str,
	) -> BoxFuture<'a, tg::Result<tg::group::member::list::Output>>;

	fn add_group_member<'a>(
		&'a self,
		group: &'a str,
		user: &'a str,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn remove_group_member<'a>(
		&'a self,
		group: &'a str,
		user: &'a str,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;
}

impl<T> Group for T
where
	T: tg::handle::Group,
{
	fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::group::create::Output>> {
		self.create_group(arg).boxed()
	}

	fn list_groups(
		&self,
		arg: tg::group::list::Arg,
	) -> BoxFuture<'_, tg::Result<tg::group::list::Output>> {
		self.list_groups(arg).boxed()
	}

	fn try_get_group<'a>(&'a self, group: &'a str) -> BoxFuture<'a, tg::Result<Option<tg::Group>>> {
		self.try_get_group(group).boxed()
	}

	fn try_delete_group<'a>(&'a self, group: &'a str) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_group(group).boxed()
	}

	fn list_group_namespace_grants<'a>(
		&'a self,
		group: &'a str,
	) -> BoxFuture<'a, tg::Result<Option<tg::group::grants::Output>>> {
		self.list_group_namespace_grants(group).boxed()
	}

	fn list_group_members<'a>(
		&'a self,
		group: &'a str,
	) -> BoxFuture<'a, tg::Result<tg::group::member::list::Output>> {
		self.list_group_members(group).boxed()
	}

	fn add_group_member<'a>(
		&'a self,
		group: &'a str,
		user: &'a str,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.add_group_member(group, user).boxed()
	}

	fn remove_group_member<'a>(
		&'a self,
		group: &'a str,
		user: &'a str,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.remove_group_member(group, user).boxed()
	}
}
