use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Group: Send + Sync + 'static {
	fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::group::create::Output>>;

	fn try_get_group<'a>(
		&'a self,
		group: &'a tg::group::Selector,
		arg: tg::group::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::Group>>>;

	fn try_delete_group<'a>(
		&'a self,
		group: &'a tg::group::Selector,
		arg: tg::group::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn list_group_members<'a>(
		&'a self,
		group: &'a tg::group::Selector,
		arg: tg::group::members::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::group::members::list::Output>>;

	fn add_group_member<'a>(
		&'a self,
		group: &'a tg::group::Selector,
		arg: tg::group::members::add::Arg,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn remove_group_member<'a>(
		&'a self,
		group: &'a tg::group::Selector,
		member: &'a tg::group::Member,
		arg: tg::group::members::remove::Arg,
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

	fn try_get_group<'a>(
		&'a self,
		group: &'a tg::group::Selector,
		arg: tg::group::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::Group>>> {
		self.try_get_group(group, arg).boxed()
	}

	fn try_delete_group<'a>(
		&'a self,
		group: &'a tg::group::Selector,
		arg: tg::group::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_group(group, arg).boxed()
	}

	fn list_group_members<'a>(
		&'a self,
		group: &'a tg::group::Selector,
		arg: tg::group::members::list::Arg,
	) -> BoxFuture<'a, tg::Result<tg::group::members::list::Output>> {
		self.list_group_members(group, arg).boxed()
	}

	fn add_group_member<'a>(
		&'a self,
		group: &'a tg::group::Selector,
		arg: tg::group::members::add::Arg,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.add_group_member(group, arg).boxed()
	}

	fn remove_group_member<'a>(
		&'a self,
		group: &'a tg::group::Selector,
		member: &'a tg::group::Member,
		arg: tg::group::members::remove::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.remove_group_member(group, member, arg).boxed()
	}
}
