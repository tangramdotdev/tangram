use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Organization: Send + Sync + 'static {
	fn create_organization(
		&self,
		arg: tg::organization::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::organization::create::Output>>;

	fn try_get_organization<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
	) -> BoxFuture<'a, tg::Result<Option<tg::Organization>>>;

	fn try_delete_organization<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_get_organization_grants<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::grants::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::organization::grants::Output>>>;

	fn list_organization_members<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
	) -> BoxFuture<'a, tg::Result<tg::organization::members::list::Output>>;

	fn add_organization_member<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
		member: &'a tg::organization::Member,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn remove_organization_member<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
		member: &'a tg::organization::Member,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;
}

impl<T> Organization for T
where
	T: tg::handle::Organization,
{
	fn create_organization(
		&self,
		arg: tg::organization::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::organization::create::Output>> {
		self.create_organization(arg).boxed()
	}

	fn try_get_organization<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
	) -> BoxFuture<'a, tg::Result<Option<tg::Organization>>> {
		self.try_get_organization(organization).boxed()
	}

	fn try_delete_organization<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_organization(organization).boxed()
	}

	fn try_get_organization_grants<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
		arg: tg::organization::grants::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::organization::grants::Output>>> {
		self.try_get_organization_grants(organization, arg).boxed()
	}

	fn list_organization_members<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
	) -> BoxFuture<'a, tg::Result<tg::organization::members::list::Output>> {
		self.list_organization_members(organization).boxed()
	}

	fn add_organization_member<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
		member: &'a tg::organization::Member,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.add_organization_member(organization, member).boxed()
	}

	fn remove_organization_member<'a>(
		&'a self,
		organization: &'a tg::organization::Selector,
		member: &'a tg::organization::Member,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.remove_organization_member(organization, member)
			.boxed()
	}
}
