use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Grant: Send + Sync + 'static {
	fn grant(
		&self,
		arg: tg::grant::grant::Arg,
	) -> BoxFuture<'_, tg::Result<tg::grant::grant::Output>>;

	fn revoke(&self, arg: tg::grant::revoke::Arg) -> BoxFuture<'_, tg::Result<()>>;

	fn try_delete_grant<'a>(
		&'a self,
		grant: &'a tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;

	fn try_get_grant<'a>(
		&'a self,
		grant: &'a tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::grant::get::Output>>>;

	fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::grant::list::Output>>>;
}

impl<T> Grant for T
where
	T: tg::instance::Grant,
{
	fn grant(
		&self,
		arg: tg::grant::grant::Arg,
	) -> BoxFuture<'_, tg::Result<tg::grant::grant::Output>> {
		self.grant(arg).boxed()
	}

	fn revoke(&self, arg: tg::grant::revoke::Arg) -> BoxFuture<'_, tg::Result<()>> {
		self.revoke(arg).boxed()
	}

	fn try_delete_grant<'a>(
		&'a self,
		grant: &'a tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_grant(grant, arg).boxed()
	}

	fn try_get_grant<'a>(
		&'a self,
		grant: &'a tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> BoxFuture<'a, tg::Result<Option<tg::grant::get::Output>>> {
		self.try_get_grant(grant, arg).boxed()
	}

	fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::grant::list::Output>>> {
		self.list_grants(arg).boxed()
	}
}
