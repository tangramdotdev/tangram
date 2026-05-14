use {
	crate::prelude::*,
	futures::{future::BoxFuture, prelude::*},
};

pub trait Namespace: Send + Sync + 'static {
	fn try_get_namespace<'a>(
		&'a self,
		namespace: &'a tg::Namespace,
	) -> BoxFuture<'a, tg::Result<Option<tg::namespace::get::Output>>>;

	fn create_namespace<'a>(
		&'a self,
		namespace: &'a tg::Namespace,
	) -> BoxFuture<'a, tg::Result<()>>;

	fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::Grant>>;

	fn list_namespace_grants(
		&self,
		arg: tg::namespace::grants::list::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::namespace::grants::list::Output>>>;

	fn delete_namespace_grant(
		&self,
		arg: tg::namespace::grants::delete::Arg,
	) -> BoxFuture<'_, tg::Result<Option<()>>>;

	fn try_delete_namespace<'a>(
		&'a self,
		namespace: &'a tg::Namespace,
	) -> BoxFuture<'a, tg::Result<Option<()>>>;
}

impl<T> Namespace for T
where
	T: tg::handle::Namespace,
{
	fn try_get_namespace<'a>(
		&'a self,
		namespace: &'a tg::Namespace,
	) -> BoxFuture<'a, tg::Result<Option<tg::namespace::get::Output>>> {
		self.try_get_namespace(namespace).boxed()
	}

	fn create_namespace<'a>(
		&'a self,
		namespace: &'a tg::Namespace,
	) -> BoxFuture<'a, tg::Result<()>> {
		self.create_namespace(namespace).boxed()
	}

	fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> BoxFuture<'_, tg::Result<tg::Grant>> {
		self.create_namespace_grant(arg).boxed()
	}

	fn list_namespace_grants(
		&self,
		arg: tg::namespace::grants::list::Arg,
	) -> BoxFuture<'_, tg::Result<Option<tg::namespace::grants::list::Output>>> {
		self.list_namespace_grants(arg).boxed()
	}

	fn delete_namespace_grant(
		&self,
		arg: tg::namespace::grants::delete::Arg,
	) -> BoxFuture<'_, tg::Result<Option<()>>> {
		self.delete_namespace_grant(arg).boxed()
	}

	fn try_delete_namespace<'a>(
		&'a self,
		namespace: &'a tg::Namespace,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_namespace(namespace).boxed()
	}
}
