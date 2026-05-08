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

	fn try_delete_namespace<'a>(
		&'a self,
		namespace: &'a tg::Namespace,
	) -> BoxFuture<'a, tg::Result<Option<()>>> {
		self.try_delete_namespace(namespace).boxed()
	}
}
