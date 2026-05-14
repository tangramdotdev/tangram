use {crate::prelude::*, futures::FutureExt as _};

impl<L, R> tg::handle::Namespace for tg::Either<L, R>
where
	L: tg::handle::Namespace,
	R: tg::handle::Namespace,
{
	fn try_get_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<Option<tg::namespace::get::Output>>> {
		match self {
			tg::Either::Left(s) => s.try_get_namespace(namespace).left_future(),
			tg::Either::Right(s) => s.try_get_namespace(namespace).right_future(),
		}
	}

	fn create_namespace(&self, namespace: &tg::Namespace) -> impl Future<Output = tg::Result<()>> {
		match self {
			tg::Either::Left(s) => s.create_namespace(namespace).left_future(),
			tg::Either::Right(s) => s.create_namespace(namespace).right_future(),
		}
	}

	fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> impl Future<Output = tg::Result<tg::Grant>> {
		match self {
			tg::Either::Left(s) => s.create_namespace_grant(arg).left_future(),
			tg::Either::Right(s) => s.create_namespace_grant(arg).right_future(),
		}
	}

	fn list_namespace_grants(
		&self,
		arg: tg::namespace::grants::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::namespace::grants::list::Output>>> {
		match self {
			tg::Either::Left(s) => s.list_namespace_grants(arg).left_future(),
			tg::Either::Right(s) => s.list_namespace_grants(arg).right_future(),
		}
	}

	fn delete_namespace_grant(
		&self,
		arg: tg::namespace::grants::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.delete_namespace_grant(arg).left_future(),
			tg::Either::Right(s) => s.delete_namespace_grant(arg).right_future(),
		}
	}

	fn try_delete_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		match self {
			tg::Either::Left(s) => s.try_delete_namespace(namespace).left_future(),
			tg::Either::Right(s) => s.try_delete_namespace(namespace).right_future(),
		}
	}
}
