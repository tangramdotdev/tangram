use crate::prelude::*;

impl tg::handle::Namespace for tg::Session {
	fn try_get_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<Option<tg::namespace::get::Output>>> {
		self.try_get_namespace(namespace)
	}

	fn create_namespace(&self, namespace: &tg::Namespace) -> impl Future<Output = tg::Result<()>> {
		self.create_namespace(namespace)
	}

	fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> impl Future<Output = tg::Result<tg::Grant>> {
		self.create_namespace_grant(arg)
	}

	fn list_namespace_grants(
		&self,
		arg: tg::namespace::grants::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::namespace::grants::list::Output>>> {
		self.list_namespace_grants(arg)
	}

	fn delete_namespace_grant(
		&self,
		arg: tg::namespace::grants::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.delete_namespace_grant(arg)
	}

	fn try_delete_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_namespace(namespace)
	}
}
