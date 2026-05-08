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

	fn try_delete_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_namespace(namespace)
	}
}
