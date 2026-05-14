use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::Namespace for Handle {
	fn try_get_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<Option<tg::namespace::get::Output>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_namespace(namespace)) }
	}

	fn create_namespace(&self, namespace: &tg::Namespace) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.create_namespace(namespace)) }
	}

	fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> impl Future<Output = tg::Result<tg::Grant>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.create_namespace_grant(arg)) }
	}

	fn list_namespace_grants(
		&self,
		arg: tg::namespace::grants::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::namespace::grants::list::Output>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.list_namespace_grants(arg)) }
	}

	fn delete_namespace_grant(
		&self,
		arg: tg::namespace::grants::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.delete_namespace_grant(arg)) }
	}

	fn try_delete_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_delete_namespace(namespace))
		}
	}
}
