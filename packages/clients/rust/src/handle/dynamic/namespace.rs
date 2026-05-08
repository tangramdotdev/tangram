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

	fn try_delete_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_delete_namespace(namespace))
		}
	}
}
