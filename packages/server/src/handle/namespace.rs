use {crate::Server, tangram_client::prelude::*};

impl tg::handle::Namespace for Server {
	async fn try_get_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<tg::namespace::get::Output>> {
		self.session(&self.context)
			.try_get_namespace(namespace)
			.await
	}

	async fn create_namespace(&self, namespace: &tg::Namespace) -> tg::Result<()> {
		self.session(&self.context)
			.create_namespace(namespace)
			.await
	}

	async fn try_delete_namespace(&self, namespace: &tg::Namespace) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_delete_namespace(namespace)
			.await
	}
}
