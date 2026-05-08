use {crate::Session, tangram_client::prelude::*};

impl tg::handle::Namespace for Session {
	async fn try_get_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<tg::namespace::get::Output>> {
		self.try_get_namespace(namespace).await
	}

	async fn create_namespace(&self, namespace: &tg::Namespace) -> tg::Result<()> {
		self.create_namespace(namespace).await
	}

	async fn try_delete_namespace(&self, namespace: &tg::Namespace) -> tg::Result<Option<()>> {
		self.try_delete_namespace(namespace).await
	}
}
