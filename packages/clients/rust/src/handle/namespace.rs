use crate::prelude::*;

pub trait Namespace: Clone + Unpin + Send + Sync + 'static {
	fn try_get_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<Option<tg::namespace::get::Output>>> + Send;

	fn create_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_delete_namespace(namespace)
				.await?
				.ok_or_else(|| tg::error!("failed to find the namespace"))
		}
	}

	fn try_delete_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;
}

impl tg::handle::Namespace for tg::Client {
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
