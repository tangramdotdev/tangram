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

	fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> impl Future<Output = tg::Result<tg::Grant>> + Send;

	fn list_namespace_grants(
		&self,
		arg: tg::namespace::grants::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::namespace::grants::list::Output>>> + Send;

	fn delete_namespace_grant(
		&self,
		arg: tg::namespace::grants::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

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

	async fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> tg::Result<tg::Grant> {
		self.session(&self.context)
			.create_namespace_grant(arg)
			.await
	}

	async fn list_namespace_grants(
		&self,
		arg: tg::namespace::grants::list::Arg,
	) -> tg::Result<Option<tg::namespace::grants::list::Output>> {
		self.session(&self.context).list_namespace_grants(arg).await
	}

	async fn delete_namespace_grant(
		&self,
		arg: tg::namespace::grants::delete::Arg,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.delete_namespace_grant(arg)
			.await
	}

	async fn try_delete_namespace(&self, namespace: &tg::Namespace) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.try_delete_namespace(namespace)
			.await
	}
}
