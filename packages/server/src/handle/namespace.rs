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
			.create_namespace(tg::namespace::create::Arg {
				namespace: namespace.clone(),
				all: false,
			})
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
