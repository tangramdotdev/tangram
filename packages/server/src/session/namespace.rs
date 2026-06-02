use {crate::Session, tangram_client::prelude::*};

impl tg::handle::Namespace for Session {
	async fn try_get_namespace(
		&self,
		namespace: &tg::Namespace,
	) -> tg::Result<Option<tg::namespace::get::Output>> {
		// self.try_get_namespace(namespace).await
		Err(tg::error!("todo"))
	}

	async fn create_namespace(&self, namespace: &tg::Namespace) -> tg::Result<()> {
		// self.create_namespace(tg::namespace::create::Arg {
		// 	namespace: namespace.clone(),
		// 	all: false,
		// })
		// .await
		Err(tg::error!("todo"))
	}

	async fn create_namespace_grant(
		&self,
		arg: tg::namespace::grants::create::Arg,
	) -> tg::Result<tg::Grant> {
		// self.create_namespace_grant(arg).await
		Err(tg::error!("todo"))
	}

	async fn list_namespace_grants(
		&self,
		arg: tg::namespace::grants::list::Arg,
	) -> tg::Result<Option<tg::namespace::grants::list::Output>> {
		// self.list_namespace_grants(arg).await
		Err(tg::error!("todo"))
	}

	async fn delete_namespace_grant(
		&self,
		arg: tg::namespace::grants::delete::Arg,
	) -> tg::Result<Option<()>> {
		// self.delete_namespace_grant(arg).await
		Err(tg::error!("todo"))
	}

	async fn try_delete_namespace(&self, namespace: &tg::Namespace) -> tg::Result<Option<()>> {
		// self.try_delete_namespace(namespace).await
		Err(tg::error!("todo"))
	}
}
