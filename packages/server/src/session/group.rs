use {crate::Session, tangram_client::prelude::*};

impl tg::handle::Group for Session {
	async fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		// self.create_group(arg).await
		Err(tg::error!("todo"))
	}

	async fn list_groups(&self, arg: tg::group::list::Arg) -> tg::Result<tg::group::list::Output> {
		// self.list_groups(arg).await
		Err(tg::error!("todo"))
	}

	async fn try_get_group(&self, group: &str) -> tg::Result<Option<tg::Group>> {
		// self.try_get_group(group).await
		Err(tg::error!("todo"))
	}

	async fn try_delete_group(&self, group: &str) -> tg::Result<Option<()>> {
		// self.try_delete_group(group).await
		Err(tg::error!("todo"))
	}

	async fn list_group_namespace_grants(
		&self,
		arg: tg::group::grants::Arg,
	) -> tg::Result<Option<tg::group::grants::Output>> {
		// self.list_group_namespace_grants(arg).await
		Err(tg::error!("todo"))
	}

	async fn list_group_members(&self, group: &str) -> tg::Result<tg::group::member::list::Output> {
		// self.list_group_members(group)
		// 	.await?
		// 	.ok_or_else(|| tg::error!("failed to find the group"))
		Err(tg::error!("todo"))
	}

	async fn add_group_member(&self, group: &str, user: &str) -> tg::Result<()> {
		// self.add_group_member(group, user).await
		Err(tg::error!("todo"))
	}

	async fn remove_group_member(&self, group: &str, user: &str) -> tg::Result<Option<()>> {
		// self.remove_group_member(group, user).await
		Err(tg::error!("todo"))
	}
}
