use {crate::Server, tangram_client::prelude::*};

impl tg::handle::Group for Server {
	async fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		self.session(&self.context).create_group(arg).await
	}

	async fn list_groups(&self, arg: tg::group::list::Arg) -> tg::Result<tg::group::list::Output> {
		self.session(&self.context).list_groups(arg).await
	}

	async fn try_get_group(&self, group: &str) -> tg::Result<Option<tg::Group>> {
		self.session(&self.context).try_get_group(group).await
	}

	async fn try_delete_group(&self, group: &str) -> tg::Result<Option<()>> {
		self.session(&self.context).try_delete_group(group).await
	}

	async fn list_group_namespace_grants(
		&self,
		group: &str,
		arg: tg::group::grants::Arg,
	) -> tg::Result<Option<tg::group::grants::Output>> {
		self.session(&self.context)
			.list_group_namespace_grants(group, arg)
			.await
	}

	async fn list_group_members(
		&self,
		group: &str,
		arg: tg::group::member::list::Arg,
	) -> tg::Result<tg::group::member::list::Output> {
		self.session(&self.context)
			.list_group_members(group, arg)
			.await?
			.ok_or_else(|| tg::error!("failed to find the group"))
	}

	async fn add_group_member(&self, group: &str, user: &str) -> tg::Result<()> {
		self.session(&self.context)
			.add_group_member(group, user)
			.await
	}

	async fn remove_group_member(&self, group: &str, user: &str) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.remove_group_member(group, user)
			.await
	}
}
