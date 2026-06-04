use {crate::Session, tangram_client::prelude::*};

impl tg::handle::Group for Session {
	async fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		self.create_group(arg).await
	}

	async fn list_groups(&self, arg: tg::group::list::Arg) -> tg::Result<tg::group::list::Output> {
		self.list_groups(arg).await
	}

	async fn try_get_group(&self, group: &tg::group::Selector) -> tg::Result<Option<tg::Group>> {
		self.try_get_group(group).await
	}

	async fn try_delete_group(&self, group: &tg::group::Selector) -> tg::Result<Option<()>> {
		self.try_delete_group(group).await
	}

	async fn try_get_group_grants(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::grants::Arg,
	) -> tg::Result<Option<tg::group::grants::Output>> {
		self.try_get_group_grants(group, arg).await
	}

	async fn list_group_members(
		&self,
		group: &tg::group::Selector,
	) -> tg::Result<tg::group::members::list::Output> {
		self.list_group_members(group).await
	}

	async fn add_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<()> {
		self.add_group_member(group, member).await
	}

	async fn remove_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<Option<()>> {
		self.remove_group_member(group, member).await
	}
}
