use crate::prelude::*;

pub trait Group: Clone + Unpin + Send + Sync + 'static {
	fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> impl Future<Output = tg::Result<tg::group::create::Output>> + Send;

	fn list_groups(
		&self,
		arg: tg::group::list::Arg,
	) -> impl Future<Output = tg::Result<tg::group::list::Output>> + Send;

	fn try_get_group(
		&self,
		group: &tg::group::Selector,
	) -> impl Future<Output = tg::Result<Option<tg::Group>>> + Send;

	fn delete_group(
		&self,
		group: &tg::group::Selector,
	) -> impl Future<Output = tg::Result<()>> + Send {
		async move {
			self.try_delete_group(group)
				.await?
				.ok_or_else(|| tg::error!("failed to find the group"))
		}
	}

	fn try_delete_group(
		&self,
		group: &tg::group::Selector,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;

	fn try_get_group_grants(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::group::grants::Output>>> + Send;

	fn list_group_members(
		&self,
		group: &tg::group::Selector,
	) -> impl Future<Output = tg::Result<tg::group::members::list::Output>> + Send;

	fn add_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn remove_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> impl Future<Output = tg::Result<Option<()>>> + Send;
}

impl tg::handle::Group for tg::Client {
	async fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> tg::Result<tg::group::create::Output> {
		self.session(&self.context).create_group(arg).await
	}

	async fn list_groups(&self, arg: tg::group::list::Arg) -> tg::Result<tg::group::list::Output> {
		self.session(&self.context).list_groups(arg).await
	}

	async fn try_get_group(&self, group: &tg::group::Selector) -> tg::Result<Option<tg::Group>> {
		self.session(&self.context).try_get_group(group).await
	}

	async fn try_delete_group(&self, group: &tg::group::Selector) -> tg::Result<Option<()>> {
		self.session(&self.context).try_delete_group(group).await
	}

	async fn try_get_group_grants(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::grants::Arg,
	) -> tg::Result<Option<tg::group::grants::Output>> {
		self.session(&self.context)
			.try_get_group_grants(group, arg)
			.await
	}

	async fn list_group_members(
		&self,
		group: &tg::group::Selector,
	) -> tg::Result<tg::group::members::list::Output> {
		self.session(&self.context).list_group_members(group).await
	}

	async fn add_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<()> {
		self.session(&self.context)
			.add_group_member(group, member)
			.await
	}

	async fn remove_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
	) -> tg::Result<Option<()>> {
		self.session(&self.context)
			.remove_group_member(group, member)
			.await
	}
}
