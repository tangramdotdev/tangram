use {super::Handle, crate::prelude::*, futures::future::BoxFuture};

impl tg::handle::Group for Handle {
	fn create_group(
		&self,
		arg: tg::group::create::Arg,
	) -> impl Future<Output = tg::Result<tg::group::create::Output>> {
		self.0.create_group(arg)
	}

	fn list_groups(
		&self,
		arg: tg::group::list::Arg,
	) -> impl Future<Output = tg::Result<tg::group::list::Output>> {
		self.0.list_groups(arg)
	}

	fn try_get_group(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::Group>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_group(group, arg)) }
	}

	fn try_delete_group(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_delete_group(group, arg)) }
	}

	fn try_get_group_grants(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::group::grants::Output>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_group_grants(group, arg))
		}
	}

	fn list_group_members(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::members::list::Arg,
	) -> impl Future<Output = tg::Result<tg::group::members::list::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.list_group_members(group, arg)) }
	}

	fn add_group_member(
		&self,
		group: &tg::group::Selector,
		arg: tg::group::members::add::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.add_group_member(group, arg)) }
	}

	fn remove_group_member(
		&self,
		group: &tg::group::Selector,
		member: &tg::group::Member,
		arg: tg::group::members::remove::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.remove_group_member(group, member, arg),
			)
		}
	}
}
