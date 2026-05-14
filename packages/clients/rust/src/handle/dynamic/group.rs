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

	fn try_get_group(&self, group: &str) -> impl Future<Output = tg::Result<Option<tg::Group>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_group(group)) }
	}

	fn try_delete_group(&self, group: &str) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_delete_group(group)) }
	}

	fn list_group_namespace_grants(
		&self,
		group: &str,
		arg: tg::group::grants::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::group::grants::Output>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(
				self.0.list_group_namespace_grants(group, arg),
			)
		}
	}

	fn list_group_members(
		&self,
		group: &str,
		arg: tg::group::member::list::Arg,
	) -> impl Future<Output = tg::Result<tg::group::member::list::Output>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.list_group_members(group, arg)) }
	}

	fn add_group_member(&self, group: &str, user: &str) -> impl Future<Output = tg::Result<()>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.add_group_member(group, user)) }
	}

	fn remove_group_member(
		&self,
		group: &str,
		user: &str,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe {
			std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.remove_group_member(group, user))
		}
	}
}
