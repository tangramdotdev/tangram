use crate::prelude::*;

impl tg::instance::Remote for tg::Session {
	fn list_remotes(
		&self,
		arg: tg::remote::list::Arg,
	) -> impl Future<Output = tg::Result<tg::remote::list::Output>> {
		self.list_remotes(arg)
	}

	fn try_get_remote(
		&self,
		remote: &tg::remote::Selector,
		arg: tg::remote::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::remote::get::Output>>> {
		self.try_get_remote(remote, arg)
	}

	fn put_remote(
		&self,
		remote: &tg::remote::Selector,
		arg: tg::remote::put::Arg,
	) -> impl Future<Output = tg::Result<()>> {
		self.put_remote(remote, arg)
	}

	fn try_delete_remote(
		&self,
		remote: &tg::remote::Selector,
		arg: tg::remote::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_remote(remote, arg)
	}
}
