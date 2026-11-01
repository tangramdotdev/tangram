use crate::prelude::*;

impl tg::instance::Grant for tg::Session {
	fn grant(
		&self,
		arg: tg::grant::grant::Arg,
	) -> impl Future<Output = tg::Result<tg::grant::grant::Output>> {
		self.grant(arg)
	}

	fn revoke(&self, arg: tg::grant::revoke::Arg) -> impl Future<Output = tg::Result<()>> {
		self.revoke(arg)
	}

	fn try_delete_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.try_delete_grant(grant, arg)
	}

	fn try_get_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::get::Output>>> {
		self.try_get_grant(grant, arg)
	}

	fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::list::Output>>> {
		self.list_grants(arg)
	}
}
