use {super::Instance, crate::prelude::*, futures::future::BoxFuture};

impl tg::instance::Grant for Instance {
	fn grant(
		&self,
		arg: tg::grant::grant::Arg,
	) -> impl Future<Output = tg::Result<tg::grant::grant::Output>> {
		self.0.grant(arg)
	}

	fn revoke(&self, arg: tg::grant::revoke::Arg) -> impl Future<Output = tg::Result<()>> {
		self.0.revoke(arg)
	}

	fn try_delete_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_delete_grant(grant, arg)) }
	}

	fn try_get_grant(
		&self,
		grant: &tg::grant::Id,
		arg: tg::grant::get::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::get::Output>>> {
		unsafe { std::mem::transmute::<_, BoxFuture<'_, _>>(self.0.try_get_grant(grant, arg)) }
	}

	fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::list::Output>>> {
		self.0.list_grants(arg)
	}
}
