use {super::Instance, crate::prelude::*};

impl tg::instance::Grant for Instance {
	fn create_grant(
		&self,
		arg: tg::grant::create::Arg,
	) -> impl Future<Output = tg::Result<tg::grant::create::Output>> {
		self.0.create_grant(arg)
	}

	fn delete_grant(
		&self,
		arg: tg::grant::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.0.delete_grant(arg)
	}

	fn list_grants(
		&self,
		arg: tg::grant::list::Arg,
	) -> impl Future<Output = tg::Result<Option<tg::grant::list::Output>>> {
		self.0.list_grants(arg)
	}
}
