use crate::prelude::*;

impl tg::handle::Grant for tg::Session {
	fn create_grant(
		&self,
		arg: tg::grant::create::Arg,
	) -> impl Future<Output = tg::Result<tg::Grant>> {
		self.create_grant(arg)
	}

	fn delete_grant(
		&self,
		arg: tg::grant::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.delete_grant(arg)
	}
}
