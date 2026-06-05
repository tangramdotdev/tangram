use {super::Handle, crate::prelude::*};

impl tg::handle::Grant for Handle {
	fn create_grant(
		&self,
		arg: tg::grant::create::Arg,
	) -> impl Future<Output = tg::Result<tg::Grant>> {
		self.0.create_grant(arg)
	}

	fn delete_grant(
		&self,
		arg: tg::grant::delete::Arg,
	) -> impl Future<Output = tg::Result<Option<()>>> {
		self.0.delete_grant(arg)
	}
}
