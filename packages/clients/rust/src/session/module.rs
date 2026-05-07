use crate::prelude::*;

impl tg::handle::Module for tg::Session {
	fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> impl Future<Output = tg::Result<tg::module::resolve::Output>> {
		self.resolve_module(arg)
	}

	fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> impl Future<Output = tg::Result<tg::module::load::Output>> {
		self.load_module(arg)
	}
}
