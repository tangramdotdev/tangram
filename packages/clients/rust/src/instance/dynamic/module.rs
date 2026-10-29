use {super::Instance, crate::prelude::*};

impl tg::instance::Module for Instance {
	fn resolve_module(
		&self,
		arg: tg::module::resolve::Arg,
	) -> impl Future<Output = tg::Result<tg::module::resolve::Output>> {
		self.0.resolve_module(arg)
	}

	fn load_module(
		&self,
		arg: tg::module::load::Arg,
	) -> impl Future<Output = tg::Result<tg::module::load::Output>> {
		self.0.load_module(arg)
	}

	fn module_interface(
		&self,
		arg: tg::module::interface::Arg,
	) -> impl Future<Output = tg::Result<tg::module::interface::Output>> {
		self.0.module_interface(arg)
	}
}
