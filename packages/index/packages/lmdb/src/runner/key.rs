use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	RegistrationIndex(tg::runner::Id),
	Registration(tg::runner::Id),
	RunnerSandbox {
		runner: tg::runner::Id,
		sandbox: tg::sandbox::Id,
	},
}
