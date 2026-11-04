use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Attempt(tg::runner::Id),
	Registration(tg::runner::Id),
	RunnerSandbox {
		runner: tg::runner::Id,
		sandbox: tg::sandbox::Id,
	},
}
