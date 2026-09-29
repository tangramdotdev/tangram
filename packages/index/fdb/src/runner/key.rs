use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	RunnerSandbox {
		runner: tg::runner::Id,
		sandbox: tg::sandbox::Id,
	},
}
