use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Runner(tg::runner::Id),
	RunnerSandbox {
		runner: tg::runner::Id,
		sandbox: tg::sandbox::Id,
	},
	SchedulerRunner {
		runner: tg::runner::Id,
		scheduler: tg::scheduler::Id,
	},
	SandboxScheduler {
		sandbox: tg::sandbox::Id,
	},
	SchedulerSandbox {
		sandbox: tg::sandbox::Id,
		scheduler: tg::scheduler::Id,
	},
}
