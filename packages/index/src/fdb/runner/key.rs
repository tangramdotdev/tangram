use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Runner(tg::runner::Id),
	RunnerSandbox {
		runner: tg::runner::Id,
		sandbox: tg::sandbox::Id,
	},
	RunnerScheduler {
		runner: tg::runner::Id,
		scheduler: tg::scheduler::Id,
	},
	SchedulerRunner {
		scheduler: tg::scheduler::Id,
		runner: tg::runner::Id,
	},
}
