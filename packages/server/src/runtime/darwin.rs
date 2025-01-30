use crate::Server;
use tangram_client as tg;

#[derive(Clone)]
pub struct Runtime {
	pub(crate) server: Server,
}

impl Runtime {
	pub fn new(server: &Server) -> Self {
		Self {
			server: server.clone(),
		}
	}

	pub async fn run(&self, process: &tg::Process) -> super::Output {
		let (error, exit, value) = match self.run_inner(process).await {
			Ok((exit, value)) => (None, exit, value),
			Err(error) => (Some(error), None, None),
		};
		super::Output { error, exit, output: value }
	}

	pub async fn run_inner(
		&self,
		process: &tg::Process,
	) -> tg::Result<(Option<tg::process::Exit>, Option<tg::Value>)> {
		todo!()
	}
}
