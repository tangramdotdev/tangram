use tangram_client::prelude::*;

/// A put of the request of a boot that a scheduler accepted, which the scheduler recovers from the index if it restarts before the sandbox's runner connects.
#[derive(Clone, Debug, tangram_serialize::Deserialize, tangram_serialize::Serialize)]
pub struct Arg {
	#[tangram_serialize(id = 0)]
	pub boot: u64,

	#[tangram_serialize(id = 1)]
	pub request: Vec<u8>,

	#[tangram_serialize(id = 2)]
	pub sandbox: tg::sandbox::Id,

	#[tangram_serialize(id = 3)]
	pub scheduler: tg::scheduler::Id,
}

impl Arg {
	/// Whether the sandbox's scheduler record and the request in the scheduler's queue are what this put produces.
	#[must_use]
	pub fn is_applied(
		&self,
		record: Option<&super::SandboxScheduler>,
		request: Option<&[u8]>,
	) -> bool {
		record.is_some_and(|record| {
			record.boot == self.boot && record.scheduler.as_ref() == Some(&self.scheduler)
		}) && request == Some(self.request.as_slice())
	}
}
