use {
	dashmap::DashMap, indexmap::IndexMap, std::collections::BTreeSet, tangram_client::prelude::*,
};

pub mod control;
pub mod put;

pub type IndexTask = tangram_futures::task::Shared<tg::Result<()>>;

pub type Map = DashMap<tg::process::Id, tg::sandbox::Id, tg::id::BuildHasher>;

pub type Processes = DashMap<tg::process::Id, State, tg::id::BuildHasher>;

pub struct Child {
	pub data: tg::process::data::Child,
	pub lease: Option<tg::process::Lease>,
	pub location: Option<tg::location::Arg>,
}

pub struct State {
	/// Whether the home instance has acknowledged the process's start, after which the process's ID may be exposed to the home instance.
	pub acknowledged: bool,

	pub changed: tokio::sync::watch::Sender<()>,

	pub children: IndexMap<tg::process::Id, Child, tg::id::BuildHasher>,

	pub control: tg::process::control::Sender,

	pub control_sender: control::local::Local,

	pub data: tg::process::Data,

	pub finish: Option<tg::process::control::FinishServerRequestArg>,

	pub index_task: IndexTask,

	pub inner_token: tokio::sync::watch::Receiver<Option<String>>,

	pub leases: BTreeSet<tg::process::Lease>,

	pub parent: Option<tg::process::Id>,

	pub process: Option<tangram_sandbox::Process>,

	pub stopper: tangram_futures::task::Stopper,
}

impl State {
	/// Get the process's data, with every child, for the runner's own use.
	#[must_use]
	pub fn data(&self) -> tg::process::Data {
		let mut data = self.data.clone();
		data.children = Some(
			self.children
				.values()
				.map(|child| child.data.clone())
				.collect(),
		);

		data
	}
}
