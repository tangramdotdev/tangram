use {
	super::process::ConnectedEvent,
	dashmap::DashMap,
	indexmap::IndexMap,
	std::{
		collections::{BTreeMap, BTreeSet},
		sync::Arc,
	},
	tangram_client::prelude::*,
};

pub struct SpawnSandboxTaskArg {
	pub allocation: crate::capacity::Allocation,
	pub arg: tg::sandbox::create::Arg,
	pub boot: u64,
	pub creator: tg::Principal,
	pub id: Option<tg::sandbox::Id>,
	pub location: tg::Location,
	pub owner: tg::Principal,
	pub process: Option<tg::runner::control::Process>,

	/// Whether the spawn made the process public, which the runner carries to the process's home in the start of a process whose ID it derives.
	pub public: bool,

	/// Whether a scheduler offered the boot, in which case the scheduler sends the spawns into the sandbox, and otherwise the runner's server created the sandbox on the shortcut path.
	pub scheduled: bool,

	/// The sequence of the runner's first write of the boot, which follows the writes of the boot's scheduler.
	pub sequence: u64,

	pub token: Option<String>,
}

#[must_use]
pub struct SpawnSandboxTaskOutput {
	pub events: tokio::sync::mpsc::UnboundedReceiver<tg::Result<Event>>,
}

pub enum Event {
	Destroyed,
	Ready(ReadyEvent),
}

#[derive(Clone, Debug)]
pub struct ReadyEvent {
	pub connected_event: Option<ConnectedEvent>,
	pub sandbox: tg::sandbox::Id,
}

#[derive(Default)]
pub struct Sandboxes {
	indexes: DashMap<tg::sandbox::Id, u64, tg::id::BuildHasher>,
	states: DashMap<u64, State, fnv::FnvBuildHasher>,
	updated: tokio::sync::Notify,
}

pub struct State {
	pub allocation: Option<Arc<tokio::sync::Mutex<Option<crate::capacity::Allocation>>>>,
	pub authorization_tokens: tg::authorization::Tokens,
	pub boot: Arc<Boot>,
	pub capacity: tg::runner::Capacity,
	pub changed: tokio::sync::watch::Sender<()>,
	pub control_sender: tangram_server_sandbox_core::control::local::Local,
	pub create: tg::sandbox::create::Arg,
	pub creator: tg::Principal,
	pub error: Option<tg::Either<tg::error::Data, tg::error::Id>>,
	pub expires_at: Option<i64>,
	pub id: tg::sandbox::Id,
	pub location: tg::Location,
	pub owner: tg::Principal,
	pub process_ids: IndexMap<tg::process::Id, (), tg::id::BuildHasher>,
	pub processes: Arc<tangram_server_process_core::Processes>,
	pub sandbox: Option<tangram_sandbox::Sandbox>,
	pub status: tg::sandbox::Status,
	pub stopped_at: Option<i64>,
	pub token: tokio::sync::watch::Receiver<Option<String>>,
	pub tokens: BTreeMap<tg::artifact::Id, tg::authorization::Token>,
	pub usage: Option<tg::sandbox::Usage>,
	pub vfs: Option<std::sync::Arc<dyn tangram_server_vfs_core::Provider>>,
}

/// The boots that the runner holds, by sandbox and boot, from the runner's yes until each boot ends locally.
#[derive(Default)]
pub struct Boots {
	boots: tokio::sync::watch::Sender<BTreeMap<(tg::sandbox::Id, u64), Arc<Boot>>>,
}

/// A boot that the runner holds. The boot numbers every write of the runner for it from its one counter, which gives no number once the runner gave the boot up, remembers every process that it accepted, and counts the finishes of its processes that no server has accepted yet.
pub struct Boot {
	pub boot: u64,

	/// How the runner gave up the boot, once its scheduler ended it.
	given_up: tokio::sync::watch::Sender<Option<Teardown>>,

	inner: std::sync::Mutex<BootInner>,

	/// The proof of a boot on the shortcut path, which the runner reports when it connects to its scheduler.
	pub proof: Option<tg::sandbox::id::Proof>,

	pub sandbox: tg::sandbox::Id,

	/// Whether a scheduler offered the boot, rather than the runner's server creating it on the shortcut path.
	pub scheduled: bool,

	/// The number of finishes of the boot's processes that no server has accepted yet.
	unaccepted: tokio::sync::watch::Sender<usize>,
}

struct BootInner {
	/// Whether the boot took the number of its end, after which the runner does not give it up.
	ending: bool,

	finished: BTreeSet<tg::process::Id>,

	/// The processes that this server spawned into the boot, which this server finishes, rather than the sandbox's scheduler.
	local: BTreeSet<tg::process::Id>,

	processes: Vec<tg::runner::control::Process>,

	/// The number of the boot's next write.
	sequence: u64,
}

/// A finish of a process of a boot that no server has accepted yet, which counts as accepted once it is dropped.
pub struct Unaccepted {
	boot: Arc<Boot>,
}

/// How a boot ends locally, which stops its sandbox or destroys it. The runner gives up a boot that its scheduler ended with a stop if the scheduler keeps the sandbox's directory, and with a destroy otherwise.
#[derive(Clone, Copy, Debug)]
pub enum Teardown {
	Destroy,
	Stop,
}

impl Sandboxes {
	#[must_use]
	pub fn get(&self, index: u64) -> Option<dashmap::mapref::one::Ref<'_, u64, State>> {
		self.states.get(&index)
	}

	#[must_use]
	pub fn get_by_id(
		&self,
		id: &tg::sandbox::Id,
	) -> Option<dashmap::mapref::one::Ref<'_, u64, State>> {
		let index = *self.indexes.get(id)?;

		self.states.get(&index)
	}

	#[must_use]
	pub fn get_mut_by_id(
		&self,
		id: &tg::sandbox::Id,
	) -> Option<dashmap::mapref::one::RefMut<'_, u64, State>> {
		let index = *self.indexes.get(id)?;

		self.states.get_mut(&index)
	}

	pub fn insert(&self, index: u64, state: State) {
		let id = state.id.clone();
		match self.states.entry(index) {
			dashmap::Entry::Occupied(_) => panic!("the sandbox index is already in use"),
			dashmap::Entry::Vacant(entry) => {
				entry.insert(state);
			},
		}
		self.indexes.insert(id, index);
		self.updated.notify_waiters();
	}

	#[must_use]
	pub fn iter(&self) -> dashmap::iter::Iter<'_, u64, State, fnv::FnvBuildHasher> {
		self.states.iter()
	}

	#[must_use]
	pub fn remove(&self, index: u64) -> Option<State> {
		let (_, state) = self.states.remove(&index)?;
		self.indexes
			.remove_if(&state.id, |_, current| *current == index);
		self.updated.notify_waiters();

		Some(state)
	}

	/// Wake the waiters for a change to the sandboxes, such as the end of a boot that was starting.
	pub fn notify_updated(&self) {
		self.updated.notify_waiters();
	}

	/// Get a future that completes when a sandbox's state is stored or removed, or when a boot that was starting ends. Create it before reading the state, so that a change between the read and the wait is not missed.
	pub fn updated(&self) -> tokio::sync::futures::Notified<'_> {
		self.updated.notified()
	}
}

impl State {
	#[must_use]
	pub fn data(&self) -> tg::sandbox::get::Output {
		let id = self.id.clone();
		let arg = &self.create;
		let data = tg::sandbox::Data {
			boot: self.boot.boot,
			cpu: arg.cpu,
			creator: self.creator.clone(),
			error: self.error.clone(),
			expires_at: self.expires_at,
			hostname: arg.hostname.clone(),
			id,
			memory: arg.memory,
			mounts: arg.mounts.clone(),
			network: arg.network.clone(),
			owner: self.owner.clone(),
			status: self.status,
			stopped_at: self.stopped_at,
			storage: arg.storage,
			time_to_live: arg.time_to_live,
			time_to_stop: arg.time_to_stop,
			usage: self.usage.clone(),
		};
		tg::sandbox::get::Output {
			data,
			location: Some(self.location.clone()),
			tokens: self.authorization_tokens.clone(),
		}
	}

	#[must_use]
	pub fn processes(
		&self,
		position: u64,
		length: u64,
	) -> tg::sandbox::control::GetProcessesClientResponseOutput {
		let processes = self
			.process_ids
			.keys()
			.skip(usize::try_from(position).unwrap())
			.take(usize::try_from(length).unwrap())
			.cloned()
			.collect();
		tg::sandbox::control::GetProcessesClientResponseOutput {
			length: u64::try_from(self.process_ids.len()).unwrap(),
			processes,
			status: self.status,
		}
	}
}

impl Boots {
	/// Remember a boot that the runner holds.
	pub fn insert(&self, boot: Arc<Boot>) {
		self.boots.send_modify(|boots| {
			boots.insert((boot.sandbox.clone(), boot.boot), boot);
		});
	}

	/// Forget a boot once it ended locally, which lets a later boot of its sandbox create.
	pub fn remove(&self, sandbox: &tg::sandbox::Id, boot: u64) {
		self.boots.send_modify(|boots| {
			boots.remove(&(sandbox.clone(), boot));
		});
	}

	#[must_use]
	pub fn get(&self, sandbox: &tg::sandbox::Id, boot: u64) -> Option<Arc<Boot>> {
		self.boots.borrow().get(&(sandbox.clone(), boot)).cloned()
	}

	/// Get the sandbox's latest boot that the runner holds.
	#[must_use]
	pub fn latest(&self, sandbox: &tg::sandbox::Id) -> Option<Arc<Boot>> {
		self.boots
			.borrow()
			.range((sandbox.clone(), 0)..=(sandbox.clone(), u64::MAX))
			.next_back()
			.map(|(_, boot)| boot.clone())
	}

	/// Get every boot that the runner holds.
	#[must_use]
	pub fn list(&self) -> Vec<Arc<Boot>> {
		self.boots.borrow().values().cloned().collect()
	}

	/// Wait until every boot of the sandbox before the boot has ended locally, so that the boot creates on the sandbox's directory only after the earlier boots are done with it.
	pub async fn wait_ended_before(&self, sandbox: &tg::sandbox::Id, boot: u64) {
		let mut boots = self.boots.subscribe();
		boots
			.wait_for(|boots| {
				boots
					.range((sandbox.clone(), 0)..(sandbox.clone(), boot))
					.next()
					.is_none()
			})
			.await
			.ok();
	}
}

impl Boot {
	/// Create a boot whose first write the runner numbers with the sequence.
	#[must_use]
	pub fn new(
		boot: u64,
		sandbox: tg::sandbox::Id,
		proof: Option<tg::sandbox::id::Proof>,
		scheduled: bool,
		sequence: u64,
	) -> Self {
		let inner = BootInner {
			ending: false,
			finished: BTreeSet::new(),
			local: BTreeSet::new(),
			processes: Vec::new(),
			sequence,
		};
		Self {
			boot,
			given_up: tokio::sync::watch::Sender::new(None),
			inner: std::sync::Mutex::new(inner),
			proof,
			sandbox,
			scheduled,
			unaccepted: tokio::sync::watch::Sender::new(0),
		}
	}

	/// Get the number of the boot's next write, or `None` once the runner gave the boot up, after which the runner writes nothing more for it.
	pub fn next(&self) -> Option<u64> {
		let mut inner = self.inner.lock().unwrap();
		if self.given_up.borrow().is_some() {
			return None;
		}
		let sequence = inner.sequence;
		inner.sequence += 1;
		Some(sequence)
	}

	/// Get the number of the boot's end, or `None` once the runner gave the boot up. After this, the runner no longer gives the boot up, because its end is on the way to the index.
	pub fn end(&self) -> Option<u64> {
		let mut inner = self.inner.lock().unwrap();
		if self.given_up.borrow().is_some() {
			return None;
		}
		inner.ending = true;
		let sequence = inner.sequence;
		inner.sequence += 1;
		Some(sequence)
	}

	/// Give up the boot because its scheduler ended it, so that the runner writes nothing more for it, unless the boot already took the number of its end or was given up. Return whether the boot was given up now.
	pub fn give_up(&self, kind: Teardown) -> bool {
		let inner = self.inner.lock().unwrap();
		if inner.ending || self.given_up.borrow().is_some() {
			return false;
		}
		self.given_up.send_replace(Some(kind));
		drop(inner);

		true
	}

	/// Get how the runner gave up the boot, if it did.
	#[must_use]
	pub fn given_up(&self) -> Option<Teardown> {
		*self.given_up.borrow()
	}

	/// Wait until the runner gives up the boot, and get how.
	pub async fn wait_given_up(&self) -> Teardown {
		let mut given_up = self.given_up.subscribe();
		let kind = given_up.wait_for(Option::is_some).await.unwrap();
		kind.unwrap()
	}

	/// Remember that the boot accepted a process, and whether this server spawned it.
	pub fn accept(&self, process: tg::runner::control::Process, local: bool) {
		let mut inner = self.inner.lock().unwrap();
		if local && let Some(id) = &process.id {
			inner.local.insert(id.clone());
		}
		inner.processes.push(process);
	}

	/// Remember that a process of the boot finished, or that a finish of it is under way.
	pub fn finish(&self, process: &tg::process::Id) {
		self.inner.lock().unwrap().finished.insert(process.clone());
	}

	/// Get whether this server spawned the process into the boot, in which case this server finishes it, rather than the sandbox's scheduler.
	#[must_use]
	pub fn local(&self, process: &tg::runner::control::Process) -> bool {
		let inner = self.inner.lock().unwrap();
		process
			.id
			.as_ref()
			.is_some_and(|id| inner.local.contains(id))
	}

	/// Get the IDs of the processes that the boot accepted.
	#[must_use]
	pub fn ids(&self) -> Vec<tg::process::Id> {
		let inner = self.inner.lock().unwrap();
		inner
			.processes
			.iter()
			.filter_map(|process| process.id.clone())
			.collect()
	}

	/// Get the processes that the boot accepted, whose IDs are known, and that did not finish.
	#[must_use]
	pub fn unfinished(&self) -> Vec<tg::runner::control::Process> {
		let inner = self.inner.lock().unwrap();
		inner
			.processes
			.iter()
			.filter(|process| {
				process
					.id
					.as_ref()
					.is_some_and(|id| !inner.finished.contains(id))
			})
			.cloned()
			.collect()
	}

	/// Get what the runner reports about the boot when it connects to its scheduler, which is the processes that the scheduler offered into the boot and that did not finish, and the proof of a boot on the shortcut path.
	#[must_use]
	pub fn report(&self) -> (Vec<tg::process::Id>, Option<tg::sandbox::id::Proof>) {
		let processes = self
			.unfinished()
			.into_iter()
			.filter(|process| !self.local(process))
			.filter_map(|process| process.id)
			.collect();

		(processes, self.proof.clone())
	}

	/// Begin a finish of a process of the boot, which counts as unaccepted until the returned value is dropped.
	#[must_use]
	pub fn begin_finish(self: &Arc<Self>) -> Unaccepted {
		self.unaccepted.send_modify(|count| *count += 1);
		Unaccepted { boot: self.clone() }
	}

	/// Wait until a server has accepted every finish of the boot's processes.
	pub async fn wait_finishes(&self) {
		let mut unaccepted = self.unaccepted.subscribe();
		unaccepted.wait_for(|count| *count == 0).await.ok();
	}
}

impl Teardown {
	/// Get the teardown of a boot whose sandbox is left without processes or whose runner shuts down, which destroys a sandbox whose ttl is zero and stops any other.
	#[must_use]
	pub fn with_time_to_live(time_to_live: Option<std::time::Duration>) -> Self {
		if time_to_live.is_some_and(|time_to_live| time_to_live.is_zero()) {
			Self::Destroy
		} else {
			Self::Stop
		}
	}
}

impl<'a> IntoIterator for &'a Sandboxes {
	type Item = dashmap::mapref::multiple::RefMulti<'a, u64, State>;
	type IntoIter = dashmap::iter::Iter<'a, u64, State, fnv::FnvBuildHasher>;

	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}

impl Drop for Unaccepted {
	fn drop(&mut self) {
		self.boot
			.unaccepted
			.send_modify(|count| *count = count.saturating_sub(1));
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn boot(sequence: u64) -> Arc<Boot> {
		let sandbox = tg::sandbox::Id::new();
		Arc::new(Boot::new(0, sandbox, None, true, sequence))
	}

	#[test]
	fn a_boot_numbers_its_writes_in_order_and_gives_no_number_once_given_up() {
		let boot = boot(1);
		assert_eq!(boot.next(), Some(1));
		assert_eq!(boot.next(), Some(2));
		assert!(boot.give_up(Teardown::Stop));
		assert_eq!(boot.next(), None);
		assert_eq!(boot.end(), None);
		assert!(!boot.give_up(Teardown::Destroy));
		assert!(matches!(boot.given_up(), Some(Teardown::Stop)));
	}

	#[test]
	fn a_boot_that_took_the_number_of_its_end_is_not_given_up() {
		let boot = boot(1);
		assert_eq!(boot.end(), Some(1));
		assert!(!boot.give_up(Teardown::Stop));
		assert_eq!(boot.next(), Some(2));
	}

	#[tokio::test]
	async fn a_boot_waits_for_every_finish_that_it_began() {
		let boot = boot(1);
		let first = boot.begin_finish();
		let second = boot.begin_finish();
		let mut wait = std::pin::pin!(boot.wait_finishes());
		assert!(poll(&mut wait).is_pending());
		drop(first);
		assert!(poll(&mut wait).is_pending());
		drop(second);
		assert!(poll(&mut wait).is_ready());
	}

	#[tokio::test]
	async fn a_later_boot_waits_for_the_earlier_boots_of_its_sandbox() {
		let boots = Boots::default();
		let sandbox = tg::sandbox::Id::new();
		let other = tg::sandbox::Id::new();
		boots.insert(Arc::new(Boot::new(0, sandbox.clone(), None, true, 1)));
		boots.insert(Arc::new(Boot::new(0, other.clone(), None, true, 1)));
		boots.insert(Arc::new(Boot::new(1, sandbox.clone(), None, true, 1)));
		assert_eq!(boots.latest(&sandbox).unwrap().boot, 1);
		let mut wait = std::pin::pin!(boots.wait_ended_before(&sandbox, 1));
		assert!(poll(&mut wait).is_pending());
		boots.remove(&other, 0);
		assert!(poll(&mut wait).is_pending());
		boots.remove(&sandbox, 0);
		assert!(poll(&mut wait).is_ready());
	}

	fn poll<F: std::future::Future + Unpin>(future: &mut F) -> std::task::Poll<F::Output> {
		let waker = std::task::Waker::noop();
		let mut context = std::task::Context::from_waker(waker);
		std::pin::Pin::new(future).poll(&mut context)
	}
}
