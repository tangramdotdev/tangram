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

	/// Whether an earlier boot of the sandbox ran on this runner, so that a start without the sandbox's directory fails.
	pub placed: bool,

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

/// A boot that the runner holds. The boot numbers every write of the runner for it from its one counter, which gives no number once the runner gave the boot up, remembers every process that it accepted, and tracks the finish of each of its processes until a server answers it.
pub struct Boot {
	pub boot: u64,

	/// A change to the finishes of the boot's processes, which wakes the wait for every finish.
	changed: tokio::sync::watch::Sender<()>,

	/// The number of the runner's first write of the boot.
	first: u64,

	/// How the runner gave up the boot, once its scheduler ended it.
	abandoned: tokio::sync::watch::Sender<Option<Teardown>>,

	inner: std::sync::Mutex<BootInner>,

	/// The location of the sandbox's home.
	pub location: tg::Location,

	/// The proof of a boot on the shortcut path, which the runner reports when it connects to its scheduler.
	pub proof: Option<tg::sandbox::id::Proof>,

	pub sandbox: tg::sandbox::Id,

	/// Whether a scheduler offered the boot, rather than the runner's server creating it on the shortcut path.
	pub scheduled: bool,
}

struct BootInner {
	/// Whether the boot took the number of its end, after which the runner does not give it up.
	ending: bool,

	/// The finish of each process of the boot that has a finish under way or that a server answered. A process without an entry is unfinished, and the end of the boot finishes it from the runner's memory.
	finishes: BTreeMap<tg::process::Id, Finish>,

	/// The processes that this server spawned into the boot, which this server finishes, rather than the sandbox's scheduler.
	local: BTreeSet<tg::process::Id>,

	processes: Vec<tg::runner::control::Process>,

	/// The number of the boot's next write.
	sequence: u64,
}

/// The finish of a process of a boot.
enum Finish {
	/// A server accepted or refused a finish of the process, which is final.
	Answered,

	/// The number of finishes of the process that are under way and that no server has answered yet.
	Pending(usize),
}

/// A finish of a process of a boot that is under way. Call `answered` once a server accepts or refuses it. If it is dropped before, such as when its task is aborted, the process is unfinished again, unless another finish of it is under way, so that the end of the boot finishes it from the runner's memory.
pub struct Unaccepted {
	answered: bool,
	boot: Arc<Boot>,
	process: tg::process::Id,
}

/// How a boot ends locally, which stops its sandbox or destroys it. The runner gives up a boot that its scheduler ended with a stop, unless the index's record of the end that the runner learns of destroys the sandbox.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
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
		location: tg::Location,
		proof: Option<tg::sandbox::id::Proof>,
		scheduled: bool,
		sequence: u64,
	) -> Self {
		let inner = BootInner {
			ending: false,
			finishes: BTreeMap::new(),
			local: BTreeSet::new(),
			processes: Vec::new(),
			sequence,
		};
		Self {
			boot,
			changed: tokio::sync::watch::Sender::new(()),
			first: sequence,
			abandoned: tokio::sync::watch::Sender::new(None),
			inner: std::sync::Mutex::new(inner),
			location,
			proof,
			sandbox,
			scheduled,
		}
	}

	/// Get the number of the boot's next write, or `None` once the runner gave the boot up, after which the runner writes nothing more for it.
	pub fn next(&self) -> Option<u64> {
		let mut inner = self.inner.lock().unwrap();
		if self.abandoned.borrow().is_some() {
			return None;
		}
		let sequence = inner.sequence;
		inner.sequence += 1;
		Some(sequence)
	}

	/// Get whether the runner writes and reports the boot's end, which it does unless it gave the boot up, or the boot is on the shortcut path and the runner numbered no write of it, so that no index has a record of it.
	#[must_use]
	pub fn writes_end(&self) -> bool {
		let inner = self.inner.lock().unwrap();
		self.abandoned.borrow().is_none() && (self.scheduled || inner.sequence > self.first)
	}

	/// Take the number of the boot's end once a server has answered every finish of the boot's processes, which is the only way to get the end's number. First finish each unfinished process from the runner's memory with the function, and again each process whose finish was dropped before a server answered it, so that every process of the boot has a finish before its end. Return the end's number and the processes that the function finished, or `None` once the runner gave the boot up, after which the boot takes no end.
	pub async fn end_after_finishes(
		&self,
		mut finish: impl FnMut(Vec<tg::runner::control::Process>),
	) -> Option<(u64, Vec<tg::runner::control::Process>)> {
		let mut finished = Vec::new();
		loop {
			let unfinished = self.unfinished();
			if !unfinished.is_empty() {
				finish(unfinished.clone());
				finished.extend(unfinished);
			}
			tokio::select! {
				() = self.wait_finishes() => {},
				_ = self.wait_abandoned() => return None,
			}

			// Take the number of the end once no process of the boot is unfinished, under the lock of the check, so that no process is accepted in between.
			if let Some(sequence) = self.end() {
				return Some((sequence, finished));
			}
		}
	}

	/// Get the number of the boot's end, or `None` while a process of the boot is unfinished or once the runner gave the boot up. After this, the runner no longer gives the boot up, because its end is on the way to the index, and the boot accepts no process.
	fn end(&self) -> Option<u64> {
		let mut inner = self.inner.lock().unwrap();
		let unfinished = inner.processes.iter().any(|process| {
			process
				.id
				.as_ref()
				.is_some_and(|id| !inner.finishes.contains_key(id))
		});
		if unfinished || self.abandoned.borrow().is_some() {
			return None;
		}
		inner.ending = true;
		let sequence = inner.sequence;
		inner.sequence += 1;
		Some(sequence)
	}

	/// Give up the boot because its scheduler ended it, so that the runner writes nothing more for it, unless the boot already took the number of its end or was given up. Return whether the boot was given up now.
	pub fn abandon(&self, kind: Teardown) -> bool {
		let inner = self.inner.lock().unwrap();
		if inner.ending || self.abandoned.borrow().is_some() {
			return false;
		}
		self.abandoned.send_replace(Some(kind));
		drop(inner);

		true
	}

	/// Get how the runner gave up the boot, if it did.
	#[must_use]
	pub fn abandoned(&self) -> Option<Teardown> {
		*self.abandoned.borrow()
	}

	/// Wait until the runner gives up the boot, and get how.
	pub async fn wait_abandoned(&self) -> Teardown {
		let mut abandoned = self.abandoned.subscribe();
		let kind = abandoned.wait_for(Option::is_some).await.unwrap();
		kind.unwrap()
	}

	/// Accept a process into the boot, and remember it and whether this server spawned it, unless the boot took the number of its end or the runner gave it up, after which the boot is over and accepts no process. Return whether the boot accepted the process. A process without an ID is remembered once the runner derives its ID.
	#[must_use]
	pub fn accept(&self, process: tg::runner::control::Process, local: bool) -> bool {
		let mut inner = self.inner.lock().unwrap();
		if inner.ending || self.abandoned.borrow().is_some() {
			return false;
		}
		let Some(id) = &process.id else {
			return true;
		};
		if local {
			inner.local.insert(id.clone());
		}
		inner.processes.push(process);

		true
	}

	/// Get what the runner reports about the boot when it connects to its scheduler, which is the processes that the scheduler offered into the boot and whose finish no server answered, and the proof of a boot on the shortcut path.
	#[must_use]
	pub fn report(&self) -> (Vec<tg::process::Id>, Option<tg::sandbox::id::Proof>) {
		let processes = self
			.unanswered()
			.into_iter()
			.filter(|process| !self.local(process))
			.filter_map(|process| process.id)
			.collect();

		(processes, self.proof.clone())
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

	/// Get the processes that the boot accepted that have no finish under way and that no server answered a finish of, which the end of the boot finishes from the runner's memory.
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
					.is_some_and(|id| !inner.finishes.contains_key(id))
			})
			.cloned()
			.collect()
	}

	/// Get the processes that the boot accepted and whose finish no server answered.
	#[must_use]
	pub fn unanswered(&self) -> Vec<tg::runner::control::Process> {
		let inner = self.inner.lock().unwrap();
		inner
			.processes
			.iter()
			.filter(|process| {
				process
					.id
					.as_ref()
					.is_some_and(|id| !matches!(inner.finishes.get(id), Some(Finish::Answered)))
			})
			.cloned()
			.collect()
	}

	/// Begin a finish of a process of the boot, which is under way until a server answers it or the returned value is dropped.
	#[must_use]
	pub fn begin_finish(self: &Arc<Self>, process: &tg::process::Id) -> Unaccepted {
		let mut inner = self.inner.lock().unwrap();
		let finish = inner
			.finishes
			.entry(process.clone())
			.or_insert(Finish::Pending(0));
		if let Finish::Pending(count) = finish {
			*count += 1;
		}
		drop(inner);
		self.changed.send_replace(());
		Unaccepted {
			answered: false,
			boot: self.clone(),
			process: process.clone(),
		}
	}

	/// Wait until a server has answered every finish of the boot's processes that is under way.
	pub async fn wait_finishes(&self) {
		let mut changed = self.changed.subscribe();
		loop {
			let pending = self
				.inner
				.lock()
				.unwrap()
				.finishes
				.values()
				.any(|finish| matches!(finish, Finish::Pending(_)));
			if !pending {
				return;
			}
			changed.changed().await.unwrap();
		}
	}
}

impl Unaccepted {
	/// Record that a server accepted or refused the finish, which is final, so that nothing finishes the process again.
	pub fn answered(mut self) {
		self.answered = true;
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
		let mut inner = self.boot.inner.lock().unwrap();
		if self.answered {
			inner
				.finishes
				.insert(self.process.clone(), Finish::Answered);
		} else if let Some(Finish::Pending(count)) = inner.finishes.get_mut(&self.process) {
			*count -= 1;
			if *count == 0 {
				inner.finishes.remove(&self.process);
			}
		}
		drop(inner);
		self.boot.changed.send_replace(());
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn a_boot_numbers_its_writes_in_order_and_gives_no_number_once_abandoned() {
		let boot = boot(1);
		assert_eq!(boot.next(), Some(1));
		assert_eq!(boot.next(), Some(2));
		assert!(boot.abandon(Teardown::Stop));
		assert_eq!(boot.next(), None);
		assert_eq!(boot.end(), None);
		assert!(!boot.abandon(Teardown::Destroy));
		assert!(matches!(boot.abandoned(), Some(Teardown::Stop)));
	}

	#[test]
	fn a_boot_accepts_no_process_once_it_took_the_number_of_its_end_or_was_abandoned() {
		let ended = boot(1);
		assert!(ended.accept(process(&tg::process::Id::new()), false));
		assert!(ended.end().is_none(), "the end should wait for the process");
		let ended = boot(1);
		assert_eq!(ended.end(), Some(1));
		assert!(
			!ended.accept(process(&tg::process::Id::new()), false),
			"a boot that took the number of its end would accept a process"
		);
		assert!(ended.unfinished().is_empty());
		let abandoned = boot(1);
		assert!(abandoned.abandon(Teardown::Stop));
		assert!(
			!abandoned.accept(process(&tg::process::Id::new()), false),
			"a boot that was given up would accept a process"
		);
	}

	#[test]
	fn a_boot_that_took_the_number_of_its_end_is_not_abandoned() {
		let boot = boot(1);
		assert_eq!(boot.end(), Some(1));
		assert!(!boot.abandon(Teardown::Stop));
		assert_eq!(boot.next(), Some(2));
	}

	#[test]
	fn a_boot_on_the_shortcut_path_writes_its_end_once_it_numbered_a_write() {
		let shortcut = Boot::new(0, tg::sandbox::Id::new(), local(), None, false, 1);
		assert!(!shortcut.writes_end());
		assert_eq!(shortcut.next(), Some(1));
		assert!(
			shortcut.writes_end(),
			"a shortcut boot that numbered a write would write no end"
		);
		let scheduled = boot(1);
		assert!(scheduled.writes_end());
		assert!(scheduled.abandon(Teardown::Stop));
		assert!(!scheduled.writes_end());
	}

	#[tokio::test]
	async fn a_boot_takes_its_end_only_after_a_server_answered_every_finish() {
		let boot = boot(1);
		let process = tg::process::Id::new();
		assert!(boot.accept(self::process(&process), true));
		let finish = boot.begin_finish(&process);

		// The end waits while the process's finish is under way.
		let mut end =
			std::pin::pin!(boot.end_after_finishes(|_| panic!("the process is finishing")));
		assert!(poll(&mut end).is_pending());

		// The end takes its number once a server answered the finish.
		finish.answered();
		let std::task::Poll::Ready(Some((sequence, finished))) = poll(&mut end) else {
			panic!("the end should take its number");
		};
		assert_eq!(sequence, 1);
		assert!(finished.is_empty());
	}

	#[tokio::test]
	async fn a_boot_finishes_from_memory_again_a_process_whose_finish_was_dropped_before_its_end() {
		let boot = boot(1);
		let process = tg::process::Id::new();
		assert!(boot.accept(self::process(&process), true));
		let finishes = std::cell::RefCell::new(Vec::new());
		let calls = std::cell::Cell::new(0);

		// The end finishes the unfinished process from memory and waits for the finish.
		let mut end = std::pin::pin!(boot.end_after_finishes(|processes| {
			calls.set(calls.get() + 1);
			for process in processes {
				let finish = boot.begin_finish(process.id.as_ref().unwrap());
				finishes.borrow_mut().push(finish);
			}
		}));
		assert!(poll(&mut end).is_pending());
		assert_eq!(calls.get(), 1);

		// A finish that is dropped before a server answered it is begun again.
		finishes.borrow_mut().clear();
		assert!(poll(&mut end).is_pending());
		assert_eq!(calls.get(), 2);

		// The end takes its number once a server answered the second finish.
		finishes.borrow_mut().pop().unwrap().answered();
		let std::task::Poll::Ready(Some((_, finished))) = poll(&mut end) else {
			panic!("the end should take its number");
		};
		assert_eq!(finished.len(), 2);
	}

	#[tokio::test]
	async fn a_boot_that_is_abandoned_while_its_end_waits_takes_no_end() {
		let boot = boot(1);
		let process = tg::process::Id::new();
		assert!(boot.accept(self::process(&process), true));
		let finish = boot.begin_finish(&process);
		let mut end = std::pin::pin!(boot.end_after_finishes(|_| {}));
		assert!(poll(&mut end).is_pending());
		assert!(boot.abandon(Teardown::Stop));
		assert!(matches!(poll(&mut end), std::task::Poll::Ready(None)));
		drop(finish);
	}

	#[tokio::test]
	async fn a_boot_waits_for_every_finish_that_it_began() {
		let boot = boot(1);
		let first = boot.begin_finish(&tg::process::Id::new());
		let second = boot.begin_finish(&tg::process::Id::new());
		let mut wait = std::pin::pin!(boot.wait_finishes());
		assert!(poll(&mut wait).is_pending());
		first.answered();
		assert!(poll(&mut wait).is_pending());
		second.answered();
		assert!(poll(&mut wait).is_ready());
	}

	#[tokio::test]
	async fn a_process_whose_finish_is_dropped_before_a_server_answers_it_is_unfinished_again() {
		let boot = boot(1);
		let process = tg::process::Id::new();
		assert!(boot.accept(self::process(&process), true));
		assert_eq!(boot.unfinished().len(), 1);

		// A finish that is under way leaves the process out of the unfinished processes, and the wait waits for it.
		let finish = boot.begin_finish(&process);
		assert!(boot.unfinished().is_empty());
		let mut wait = std::pin::pin!(boot.wait_finishes());
		assert!(poll(&mut wait).is_pending());

		// A finish that is dropped before a server answers it, such as when its task is aborted, makes the process unfinished again, so that the end of the boot finishes it from memory.
		drop(finish);
		assert!(poll(&mut wait).is_ready());
		assert_eq!(boot.unfinished().len(), 1);

		// A second finish that is begun before the first is dropped keeps the process finishing.
		let first = boot.begin_finish(&process);
		let second = boot.begin_finish(&process);
		drop(first);
		assert!(boot.unfinished().is_empty());

		// A finish that a server answered is final.
		second.answered();
		assert!(boot.unfinished().is_empty());
		drop(boot.begin_finish(&process));
		assert!(boot.unfinished().is_empty());
	}

	#[tokio::test]
	async fn a_later_boot_waits_for_the_earlier_boots_of_its_sandbox() {
		let boots = Boots::default();
		let sandbox = tg::sandbox::Id::new();
		let other = tg::sandbox::Id::new();
		boots.insert(Arc::new(Boot::new(
			0,
			sandbox.clone(),
			local(),
			None,
			true,
			1,
		)));
		boots.insert(Arc::new(Boot::new(
			0,
			other.clone(),
			local(),
			None,
			true,
			1,
		)));
		boots.insert(Arc::new(Boot::new(
			1,
			sandbox.clone(),
			local(),
			None,
			true,
			1,
		)));
		assert_eq!(boots.latest(&sandbox).unwrap().boot, 1);
		let mut wait = std::pin::pin!(boots.wait_ended_before(&sandbox, 1));
		assert!(poll(&mut wait).is_pending());
		boots.remove(&other, 0);
		assert!(poll(&mut wait).is_pending());
		boots.remove(&sandbox, 0);
		assert!(poll(&mut wait).is_ready());
	}

	fn boot(sequence: u64) -> Arc<Boot> {
		let sandbox = tg::sandbox::Id::new();
		Arc::new(Boot::new(0, sandbox, local(), None, true, sequence))
	}

	fn local() -> tg::Location {
		tg::Location::Local(tg::location::Local::default())
	}

	fn process(id: &tg::process::Id) -> tg::runner::control::Process {
		let data = tg::process::Data {
			cacheable: false,
			checksum: tg::process::Checksum::default(),
			children: None,
			command: tg::Referent::with_node(tg::Either::Right(tg::command::Id::new(b"command"))),
			created_at: 0,
			creator: tg::Principal::Root,
			debug: None,
			error: None,
			exit: None,
			finished_at: None,
			host: "host".to_owned(),
			log: None,
			output: None,
			owner: tg::Principal::Root,
			retry: false,
			sandbox: None,
			started_at: None,
			status: tg::process::Status::Started,
			stderr: tg::process::Stdio::default(),
			stdin: tg::process::Stdio::default(),
			stdout: tg::process::Stdio::default(),
			tty: None,
		};
		tg::runner::control::Process {
			data,
			id: Some(id.clone()),
			lease: tg::process::Lease::new(),
			leases: Vec::new(),
			open: Vec::new(),
			options: tg::referent::Options::default(),
			parent: None,
			token: None,
		}
	}

	fn poll<F: std::future::Future + Unpin>(future: &mut F) -> std::task::Poll<F::Output> {
		let waker = std::task::Waker::noop();
		let mut context = std::task::Context::from_waker(waker);
		std::pin::Pin::new(future).poll(&mut context)
	}
}
