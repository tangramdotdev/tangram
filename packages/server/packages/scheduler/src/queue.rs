use {
	super::{
		Scheduler, SchedulerState,
		placement::Parent,
		sandbox::{Entries, ParentLinks, Sandbox, State},
	},
	std::collections::BTreeMap,
	tangram_client::prelude::*,
};

#[cfg(test)]
mod tests;

/// The sandboxes that the scheduler holds, in the order that they were enqueued. The scheduler passes over the queue one sandbox at a time, and at most one pass is in progress.
pub(super) struct Queue {
	/// Whether the capacity changed during the pass that is in progress, so that another pass follows it.
	pub(super) again: bool,

	/// The next sandbox of the pass that is in progress.
	pub(super) cursor: Option<tg::sandbox::Id>,

	pub(super) list: List,
}

/// A list of sandboxes whose links are in the sandboxes' entries, so that adding and removing a sandbox take constant time.
#[derive(Default)]
pub(super) struct List {
	pub(super) head: Option<tg::sandbox::Id>,
	pub(super) len: usize,
	pub(super) tail: Option<tg::sandbox::Id>,
}

#[derive(Default)]
pub(super) struct Links {
	pub(super) next: Option<tg::sandbox::Id>,
	pub(super) previous: Option<tg::sandbox::Id>,
}

impl Queue {
	pub(super) fn new() -> Self {
		Self {
			again: false,
			cursor: None,
			list: List::default(),
		}
	}

	pub(super) fn len(&self) -> usize {
		self.list.len
	}

	/// Whether a pass is in progress.
	pub(super) fn passing(&self) -> bool {
		self.cursor.is_some()
	}

	/// Add a sandbox to the end of the queue, which the pass that is in progress reaches, and which a new pass starts at otherwise.
	pub(super) fn push(&mut self, entries: &mut Entries, id: &tg::sandbox::Id) {
		self.list.push(entries, Sandbox::links_mut, id);
		if self.cursor.is_none() {
			self.cursor = Some(id.clone());
		}
	}

	/// Take the next sandbox of the pass that is in progress.
	pub(super) fn next(&mut self, entries: &Entries) -> Option<tg::sandbox::Id> {
		let id = self.cursor.take()?;
		self.cursor.clone_from(&entries[&id].links.next);
		self.finish_pass();

		Some(id)
	}

	pub(super) fn remove(&mut self, entries: &mut Entries, id: &tg::sandbox::Id) {
		let next = self.list.remove(entries, Sandbox::links_mut, id);
		if self.cursor.as_ref() == Some(id) {
			self.cursor = next;
			self.finish_pass();
		}
	}

	/// Pass over the queue because the capacity changed. A change during a pass does not start the pass over, and is picked up by one pass that follows it.
	pub(super) fn wake(&mut self) {
		if self.cursor.is_some() {
			self.again = true;
		} else {
			self.cursor.clone_from(&self.list.head);
		}
	}

	/// Start another pass if the pass that is in progress reached the end of the queue and the capacity changed during it.
	pub(super) fn finish_pass(&mut self) {
		if self.cursor.is_none() && self.again {
			self.again = false;
			self.cursor.clone_from(&self.list.head);
		}
	}
}

impl List {
	pub(super) fn push(
		&mut self,
		entries: &mut Entries,
		links: impl Fn(&mut Sandbox) -> &mut Links,
		id: &tg::sandbox::Id,
	) {
		let previous = self.tail.replace(id.clone());
		if let Some(previous) = &previous {
			links(entries.get_mut(previous).unwrap()).next = Some(id.clone());
		} else {
			self.head = Some(id.clone());
		}
		*links(entries.get_mut(id).unwrap()) = Links {
			next: None,
			previous,
		};
		self.len += 1;
	}

	/// Remove a sandbox, and return the sandbox that followed it.
	pub(super) fn remove(
		&mut self,
		entries: &mut Entries,
		links: impl Fn(&mut Sandbox) -> &mut Links,
		id: &tg::sandbox::Id,
	) -> Option<tg::sandbox::Id> {
		let Links { next, previous } = std::mem::take(links(entries.get_mut(id).unwrap()));
		if let Some(previous) = &previous {
			links(entries.get_mut(previous).unwrap())
				.next
				.clone_from(&next);
		} else {
			self.head.clone_from(&next);
		}
		if let Some(next) = &next {
			links(entries.get_mut(next).unwrap()).previous = previous;
		} else {
			self.tail = previous;
		}
		self.len -= 1;

		next
	}
}

impl SchedulerState {
	/// Whether the scheduler can offer a sandbox, which a restarted scheduler does only once each of its runners has reported or been declared dead.
	pub(super) fn can_schedule(&self, scheduler: &Scheduler) -> bool {
		self.recovery.is_none()
			&& self.sandboxes.offers < scheduler.config.max_offers
			&& self.queue.passing()
	}

	pub(super) fn schedule_one(&mut self, scheduler: &Scheduler) {
		let Some(sandbox) = self.queue.next(&self.sandboxes.entries) else {
			return;
		};
		self.try_offer_boot(scheduler, &sandbox);
	}

	/// Return a sandbox whose offer was not accepted to the queued state. The sandbox kept its place in the queue while it was offered.
	pub(super) fn requeue_sandbox(&mut self, id: &tg::sandbox::Id) {
		if let Some(parent) = self.sandboxes.entries[id].parent.clone() {
			self.remove_child(&parent, id);
		}
		self.sandboxes.entries.get_mut(id).unwrap().state = State::Queued;
	}

	/// Add a sandbox that the scheduler holds to the end of a parent's queue, so that the sandbox can borrow the parent's capacity.
	pub(super) fn add_parent(&mut self, id: &tg::sandbox::Id, parent: &tg::sandbox::Id) {
		let links = ParentLinks {
			links: Links::default(),
			parent: parent.clone(),
		};
		self.sandboxes
			.entries
			.get_mut(id)
			.unwrap()
			.parents
			.push(links);
		self.parents
			.0
			.entry(parent.clone())
			.or_insert_with(|| Parent {
				borrowable: None,
				children: BTreeMap::new(),
				queue: List::default(),
			})
			.queue
			.push(
				&mut self.sandboxes.entries,
				|sandbox| sandbox.parent_links_mut(parent),
				id,
			);
	}

	/// Remove a sandbox from the queue, and from its parents' queues.
	pub(super) fn remove_queued_sandbox(&mut self, id: &tg::sandbox::Id) {
		self.queue.remove(&mut self.sandboxes.entries, id);
		let parents = self.sandboxes.entries[id]
			.parents
			.iter()
			.map(|links| links.parent.clone())
			.collect::<Vec<_>>();
		for parent_id in parents {
			let Some(parent) = self.parents.0.get_mut(&parent_id) else {
				continue;
			};
			parent.queue.remove(
				&mut self.sandboxes.entries,
				|sandbox| sandbox.parent_links_mut(&parent_id),
				id,
			);
			if parent.is_empty() {
				self.parents.0.remove(&parent_id);
			}
		}
		self.sandboxes.entries.get_mut(id).unwrap().parents.clear();
	}

	/// Find the first of a parent's children that the scheduler has not offered to a runner.
	pub(super) fn first_queued_child(&self, parent: &tg::sandbox::Id) -> Option<tg::sandbox::Id> {
		let mut child = self.parents.0.get(parent)?.queue.head.as_ref();
		while let Some(id) = child {
			let sandbox = &self.sandboxes.entries[id];
			if matches!(sandbox.state, State::Queued) {
				return Some(id.clone());
			}
			child = sandbox.parent_links(parent).next.as_ref();
		}

		None
	}
}
