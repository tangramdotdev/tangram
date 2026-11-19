use {
	super::Queue,
	crate::{
		SchedulerState,
		sandbox::{Entries, Sandbox, State},
	},
	tangram_client::prelude::*,
	tangram_server_scheduler_core::EnqueueSandboxRequestArg,
};

/// Create the entry of a queued sandbox.
fn sandbox(id: &tg::sandbox::Id) -> Sandbox {
	let request = EnqueueSandboxRequestArg {
		arg: tg::sandbox::create::Arg::default(),
		boot: 0,
		created_at: 0,
		creator: tg::Principal::default(),
		owner: tg::Principal::default(),
		owner_ancestors: Vec::new(),
		parent: None,
		process: None,
		public: false,
		runner: None,
		sandbox: id.clone(),
		scheduler: None,
		stop: false,
		token: None,
	};
	Sandbox::new(0, None, Some(request), State::Queued)
}

/// Create a queue of sandboxes, and take the sandbox that each one's arrival started a pass at.
fn queue<const N: usize>() -> (Queue, Entries, [tg::sandbox::Id; N]) {
	let ids = std::array::from_fn(|_| tg::sandbox::Id::new());
	let mut entries = Entries::default();
	let mut queue = Queue::new();
	for id in &ids {
		entries.insert(id.clone(), sandbox(id));
		queue.push(&mut entries, id);
	}
	while queue.next(&entries).is_some() {}
	(queue, entries, ids)
}

/// Take the rest of the pass that is in progress.
fn pass(queue: &mut Queue, entries: &Entries, length: usize) -> Vec<tg::sandbox::Id> {
	std::iter::from_fn(|| queue.next(entries))
		.take(length)
		.collect()
}

#[test]
fn a_new_sandbox_starts_a_pass_at_itself() {
	let (mut queue, mut entries, [_, _]) = queue();
	assert!(!queue.passing());
	let id = tg::sandbox::Id::new();
	entries.insert(id.clone(), sandbox(&id));
	queue.push(&mut entries, &id);
	assert_eq!(queue.next(&entries), Some(id));
	assert!(!queue.passing());
}

#[test]
fn a_wake_passes_over_the_queue_in_order() {
	let (mut queue, entries, [a, b, c]) = queue();
	queue.wake();
	assert_eq!(pass(&mut queue, &entries, 4), [a, b, c]);
	assert!(!queue.passing());
}

#[test]
fn wakes_during_a_pass_start_one_pass_after_it() {
	let (mut queue, entries, [a, b, c]) = queue();
	queue.wake();
	assert_eq!(queue.next(&entries), Some(a.clone()));

	// The wakes do not start the pass over.
	queue.wake();
	queue.wake();
	assert_eq!(pass(&mut queue, &entries, 2), [b.clone(), c.clone()]);

	// One pass follows.
	assert_eq!(pass(&mut queue, &entries, 4), [a, b, c]);
	assert!(!queue.passing());
}

#[test]
fn a_pass_skips_a_removed_sandbox() {
	let (mut queue, mut entries, [a, b, c]) = queue();
	queue.wake();
	assert_eq!(queue.next(&entries), Some(a.clone()));

	// Remove the next sandbox of the pass.
	queue.remove(&mut entries, &b);
	entries.remove(&b);
	assert_eq!(queue.len(), 2);
	assert_eq!(pass(&mut queue, &entries, 2), std::slice::from_ref(&c));

	// Remove the first and the last sandboxes.
	queue.remove(&mut entries, &a);
	entries.remove(&a);
	queue.remove(&mut entries, &c);
	entries.remove(&c);
	assert_eq!(queue.len(), 0);
	queue.wake();
	assert!(!queue.passing());
}

#[test]
fn a_sandbox_is_queued_under_each_of_its_parents() {
	let mut state = SchedulerState::new();
	let [a, b] = std::array::from_fn(|_| tg::sandbox::Id::new());
	let [first, second] = std::array::from_fn(|_| tg::sandbox::Id::new());
	for id in [&a, &b] {
		state.sandboxes.entries.insert(id.clone(), sandbox(id));
		state.queue.push(&mut state.sandboxes.entries, id);
	}

	// Each parent keeps its children in the order that they were added to it.
	state.add_parent(&a, &first);
	state.add_parent(&b, &first);
	state.add_parent(&b, &second);
	state.add_parent(&a, &second);
	assert_eq!(state.first_queued_child(&first), Some(a.clone()));
	assert_eq!(state.first_queued_child(&second), Some(b.clone()));

	// A sandbox that leaves the queue leaves the queue of each of its parents.
	state.remove_queued_sandbox(&a);
	assert_eq!(state.first_queued_child(&first), Some(b.clone()));
	assert_eq!(state.first_queued_child(&second), Some(b.clone()));
	state.remove_queued_sandbox(&b);
	assert!(state.parents.0.is_empty());
	assert_eq!(state.queue.len(), 0);
}
