use {
	super::{Candidate, Entries, Queue, Sandbox, State, choose_runner},
	tangram_client::prelude::*,
	tangram_server_scheduler_core::EnqueueSandboxRequestArg,
};

fn candidate(id: &tg::runner::Id, score: u128, siblings: usize) -> Candidate<'_> {
	Candidate {
		id,
		registration_index: 0,
		score: (score, score),
		siblings,
	}
}

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

/// Create runner IDs in ascending order.
fn runners() -> [tg::runner::Id; 3] {
	let mut ids = [
		tg::runner::Id::new(),
		tg::runner::Id::new(),
		tg::runner::Id::new(),
	];
	ids.sort();
	ids
}

#[test]
fn the_parents_runner_comes_first() {
	let [a, b, c] = runners();
	let candidates = [
		candidate(&a, 1, 0),
		candidate(&b, 2, 3),
		candidate(&c, 3, 0),
	];
	let chosen = choose_runner(&candidates, Some(&c)).unwrap();
	assert_eq!(chosen.id, &c);
}

#[test]
fn a_parents_runner_without_room_falls_through_to_the_siblings() {
	let [a, b, c] = runners();
	let candidates = [candidate(&a, 1, 0), candidate(&b, 2, 3)];
	let chosen = choose_runner(&candidates, Some(&c)).unwrap();
	assert_eq!(chosen.id, &b);
}

#[test]
fn the_runner_with_the_most_siblings_comes_next_and_ties_go_to_the_better_score_and_then_the_id() {
	let [a, b, c] = runners();

	// The most siblings wins over a better score.
	let candidates = [
		candidate(&a, 1, 1),
		candidate(&b, 5, 2),
		candidate(&c, 1, 0),
	];
	assert_eq!(choose_runner(&candidates, None).unwrap().id, &b);

	// Equal siblings go to the better score.
	let candidates = [
		candidate(&a, 4, 2),
		candidate(&b, 3, 2),
		candidate(&c, 1, 0),
	];
	assert_eq!(choose_runner(&candidates, None).unwrap().id, &b);

	// Equal siblings and scores go to the lower ID.
	let candidates = [candidate(&b, 3, 2), candidate(&a, 3, 2)];
	assert_eq!(choose_runner(&candidates, None).unwrap().id, &a);
}

#[test]
fn a_sandbox_without_a_parent_or_siblings_goes_to_the_best_score() {
	let [a, b, c] = runners();
	let candidates = [
		candidate(&a, 3, 0),
		candidate(&b, 1, 0),
		candidate(&c, 1, 0),
	];
	assert_eq!(choose_runner(&candidates, None).unwrap().id, &b);
	assert!(choose_runner(&[], None).is_none());
}
