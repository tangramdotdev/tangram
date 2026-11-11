use {
	super::{Candidate, choose_runner},
	tangram_client::prelude::*,
};

fn candidate(id: &tg::runner::Id, score: u128, siblings: usize) -> Candidate<'_> {
	Candidate {
		id,
		registration_index: 0,
		score: (score, score),
		siblings,
	}
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
