use {
	super::{Kind, Payload, Queue, Queues, SequenceReservation, Work},
	crate::request,
	itertools::Itertools as _,
	std::collections::BTreeSet,
};

fn fragment(batch: u8, fragment: u64, fragments: u64, payload: &'static [u8]) -> request::Fragment {
	request::Fragment {
		batch: [batch; 16],
		fragment,
		fragments,
		payload: bytes::Bytes::from_static(payload),
	}
}

fn reserve(queues: &mut Queues, kind: Kind, end: u64) {
	let reservation = SequenceReservation { end, kind };
	queues.finish_reservation(reservation);
}

#[test]
fn assembles_batches_in_fragment_order() {
	for kind in [Kind::Archive, Kind::Index] {
		let mut queues = Queues::empty(usize::MAX);
		let timeout = std::time::Duration::from_secs(60);
		queues
			.insert_fragment(kind, 0, &fragment(0, 1, 2, b"one"), timeout)
			.unwrap();
		assert!(queues.pop_work(kind).is_none());
		queues
			.insert_fragment(kind, 1, &fragment(0, 0, 2, b"zero"), timeout)
			.unwrap();
		let Some(Work::Process(batch)) = queues.pop_work(kind) else {
			panic!("expected a batch");
		};
		let payloads = batch
			.payloads
			.iter()
			.map(|payload| match payload {
				Payload::Memory(payload) => payload.clone(),
				Payload::Cache(_) => panic!("expected the payload in memory"),
			})
			.collect::<Vec<_>>();
		assert_eq!(payloads, [&b"zero"[..], &b"one"[..]]);
		assert_eq!(batch.sequences, [1, 0]);
		assert_eq!(queues.memory.used, 0);
	}
}

#[test]
fn keeps_the_queues_separate() {
	let mut queues = Queues::empty(usize::MAX);
	let timeout = std::time::Duration::from_secs(60);
	queues
		.insert_fragment(Kind::Archive, 0, &fragment(0, 0, 2, b"archive"), timeout)
		.unwrap();
	queues
		.insert_fragment(Kind::Index, 0, &fragment(0, 1, 2, b"index"), timeout)
		.unwrap();
	assert!(queues.pop_work(Kind::Archive).is_none());
	assert!(queues.pop_work(Kind::Index).is_none());
}

#[test]
fn expiration_tracks_completion_and_abandonment() {
	let mut queues = Queues::empty(usize::MAX);
	let timeout = std::time::Duration::from_secs(60);
	queues
		.insert_fragment(Kind::Index, 0, &fragment(0, 0, 2, b""), timeout)
		.unwrap();
	assert_eq!(queues.index.batches.by_expires_at.len(), 1);
	assert_eq!(
		queues.next_batch_deadline(),
		Some(queues.index.batches.active[&[0; 16]].deadline)
	);
	queues
		.insert_fragment(Kind::Index, 1, &fragment(0, 1, 2, b""), timeout)
		.unwrap();
	assert!(queues.index.batches.active.is_empty());
	assert_eq!(queues.index.batches.complete.len(), 1);
	assert_eq!(queues.index.batches.by_expires_at.len(), 1);
	assert!(matches!(
		queues.pop_work(Kind::Index),
		Some(Work::Process(_))
	));
	queues.expire_batches(Kind::Index, timeout);
	assert!(queues.pop_work(Kind::Index).is_none());

	queues
		.insert_fragment(Kind::Index, 2, &fragment(1, 0, 2, b""), timeout)
		.unwrap();
	assert_eq!(queues.index.batches.by_expires_at.len(), 2);
	queues.abandon_incomplete_batches();
	assert_eq!(queues.index.batches.by_expires_at.len(), 1);
	assert!(
		matches!(queues.pop_work(Kind::Index), Some(Work::Delete(sequences)) if sequences == [2])
	);
}

#[test]
fn expires_only_due_batches() {
	let mut queues = Queues::empty(usize::MAX);
	queues
		.insert_fragment(
			Kind::Archive,
			0,
			&fragment(0, 0, 2, b"payload"),
			std::time::Duration::ZERO,
		)
		.unwrap();
	queues.expire_batches(Kind::Archive, std::time::Duration::from_secs(60));
	assert!(
		matches!(queues.pop_work(Kind::Archive), Some(Work::Delete(sequences)) if sequences == [0])
	);
	assert!(queues.pop_work(Kind::Archive).is_none());
	assert!(queues.archive.batches.active.is_empty());
	assert_eq!(queues.archive.batches.timed_out.len(), 1);
	assert_eq!(queues.archive.batches.by_expires_at.len(), 1);
	assert_eq!(queues.memory.used, 0);
}

#[test]
fn deletes_fragments_of_complete_and_timed_out_batches() {
	for timed_out in [false, true] {
		let mut queues = Queues::empty(usize::MAX);
		let fragments = if timed_out { 2 } else { 1 };
		let timeout = std::time::Duration::ZERO;
		queues
			.insert_fragment(Kind::Index, 0, &fragment(0, 0, fragments, b""), timeout)
			.unwrap();
		if timed_out {
			queues.expire_batches(Kind::Index, std::time::Duration::from_secs(60));
		}
		queues.pop_work(Kind::Index);
		let result =
			queues.insert_fragment(Kind::Index, 1, &fragment(0, 0, fragments, b""), timeout);
		assert_eq!(result.is_err(), timed_out);
		assert!(
			matches!(queues.pop_work(Kind::Index), Some(Work::Delete(sequences)) if sequences == [1])
		);
		let (_, batch) = queues.index.batches.by_expires_at.pop_first().unwrap();
		queues
			.index
			.batches
			.by_expires_at
			.insert((tokio::time::Instant::now(), batch));
		queues.expire_batches(Kind::Index, timeout);
		assert!(queues.index.batches.complete.is_empty());
		assert!(queues.index.batches.timed_out.is_empty());
		assert!(queues.index.batches.by_expires_at.is_empty());
	}
}

#[test]
fn rejects_invalid_fragments() {
	let mut queues = Queues::empty(usize::MAX);
	let timeout = std::time::Duration::from_secs(60);
	queues
		.insert_fragment(Kind::Index, 0, &fragment(0, 0, 2, b"zero"), timeout)
		.unwrap();
	let counts = queues.insert_fragment(Kind::Index, 1, &fragment(0, 1, 3, b"one"), timeout);
	assert!(counts.is_err());
	let position = queues.insert_fragment(Kind::Index, 2, &fragment(1, 2, 2, b""), timeout);
	assert!(position.is_err());
	for sequence in 1..3 {
		assert!(
			matches!(queues.pop_work(Kind::Index), Some(Work::Delete(sequences)) if sequences == [sequence])
		);
	}
}

#[test]
fn drops_only_written_payloads_when_over_the_memory_limit() {
	let mut queues = Queues::empty(4);
	reserve(&mut queues, Kind::Index, 2);
	let timeout = std::time::Duration::from_secs(60);

	// Keep a payload whose write has not finished, even over the memory limit.
	let first = fragment(0, 0, 2, b"zero");
	let sequence = queues.try_allocate_sequence(Kind::Index).unwrap();
	queues
		.insert_fragment(Kind::Index, sequence, &first, timeout)
		.unwrap();
	let second = fragment(0, 1, 2, b"one");
	let sequence = queues.try_allocate_sequence(Kind::Index).unwrap();
	queues
		.insert_fragment(Kind::Index, sequence, &second, timeout)
		.unwrap();
	assert_eq!(queues.memory.used, 7);
	assert!(!queues.writes_finished(Kind::Index, &[0, 1]));

	// Drop the payload once its write finishes.
	queues.finish_write(Kind::Index, 1, &second, true);
	assert_eq!(queues.memory.used, 4);
	queues.finish_write(Kind::Index, 0, &first, false);
	assert!(queues.writes_finished(Kind::Index, &[0, 1]));
	assert_eq!(queues.memory.used, 4);

	// Read the dropped payload from the cache.
	let Some(Work::Process(batch)) = queues.pop_work(Kind::Index) else {
		panic!("expected a batch");
	};
	assert!(matches!(batch.payloads[0], Payload::Memory(_)));
	assert!(matches!(batch.payloads[1], Payload::Cache(1)));
	assert_eq!(queues.memory.used, 0);
	assert!(queues.memory.evictable.is_empty());
}

#[test]
fn advances_the_read_sequence_only_after_contiguous_completions() {
	let mut queue = Queue::new(2, 5);
	queue.complete(4);
	assert_eq!(queue.read_sequence, 2);
	queue.complete(2);
	assert_eq!(queue.read_sequence, 3);
	queue.complete(3);
	assert_eq!(queue.read_sequence, 5);
}

#[test]
fn coalesces_a_million_completions_behind_a_stalled_entry() {
	let end = 1_000_001;
	let mut queue = Queue::new(0, end);
	for sequence in 1..end {
		queue.complete(sequence);
	}
	assert_eq!(queue.read_sequence, 0);
	assert_eq!(queue.completed.len(), 1);
	assert_eq!(queue.completed.get(&1), Some(&end));
	queue.complete(0);
	assert_eq!(queue.read_sequence, end);
	assert!(queue.completed.is_empty());
}

#[test]
fn completion_ranges_match_individual_completions_in_every_order() {
	for sequences in (10..17).permutations(7) {
		let mut queue = Queue::new(10, 17);
		let mut completed = BTreeSet::new();
		for sequence in sequences {
			completed.insert(sequence);
			for sequence in [sequence, sequence, 9] {
				queue.complete(sequence);
			}
			let read_sequence = (10..17)
				.find(|sequence| !completed.contains(sequence))
				.unwrap_or(17);
			assert_eq!(queue.read_sequence, read_sequence);
			let expected = completed
				.range(read_sequence..)
				.copied()
				.collect::<BTreeSet<_>>();
			let actual = queue
				.completed
				.iter()
				.flat_map(|(&start, &end)| start..end)
				.collect::<BTreeSet<_>>();
			assert_eq!(actual, expected);
			assert!(
				queue
					.completed
					.iter()
					.all(|(&start, &end)| start > read_sequence && start < end)
			);
			assert!(
				queue
					.completed
					.iter()
					.tuple_windows()
					.all(|((_, end), (start, _))| end < start)
			);
		}
	}
}

#[test]
fn completes_the_last_sequences_that_can_be_reserved() {
	let end = u64::try_from(i64::MAX).unwrap();
	let mut queue = Queue::new(end - 3, end);
	queue.complete(end - 1);
	queue.complete(end - 2);
	assert_eq!(queue.read_sequence, end - 3);
	queue.complete(end - 3);
	assert_eq!(queue.read_sequence, end);
	assert!(queue.completed.is_empty());
}
