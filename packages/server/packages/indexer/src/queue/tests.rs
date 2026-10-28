use {
	super::{Kind, Payload, Queue, Queues, SequenceReservation, Work},
	itertools::Itertools as _,
	std::collections::BTreeSet,
};

fn reserve(queues: &mut Queues, kind: Kind, end: u64) {
	let reservation = SequenceReservation { end, kind };
	queues.finish_reservation(reservation);
}

fn payload(work: &Work) -> &bytes::Bytes {
	match &work.payload {
		Payload::Cache => panic!("expected the payload in memory"),
		Payload::Memory(payload) => payload,
	}
}

#[test]
fn pops_each_entry_independently_in_sequence_order() {
	for kind in [Kind::Archive, Kind::Index] {
		let mut queues = Queues::empty(usize::MAX);
		queues.insert_entry(kind, 1, bytes::Bytes::from_static(b"one"));
		queues.insert_entry(kind, 0, bytes::Bytes::from_static(b"zero"));
		let work = queues.pop_entry(kind).unwrap();
		assert_eq!(work.sequence, 0);
		assert_eq!(payload(&work), &b"zero"[..]);
		let work = queues.pop_entry(kind).unwrap();
		assert_eq!(work.sequence, 1);
		assert_eq!(payload(&work), &b"one"[..]);
		assert!(queues.pop_entry(kind).is_none());
		assert_eq!(queues.memory.used, 0);
	}
}

#[test]
fn keeps_the_queues_separate() {
	let mut queues = Queues::empty(usize::MAX);
	queues.insert_entry(Kind::Archive, 0, bytes::Bytes::from_static(b"archive"));
	assert!(queues.pop_entry(Kind::Index).is_none());
	let work = queues.pop_entry(Kind::Archive).unwrap();
	assert_eq!(payload(&work), &b"archive"[..]);
}

#[test]
fn drops_only_written_payloads_when_over_the_memory_limit() {
	let mut queues = Queues::empty(4);
	reserve(&mut queues, Kind::Index, 2);

	// Keep a payload whose write has not finished, even over the memory limit.
	let sequence = queues.try_allocate_sequence(Kind::Index).unwrap();
	queues.insert_entry(Kind::Index, sequence, bytes::Bytes::from_static(b"zero"));
	let sequence = queues.try_allocate_sequence(Kind::Index).unwrap();
	queues.insert_entry(Kind::Index, sequence, bytes::Bytes::from_static(b"one"));
	assert_eq!(queues.memory.used, 7);
	assert!(!queues.write_finished(Kind::Index, 0));
	assert!(!queues.write_finished(Kind::Index, 1));

	// Drop the payload once its write finishes.
	queues.finish_write(Kind::Index, 1, true);
	assert_eq!(queues.memory.used, 4);
	queues.finish_write(Kind::Index, 0, false);
	assert!(queues.write_finished(Kind::Index, 0));
	assert!(queues.write_finished(Kind::Index, 1));
	assert_eq!(queues.memory.used, 4);

	// Read the dropped payload from the cache.
	let work = queues.pop_entry(Kind::Index).unwrap();
	assert!(matches!(work.payload, Payload::Memory(_)));
	let work = queues.pop_entry(Kind::Index).unwrap();
	assert_eq!(work.sequence, 1);
	assert!(matches!(work.payload, Payload::Cache));
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
