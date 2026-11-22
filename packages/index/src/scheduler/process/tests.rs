use super::{Change, SchedulerProcess};

#[test]
fn a_put_then_a_delete_records_the_delete() {
	let Change::Store(stored) = SchedulerProcess::put(None, b"request") else {
		panic!("expected the put to store the entry");
	};
	assert_eq!(stored, SchedulerProcess::Queued(b"request".to_vec()));
	let Change::Store(deleted) = SchedulerProcess::delete(Some(&stored), 1) else {
		panic!("expected the delete to record itself");
	};
	assert_eq!(deleted, SchedulerProcess::Deleted(1));
}

#[test]
fn a_repeated_put_keeps_the_request() {
	let Change::Store(stored) = SchedulerProcess::put(None, b"request") else {
		panic!("expected the put to store the entry");
	};
	let change = SchedulerProcess::put(Some(&stored), b"request");
	assert_eq!(change, Change::Store(stored));
}

#[test]
fn the_puts_and_deletes_of_an_entry_applied_any_number_of_times_in_every_order_leave_the_delete() {
	// Apply a put, a delete, and a second copy of each, in every order.
	let writes = [
		Some(b"request".as_slice()),
		None,
		Some(b"request".as_slice()),
		None,
	];
	for order in crate::tests::permutations(writes.len()) {
		let mut entry = None;
		for &index in &order {
			let change = match writes[index] {
				Some(request) => SchedulerProcess::put(entry.as_ref(), request),
				None => SchedulerProcess::delete(entry.as_ref(), i64::try_from(index).unwrap()),
			};
			if let Change::Store(stored) = change {
				entry = Some(stored);
			}
		}

		// The first delete to apply stands, and no put brings the request back.
		let first = order
			.iter()
			.copied()
			.find(|index| writes[*index].is_none())
			.unwrap();
		let expected = SchedulerProcess::Deleted(i64::try_from(first).unwrap());
		assert_eq!(
			entry,
			Some(expected),
			"the order {order:?} did not leave the first delete"
		);
	}
}
