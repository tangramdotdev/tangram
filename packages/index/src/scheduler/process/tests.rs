use super::{Change, SchedulerProcess};

#[test]
fn a_put_then_a_delete_removes_the_entry() {
	let change = SchedulerProcess::put(None, b"request");
	let Change::Store(stored) = change else {
		panic!("expected the put to store the entry");
	};
	assert_eq!(stored.request.as_deref(), Some(b"request".as_slice()));
	assert_eq!(SchedulerProcess::delete(Some(&stored)), Change::Clear);
}

#[test]
fn a_delete_before_its_put_wins_and_leaves_no_record() {
	let change = SchedulerProcess::delete(None);
	let Change::Store(stored) = change else {
		panic!("expected the delete to record itself");
	};
	assert_eq!(stored.request, None);
	assert_eq!(SchedulerProcess::delete(Some(&stored)), Change::Keep);
	assert_eq!(
		SchedulerProcess::put(Some(&stored), b"request"),
		Change::Clear
	);
}

#[test]
fn a_repeated_put_keeps_the_request() {
	let Change::Store(stored) = SchedulerProcess::put(None, b"request") else {
		panic!("expected the put to store the entry");
	};
	let change = SchedulerProcess::put(Some(&stored), b"request");
	assert_eq!(change, Change::Store(stored));
}
