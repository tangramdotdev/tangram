use {super::SandboxScheduler, tangram_client::prelude::*};

#[test]
fn a_later_boot_wins_in_any_order() {
	let a = tg::scheduler::Id::new();
	let b = tg::scheduler::Id::new();

	// A put of a later boot replaces an earlier boot's scheduler.
	let change = SandboxScheduler::put(None, 0, &a).unwrap();
	assert_eq!(change.removed, None);
	let change = SandboxScheduler::put(Some(&change.stored), 1, &b).unwrap();
	assert_eq!(change.removed, Some(a.clone()));
	assert_eq!(change.stored.scheduler, Some(b.clone()));

	// An older put after a newer one does not apply.
	assert!(SandboxScheduler::put(Some(&change.stored), 0, &a).is_none());

	// An older delete after a newer put does not apply.
	assert!(SandboxScheduler::delete(Some(&change.stored), 0).is_none());
}

#[test]
fn a_delete_before_its_put_wins() {
	let scheduler = tg::scheduler::Id::new();
	let change = SandboxScheduler::delete(None, 2).unwrap();
	assert_eq!(change.removed, None);
	assert_eq!(change.stored.scheduler, None);
	assert!(SandboxScheduler::put(Some(&change.stored), 2, &scheduler).is_none());
	assert!(SandboxScheduler::put(Some(&change.stored), 3, &scheduler).is_some());
}

#[test]
fn a_delete_after_its_put_removes_the_key() {
	let scheduler = tg::scheduler::Id::new();
	let change = SandboxScheduler::put(None, 1, &scheduler).unwrap();
	let change = SandboxScheduler::delete(Some(&change.stored), 1).unwrap();
	assert_eq!(change.removed, Some(scheduler.clone()));
	assert_eq!(change.stored.scheduler, None);

	// A repeated put of the same boot by the same scheduler keeps its key.
	let change = SandboxScheduler::put(None, 1, &scheduler).unwrap();
	let change = SandboxScheduler::put(Some(&change.stored), 1, &scheduler).unwrap();
	assert_eq!(change.removed, None);
}
