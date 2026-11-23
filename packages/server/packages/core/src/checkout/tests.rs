use {super::Puts, std::time::Duration, tangram_client::prelude::*};

fn id(bytes: &[u8]) -> tg::Id {
	tg::artifact::Id::from(tg::file::Id::new(bytes)).into()
}

#[tokio::test]
async fn a_later_put_replaces_the_token() {
	let puts = Puts::new();
	let id = id(b"a");
	let guard = puts.lock(&id).await;
	let first = guard.put(10);
	assert_eq!(guard.get().unwrap().put, Some(first));
	let second = guard.put(11);
	assert!(second > first);
	assert_ne!(guard.get().unwrap().put, Some(first));
	assert_eq!(guard.get().unwrap().put, Some(second));
	guard.remove();
	assert_eq!(guard.get(), None);
}

#[tokio::test]
async fn a_touch_keeps_the_token() {
	let puts = Puts::new();
	let id = id(b"a");
	let guard = puts.lock(&id).await;
	let put = guard.put(10);
	guard.touch(20);
	let entry = guard.get().unwrap();
	assert_eq!(entry.put, Some(put));
	assert_eq!(entry.touched_at, 20);

	// Check that a touch of a checkout with no put records no token.
	drop(guard);
	let other = self::id(b"b");
	let guard = puts.lock(&other).await;
	guard.touch(20);
	assert_eq!(guard.get().unwrap().put, None);
}

#[tokio::test]
async fn a_touch_is_throttled_by_the_time_to_touch() {
	let puts = Puts::new();
	let id = id(b"a");
	let time_to_touch = Duration::from_secs(60);
	assert!(!puts.touched(&id, 100, time_to_touch));
	puts.lock(&id).await.touch(100);
	assert!(puts.touched(&id, 100, time_to_touch));
	assert!(puts.touched(&id, 159, time_to_touch));
	assert!(!puts.touched(&id, 160, time_to_touch));
}

#[tokio::test]
async fn old_entries_are_dropped() {
	let puts = Puts::new();
	let old = id(b"old");
	let new = id(b"new");
	let time_to_touch = Duration::from_secs(60);
	let _ = puts.lock(&old).await.put(100);
	let _ = puts.lock(&new).await.put(150);
	puts.retain(200, time_to_touch);
	assert_eq!(puts.lock(&old).await.get(), None);
	assert!(puts.lock(&new).await.get().is_some());
}

#[test]
fn a_publish_keeps_an_existing_path() {
	let directory = tempfile::TempDir::new().unwrap();
	let directory = directory.path();
	let puts = Puts::new();
	let id = id(b"a");
	let dst = directory.join("checkout");

	// Publish a first temp, which is renamed.
	let src = directory.join("first");
	std::fs::write(&src, b"first").unwrap();
	let (first, renamed) = puts.publish_sync(&id, 10, &src, &dst).unwrap();
	assert!(renamed);

	// Publish a second temp, which leaves the existing path and records a new token.
	let src = directory.join("second");
	std::fs::write(&src, b"second").unwrap();
	let (second, renamed) = puts.publish_sync(&id, 11, &src, &dst).unwrap();
	assert!(!renamed);
	assert!(second > first);
	assert_eq!(std::fs::read(&dst).unwrap(), b"first");
	assert_eq!(puts.lock_sync(&id).get().unwrap().put, Some(second));
}
