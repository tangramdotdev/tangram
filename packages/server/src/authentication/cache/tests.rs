use {
	super::{Cache, Invalidation},
	crate::{authentication::Entry, config::AuthenticationCache},
	std::time::Duration,
	tangram_client as tg,
};

#[tokio::test]
async fn user_billing_and_runner_entries() {
	let cache = Cache::new(&AuthenticationCache::default());
	cache.set_ready(true);
	let user = tg::Principal::User(tg::user::Id::new());
	let entry = Entry {
		billing_ready: None,
		principal: user.clone(),
	};
	cache.insert([1; 32], entry).await;
	assert!(cache.get(&[1; 32]).await.is_none());
	let entry = Entry {
		billing_ready: Some(true),
		principal: user.clone(),
	};
	cache.insert([1; 32], entry).await;
	assert_eq!(cache.get(&[1; 32]).await.unwrap().principal, user);
	let runner = tg::Principal::Runner(tg::runner::Id::new());
	let entry = Entry {
		billing_ready: None,
		principal: runner.clone(),
	};
	cache.insert([2; 32], entry).await;
	assert_eq!(cache.get(&[2; 32]).await.unwrap().principal, runner);
}

#[tokio::test]
async fn token_and_principal_invalidation() {
	let cache = Cache::new(&AuthenticationCache::default());
	cache.set_ready(true);
	let principal = tg::Principal::Runner(tg::runner::Id::new());
	for hash in [[1; 32], [2; 32]] {
		let entry = Entry {
			billing_ready: None,
			principal: principal.clone(),
		};
		cache.insert(hash, entry).await;
	}
	let entry = Entry {
		billing_ready: None,
		principal: tg::Principal::Runner(tg::runner::Id::new()),
	};
	cache.insert([3; 32], entry).await;
	cache
		.invalidate(Invalidation::Token([1; 32]))
		.await
		.unwrap();
	assert!(cache.get(&[1; 32]).await.is_none());
	assert!(cache.get(&[2; 32]).await.is_some());
	cache
		.invalidate(Invalidation::Principal(principal))
		.await
		.unwrap();
	assert!(cache.get(&[2; 32]).await.is_none());
	assert!(cache.get(&[3; 32]).await.is_some());
}

#[tokio::test]
async fn fixed_expiry() {
	let config = AuthenticationCache {
		capacity: 10,
		ttl: Duration::from_secs(1),
	};
	let cache = Cache::new(&config);
	cache.set_ready(true);
	let entry = Entry {
		billing_ready: None,
		principal: tg::Principal::Runner(tg::runner::Id::new()),
	};
	cache.insert([1; 32], entry).await;
	tokio::time::sleep(Duration::from_millis(600)).await;
	assert!(cache.get(&[1; 32]).await.is_some());
	tokio::time::sleep(Duration::from_millis(600)).await;
	assert!(cache.get(&[1; 32]).await.is_none());
}

#[tokio::test]
async fn interruption_and_disabled_cache() {
	let cache = Cache::new(&AuthenticationCache::default());
	cache.set_ready(true);
	let entry = Entry {
		billing_ready: None,
		principal: tg::Principal::Runner(tg::runner::Id::new()),
	};
	cache.insert([1; 32], entry.clone()).await;
	cache.set_ready(false);
	assert!(cache.get(&[1; 32]).await.is_none());
	cache.insert([2; 32], entry.clone()).await;
	cache.set_ready(true);
	assert!(cache.get(&[1; 32]).await.is_none());
	assert!(cache.get(&[2; 32]).await.is_none());
	let config = AuthenticationCache {
		capacity: 0,
		..AuthenticationCache::default()
	};
	let cache = Cache::new(&config);
	cache.set_ready(true);
	cache.insert([1; 32], entry).await;
	assert!(cache.get(&[1; 32]).await.is_none());
}

#[tokio::test]
async fn capacity() {
	let config = AuthenticationCache {
		capacity: 2,
		..AuthenticationCache::default()
	};
	let cache = Cache::new(&config);
	cache.set_ready(true);
	for index in 0..10 {
		let entry = Entry {
			billing_ready: None,
			principal: tg::Principal::Runner(tg::runner::Id::new()),
		};
		cache.insert([index; 32], entry).await;
	}
	let entries = cache.entries.as_ref().unwrap();
	entries.run_pending_tasks().await;
	assert!(entries.entry_count() <= 2);
}
