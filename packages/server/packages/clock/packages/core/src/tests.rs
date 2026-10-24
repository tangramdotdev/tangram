use {
	super::Clock,
	futures::FutureExt as _,
	std::{sync::Arc, time::Duration},
};

fn clock() -> Clock {
	let config = tangram_server_config::Clock {
		start: "2026-01-01T00:00:00Z".parse().unwrap(),
	};
	Clock::new(Some(&config), None)
}

#[tokio::test]
async fn sleep_fires_only_when_the_clock_reaches_its_deadline() {
	let clock = clock();
	let mut sleep = clock.sleep(Duration::from_secs(10));
	assert!((&mut sleep).now_or_never().is_none());
	clock
		.try_virtual()
		.unwrap()
		.advance(Duration::from_secs(9))
		.unwrap();
	assert!((&mut sleep).now_or_never().is_none());
	clock
		.try_virtual()
		.unwrap()
		.advance(Duration::from_secs(1))
		.unwrap();
	assert!(sleep.now_or_never().is_some());
}

#[tokio::test]
async fn advance_wakes_sleeps_in_deadline_order() {
	let clock = clock();
	let order = Arc::new(std::sync::Mutex::new(Vec::new()));
	let mut tasks = Vec::new();
	for seconds in [3, 1, 2] {
		let sleep = clock.sleep(Duration::from_secs(seconds));
		let order = order.clone();
		tasks.push(tokio::spawn(async move {
			sleep.await;
			order.lock().unwrap().push(seconds);
		}));
	}
	tokio::task::yield_now().await;
	clock
		.try_virtual()
		.unwrap()
		.advance(Duration::from_secs(5))
		.unwrap();
	for task in tasks {
		task.await.unwrap();
	}
	let order = order.lock().unwrap().clone();
	assert_eq!(order, vec![1, 2, 3]);
}

#[tokio::test]
async fn dropping_a_sleep_removes_its_waiter() {
	let clock = clock();
	let sleep = clock.sleep(Duration::from_secs(1));
	drop(sleep);
	let virtual_ = clock.try_virtual().unwrap();
	assert!(virtual_.state.lock().unwrap().waiters.is_empty());
}

#[tokio::test]
async fn setting_an_earlier_time_does_not_fire_timers() {
	let clock = clock();
	let mut sleep = clock.sleep(Duration::from_secs(1));
	let virtual_ = clock.try_virtual().unwrap();
	virtual_
		.set("2025-01-01T00:00:00Z".parse().unwrap())
		.unwrap();
	assert_eq!(clock.now(), "2025-01-01T00:00:00Z".parse().unwrap());
	assert!((&mut sleep).now_or_never().is_none());
	virtual_
		.set("2025-01-01T00:00:01Z".parse().unwrap())
		.unwrap();
	assert!(sleep.now_or_never().is_some());
}

#[tokio::test]
async fn timeout_elapses_on_advance() {
	let clock = clock();
	let timeout = clock.timeout(Duration::from_secs(5), futures::future::pending::<()>());
	let mut timeout = Box::pin(timeout);
	assert!(timeout.as_mut().now_or_never().is_none());
	clock
		.try_virtual()
		.unwrap()
		.advance(Duration::from_secs(5))
		.unwrap();
	assert!(timeout.now_or_never().unwrap().is_err());
}

#[tokio::test]
async fn interval_skips_missed_ticks() {
	let clock = clock();
	let mut interval = clock.interval(Duration::from_secs(1));
	interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
	let first = interval.tick().await;
	clock
		.try_virtual()
		.unwrap()
		.advance(Duration::from_millis(3500))
		.unwrap();
	let second = interval.tick().await;
	assert_eq!(second - first, Duration::from_secs(1));
	let mut third = Box::pin(interval.tick());
	assert!(third.as_mut().now_or_never().is_none());
	clock
		.try_virtual()
		.unwrap()
		.advance(Duration::from_millis(500))
		.unwrap();
	assert_eq!(third.await - first, Duration::from_secs(4));
}
