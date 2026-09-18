use {super::ControlConnectionPool, std::time::Duration};

#[tokio::test]
async fn pool_adds_a_replacement_after_take() {
	let pool = ControlConnectionPool::new("test", 1, Duration::from_secs(60));
	let count = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
	pool.start({
		let count = count.clone();
		move || {
			let value = count.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
			async move { Ok(value) }
		}
	});
	for expected in 0..3 {
		tokio::time::timeout(Duration::from_secs(1), async {
			while pool.pool.lock().unwrap().as_ref().unwrap().available() == 0 {
				tokio::task::yield_now().await;
			}
		})
		.await
		.unwrap();
		assert_eq!(pool.take().await.unwrap(), expected);
	}
	pool.shutdown().await;
}

#[tokio::test]
async fn pool_shutdown_cancels_connection_creation() {
	let pool = ControlConnectionPool::<()>::new("test", 1, Duration::from_secs(5));
	let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
	pool.start(move || {
		let sender = sender.clone();
		async move {
			sender.send(()).unwrap();
			std::future::pending().await
		}
	});
	receiver.recv().await.unwrap();
	tokio::time::timeout(Duration::from_secs(1), pool.shutdown())
		.await
		.unwrap();
}

#[tokio::test]
async fn pool_zero_ttl_only_creates_connections_on_demand() {
	let pool = ControlConnectionPool::new("test", 1, Duration::ZERO);
	pool.start(|| async { Ok(()) });
	assert!(pool.task.lock().unwrap().is_none());
	pool.take().await.unwrap();
	pool.take().await.unwrap();
	pool.shutdown().await;
}
