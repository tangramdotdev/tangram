use std::time::{Duration, Instant};

pub(crate) async fn with_timeout_logging<F, T>(future: F, threshold: Duration, name: &str) -> T
where
	F: Future<Output = T>,
{
	let start = Instant::now();

	let timeout_future = tokio::spawn({
		let name = name.to_owned();
		async move {
			tokio::time::sleep(threshold).await;
			tracing::warn!("Operation '{name}' is taking longer than threshold: {threshold:?}");
		}
	});

	let result = future.await;
	timeout_future.abort();
	let elapsed = start.elapsed();
	if elapsed > threshold {
		tracing::debug!(?elapsed, "Operation '{name}' completed");
	}
	result
}
