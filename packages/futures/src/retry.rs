use {
	num::ToPrimitive as _,
	std::{ops::ControlFlow, time::Duration},
};

#[derive(Clone, Debug)]
pub struct Options {
	pub backoff: Duration,
	pub jitter: Duration,
	pub max_delay: Duration,
	pub max_retries: u64,
}

pub async fn retry<F, Fut, T, E>(options: &Options, mut f: F) -> Result<T, E>
where
	F: FnMut() -> Fut,
	Fut: Future<Output = Result<ControlFlow<T, E>, E>>,
{
	let mut attempt = 0;
	loop {
		match f().await {
			Ok(ControlFlow::Break(value)) => {
				return Ok(value);
			},
			Ok(ControlFlow::Continue(_)) if attempt < options.max_retries => {
				attempt += 1;
				let delay = delay_for_attempt(attempt, options);
				tokio::time::sleep(delay).await;
			},
			Ok(ControlFlow::Continue(error)) | Err(error) => {
				return Err(error);
			},
		}
	}
}

fn delay_for_attempt(attempt: u64, options: &Options) -> Duration {
	let jitter = Duration::from_millis(rand::random_range(
		0..=options.jitter.as_millis().to_u64().unwrap(),
	));
	(options.backoff * (1 << attempt) + jitter).min(options.max_delay)
}

impl Default for Options {
	fn default() -> Self {
		Self {
			backoff: Duration::from_millis(10),
			jitter: Duration::from_millis(10),
			max_delay: Duration::from_secs(1),
			max_retries: 3,
		}
	}
}
