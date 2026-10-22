#![allow(
	dead_code,
	reason = "the timers are used once the policy timers are converted to the clock"
)]

use {
	crate::Session,
	std::{
		collections::BTreeMap,
		pin::Pin,
		sync::{Arc, Mutex},
		task::{Context, Poll},
		time::Duration,
	},
	tangram_client::prelude::*,
};

mod advance;
mod get;
mod set;
#[cfg(test)]
mod tests;

/// A server's source of time, which is either the real clock or a virtual clock that only moves when it is advanced.
#[derive(Clone)]
pub(crate) enum Clock {
	Real(Real),
	Virtual(Arc<Virtual>),
}

#[derive(Clone)]
pub(crate) struct Real {
	origin: tokio::time::Instant,
}

pub(crate) struct Virtual {
	state: Mutex<VirtualState>,
}

struct VirtualState {
	elapsed: Duration,
	next: u64,
	now: jiff::Timestamp,
	waiters: BTreeMap<(Duration, u64), tokio::sync::oneshot::Sender<()>>,
}

/// A monotonic point in time, measured from the clock's origin.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) struct Instant(Duration);

/// A future that completes when its clock reaches a deadline.
pub(crate) struct Sleep {
	inner: SleepInner,
}

enum SleepInner {
	Real(Pin<Box<tokio::time::Sleep>>),
	Virtual(VirtualSleep),
}

struct VirtualSleep {
	clock: Arc<Virtual>,
	key: Option<(Duration, u64)>,
	receiver: Option<tokio::sync::oneshot::Receiver<()>>,
}

/// A periodic timer on a clock.
pub(crate) struct Interval {
	inner: IntervalInner,
}

enum IntervalInner {
	Real {
		interval: tokio::time::Interval,
		origin: tokio::time::Instant,
	},
	Virtual {
		clock: Arc<Virtual>,
		missed_tick_behavior: tokio::time::MissedTickBehavior,
		next: Instant,
		period: Duration,
	},
}

/// The error returned when a timeout elapses.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Elapsed;

impl Clock {
	#[must_use]
	pub fn new(config: Option<&crate::config::Clock>) -> Self {
		let Some(config) = config else {
			let origin = tokio::time::Instant::now();
			return Self::Real(Real { origin });
		};
		let state = VirtualState {
			elapsed: Duration::ZERO,
			next: 0,
			now: config.start,
			waiters: BTreeMap::new(),
		};
		let clock = Virtual {
			state: Mutex::new(state),
		};
		Self::Virtual(Arc::new(clock))
	}

	#[must_use]
	pub fn now(&self) -> jiff::Timestamp {
		match self {
			Self::Real(_) => jiff::Timestamp::now(),
			Self::Virtual(clock) => clock.now(),
		}
	}

	#[must_use]
	pub fn unix_timestamp(&self) -> i64 {
		self.now().as_second()
	}

	#[must_use]
	pub fn instant(&self) -> Instant {
		match self {
			Self::Real(clock) => Instant(clock.origin.elapsed()),
			Self::Virtual(clock) => Instant(clock.state.lock().unwrap().elapsed),
		}
	}

	#[must_use]
	pub fn sleep(&self, duration: Duration) -> Sleep {
		let deadline = self.instant() + duration;
		self.sleep_until(deadline)
	}

	#[must_use]
	pub fn sleep_until(&self, deadline: Instant) -> Sleep {
		match self {
			Self::Real(clock) => {
				let deadline = clock.origin + deadline.0;
				let sleep = Box::pin(tokio::time::sleep_until(deadline));
				Sleep {
					inner: SleepInner::Real(sleep),
				}
			},
			Self::Virtual(clock) => clock.sleep_until(deadline),
		}
	}

	pub async fn timeout<F>(&self, duration: Duration, future: F) -> Result<F::Output, Elapsed>
	where
		F: Future,
	{
		let deadline = self.instant() + duration;
		self.timeout_at(deadline, future).await
	}

	pub async fn timeout_at<F>(&self, deadline: Instant, future: F) -> Result<F::Output, Elapsed>
	where
		F: Future,
	{
		let sleep = self.sleep_until(deadline);
		tokio::select! {
			biased;
			output = future => Ok(output),
			() = sleep => Err(Elapsed),
		}
	}

	#[must_use]
	pub fn interval(&self, period: Duration) -> Interval {
		let start = self.instant();
		self.interval_at(start, period)
	}

	#[must_use]
	pub fn interval_at(&self, start: Instant, period: Duration) -> Interval {
		assert!(
			period > Duration::ZERO,
			"the interval period must be positive"
		);
		match self {
			Self::Real(clock) => {
				let interval = tokio::time::interval_at(clock.origin + start.0, period);
				Interval {
					inner: IntervalInner::Real {
						interval,
						origin: clock.origin,
					},
				}
			},
			Self::Virtual(clock) => Interval {
				inner: IntervalInner::Virtual {
					clock: clock.clone(),
					missed_tick_behavior: tokio::time::MissedTickBehavior::Burst,
					next: start,
					period,
				},
			},
		}
	}

	#[must_use]
	pub fn try_virtual(&self) -> Option<&Arc<Virtual>> {
		match self {
			Self::Real(_) => None,
			Self::Virtual(clock) => Some(clock),
		}
	}
}

impl Virtual {
	#[must_use]
	pub fn now(&self) -> jiff::Timestamp {
		self.state.lock().unwrap().now
	}

	/// Advance the clock, then wake the sleeps whose deadlines it reached, in deadline order.
	pub fn advance(&self, duration: Duration) -> tg::Result<jiff::Timestamp> {
		// Move the clock and take the waiters that are due.
		let (now, senders) = {
			let mut state = self.state.lock().unwrap();
			let now = state
				.now
				.checked_add(duration)
				.map_err(|error| tg::error!(!error, "failed to advance the clock"))?;
			state.now = now;
			state.elapsed += duration;
			let key = (state.elapsed + Duration::from_nanos(1), 0);
			let pending = state.waiters.split_off(&key);
			let due = std::mem::replace(&mut state.waiters, pending);
			(now, due.into_values().collect::<Vec<_>>())
		};

		// Wake the due sleeps.
		for sender in senders {
			sender.send(()).ok();
		}

		Ok(now)
	}

	/// Set the clock's wall time. A later time advances the clock and wakes the sleeps it reaches, and an earlier time only changes the wall time.
	pub fn set(&self, now: jiff::Timestamp) -> tg::Result<jiff::Timestamp> {
		let current = self.now();
		if now < current {
			self.state.lock().unwrap().now = now;
			return Ok(now);
		}
		let duration = now
			.duration_since(current)
			.try_into()
			.map_err(|error| tg::error!(!error, "failed to compute the clock advance"))?;
		self.advance(duration)
	}

	fn sleep_until(self: &Arc<Self>, deadline: Instant) -> Sleep {
		let mut state = self.state.lock().unwrap();
		if deadline.0 <= state.elapsed {
			return Sleep {
				inner: SleepInner::Virtual(VirtualSleep {
					clock: self.clone(),
					key: None,
					receiver: None,
				}),
			};
		}
		let key = (deadline.0, state.next);
		state.next += 1;
		let (sender, receiver) = tokio::sync::oneshot::channel();
		state.waiters.insert(key, sender);
		Sleep {
			inner: SleepInner::Virtual(VirtualSleep {
				clock: self.clone(),
				key: Some(key),
				receiver: Some(receiver),
			}),
		}
	}
}

impl Interval {
	pub async fn tick(&mut self) -> Instant {
		match &mut self.inner {
			IntervalInner::Real { interval, origin } => {
				let instant = interval.tick().await;
				Instant(instant.saturating_duration_since(*origin))
			},
			IntervalInner::Virtual {
				clock,
				missed_tick_behavior,
				next,
				period,
			} => {
				// Wait for the next tick.
				let deadline = *next;
				clock.sleep_until(deadline).await;

				// Schedule the following tick.
				let now = Instant(clock.state.lock().unwrap().elapsed);
				*next = match missed_tick_behavior {
					tokio::time::MissedTickBehavior::Burst => deadline + *period,
					tokio::time::MissedTickBehavior::Delay => now + *period,
					tokio::time::MissedTickBehavior::Skip => {
						let mut next = deadline + *period;
						while next <= now {
							next = next + *period;
						}
						next
					},
				};

				deadline
			},
		}
	}

	pub fn reset(&mut self) {
		match &mut self.inner {
			IntervalInner::Real { interval, .. } => interval.reset(),
			IntervalInner::Virtual {
				clock,
				next,
				period,
				..
			} => {
				*next = Instant(clock.state.lock().unwrap().elapsed) + *period;
			},
		}
	}

	pub fn set_missed_tick_behavior(&mut self, behavior: tokio::time::MissedTickBehavior) {
		match &mut self.inner {
			IntervalInner::Real { interval, .. } => interval.set_missed_tick_behavior(behavior),
			IntervalInner::Virtual {
				missed_tick_behavior,
				..
			} => {
				*missed_tick_behavior = behavior;
			},
		}
	}
}

impl Session {
	fn virtual_clock(&self) -> tg::Result<Option<&Arc<Virtual>>> {
		self.verify_request_from_host()?;
		if !matches!(self.context.principal, tg::Principal::Root) {
			return Err(tg::error!("unauthorized"));
		}
		Ok(self.server.clock.try_virtual())
	}
}

impl std::ops::Add<Duration> for Instant {
	type Output = Self;

	fn add(self, duration: Duration) -> Self {
		Self(self.0 + duration)
	}
}

impl std::ops::Sub for Instant {
	type Output = Duration;

	fn sub(self, other: Self) -> Duration {
		self.0.saturating_sub(other.0)
	}
}

impl Future for Sleep {
	type Output = ();

	fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
		match &mut self.inner {
			SleepInner::Real(sleep) => sleep.as_mut().poll(cx),
			SleepInner::Virtual(sleep) => {
				let Some(receiver) = sleep.receiver.as_mut() else {
					return Poll::Ready(());
				};
				match Pin::new(receiver).poll(cx) {
					Poll::Ready(_) => {
						sleep.receiver = None;
						sleep.key = None;
						Poll::Ready(())
					},
					Poll::Pending => Poll::Pending,
				}
			},
		}
	}
}

impl Drop for VirtualSleep {
	fn drop(&mut self) {
		if let Some(key) = self.key.take() {
			self.clock.state.lock().unwrap().waiters.remove(&key);
		}
	}
}

impl std::fmt::Display for Elapsed {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "deadline has elapsed")
	}
}

impl std::error::Error for Elapsed {}
