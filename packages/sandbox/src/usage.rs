use {
	std::time::{Duration, Instant},
	tangram_client::prelude::*,
};

#[cfg(target_os = "macos")]
mod darwin;
#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "macos")]
pub(crate) use darwin::Source;
#[cfg(target_os = "linux")]
pub(crate) use linux::Source;

pub(crate) struct Accounting {
	requests:
		tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<tg::Result<tg::sandbox::Usage>>>,
	result: Option<tg::Result<tg::sandbox::Usage>>,
	stop: tokio::sync::watch::Sender<bool>,
	task: Option<tokio::task::JoinHandle<tg::Result<tg::sandbox::Usage>>>,
}

pub(crate) struct Snapshot {
	pub(crate) cpu: u64,
	pub(crate) memory: u64,
}

struct Accumulator {
	cpu: tg::sandbox::Cpu,
	dedicated_started_at: Instant,
	initial_cpu: u64,
	memory: u128,
	previous: Snapshot,
	previous_at: Instant,
}

impl Accounting {
	pub(crate) fn new(
		source: Source,
		cpu: tg::sandbox::Cpu,
		interval: Duration,
		dedicated_started_at: Instant,
	) -> tg::Result<Self> {
		Self::with_source(
			move || source.snapshot(),
			cpu,
			interval,
			dedicated_started_at,
		)
	}

	fn with_source(
		source: impl Fn() -> tg::Result<Snapshot> + Send + 'static,
		cpu: tg::sandbox::Cpu,
		interval: Duration,
		dedicated_started_at: Instant,
	) -> tg::Result<Self> {
		if interval.is_zero() {
			return Err(tg::error!(
				"the memory sampling interval must be greater than zero"
			));
		}
		let initial = source()?;
		let started_at = Instant::now();
		let mut accumulator = Accumulator::new(cpu, initial, started_at);
		accumulator.dedicated_started_at = dedicated_started_at;
		let (requests, mut request_receiver) = tokio::sync::mpsc::channel::<
			tokio::sync::oneshot::Sender<tg::Result<tg::sandbox::Usage>>,
		>(1);
		let (stop, mut receiver) = tokio::sync::watch::channel(false);
		let task = tokio::spawn(async move {
			let mut timer =
				tokio::time::interval_at(tokio::time::Instant::now() + interval, interval);
			timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
			loop {
				tokio::select! {
				 biased;
				 _ = receiver.changed() => break,
				 Some(sender) = request_receiver.recv() => {
					accumulator.sample(source()?, Instant::now())?;
					sender.send(accumulator.usage()).ok();
				 },
				 _ = timer.tick() => accumulator.sample(source()?, Instant::now())?,
				}
			}
			accumulator.sample(source()?, Instant::now())?;
			let usage = accumulator.usage()?;
			Ok(usage)
		});
		Ok(Self {
			requests,
			result: None,
			stop,
			task: Some(task),
		})
	}

	pub(crate) async fn usage(&mut self) -> tg::Result<tg::sandbox::Usage> {
		if let Some(result) = &self.result {
			return result.clone();
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		if self.requests.send(sender).await.is_err() {
			return self.finish().await;
		}
		let Ok(result) = receiver.await else {
			return self.finish().await;
		};
		result
	}

	pub(crate) async fn finish(&mut self) -> tg::Result<tg::sandbox::Usage> {
		if let Some(task) = self.task.as_mut() {
			self.stop.send_replace(true);
			let result = task
				.await
				.map_err(|error| tg::error!(!error, "the sandbox accounting task panicked"))
				.and_then(std::convert::identity);
			self.task.take();
			self.result = Some(result);
		}
		self.result
			.as_ref()
			.ok_or_else(|| tg::error!("the sandbox accounting result is missing"))?
			.clone()
	}
}

impl Drop for Accounting {
	fn drop(&mut self) {
		if let Some(task) = self.task.take() {
			task.abort();
		}
	}
}

impl Accumulator {
	fn new(cpu: tg::sandbox::Cpu, initial: Snapshot, started_at: Instant) -> Self {
		Self {
			cpu,
			dedicated_started_at: started_at,
			initial_cpu: initial.cpu,
			memory: 0,
			previous: initial,
			previous_at: started_at,
		}
	}

	fn sample(&mut self, snapshot: Snapshot, at: Instant) -> tg::Result<()> {
		// Integrate adjacent memory samples using the actual elapsed time.
		let bytes = u128::from(self.previous.memory) + u128::from(snapshot.memory);
		let duration = at.duration_since(self.previous_at).as_nanos();
		let memory = bytes
			.checked_mul(duration)
			.ok_or_else(|| tg::error!("the sandbox memory usage overflowed"))?;
		self.memory = self
			.memory
			.checked_add(memory)
			.ok_or_else(|| tg::error!("the sandbox memory usage overflowed"))?;
		self.previous = snapshot;
		self.previous_at = at;
		Ok(())
	}

	fn usage(&self) -> tg::Result<tg::sandbox::Usage> {
		let shared = self
			.previous
			.cpu
			.checked_sub(self.initial_cpu)
			.ok_or_else(|| tg::error!("the sandbox CPU counter decreased"))?;
		let shared = if self.cpu.shared == 0 {
			0
		} else {
			shared.div_ceil(1_000_000)
		};
		let dedicated = u128::from(self.cpu.dedicated)
			.checked_mul(
				self.previous_at
					.duration_since(self.dedicated_started_at)
					.as_nanos(),
			)
			.ok_or_else(|| tg::error!("the dedicated CPU usage overflowed"))?
			.div_ceil(1_000_000);
		let dedicated = u64::try_from(dedicated)
			.map_err(|_| tg::error!("the dedicated CPU usage is too large"))?;
		let memory = self.memory.div_ceil(2 * 1_048_576 * 1_000_000);
		let memory = u64::try_from(memory)
			.map_err(|_| tg::error!("the sandbox memory usage is too large"))?;
		let cpu = tg::sandbox::Cpu { dedicated, shared };
		Ok(tg::sandbox::Usage { cpu, memory })
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn current_usage_keeps_accounting_running_and_final_usage_is_cached() {
		let counter = std::sync::atomic::AtomicU64::new(0);
		let source = move || {
			let snapshot = Snapshot {
				cpu: counter.fetch_add(500_000_000, std::sync::atomic::Ordering::Relaxed),
				memory: 1_048_576,
			};
			Ok(snapshot)
		};
		let mut accounting =
			Accounting::with_source(source, 1.into(), Duration::from_secs(3600), Instant::now())
				.unwrap();
		assert_eq!(accounting.usage().await.unwrap().cpu.shared, 500);
		assert_eq!(accounting.usage().await.unwrap().cpu.shared, 1000);
		let usage = accounting.finish().await.unwrap();
		assert_eq!(usage.cpu.shared, 1500);
		let current = accounting.usage().await.unwrap();
		assert_eq!(current.cpu, usage.cpu);
		assert_eq!(current.memory, usage.memory);
		let finalized = accounting.finish().await.unwrap();
		assert_eq!(finalized.cpu, usage.cpu);
		assert_eq!(finalized.memory, usage.memory);
	}

	#[tokio::test]
	async fn current_usage_preserves_sampling_errors() {
		let first = std::sync::atomic::AtomicBool::new(true);
		let source = move || {
			if !first.swap(false, std::sync::atomic::Ordering::Relaxed) {
				return Err(tg::error!("the usage source failed"));
			}
			let snapshot = Snapshot { cpu: 0, memory: 0 };
			Ok(snapshot)
		};
		let mut accounting =
			Accounting::with_source(source, 1.into(), Duration::from_secs(3600), Instant::now())
				.unwrap();
		assert!(accounting.usage().await.is_err());
		assert!(accounting.usage().await.is_err());
		assert!(accounting.finish().await.is_err());
	}

	#[test]
	fn dedicated_time_includes_allocation_before_claim() {
		let allocated_at = Instant::now();
		let claimed_at = allocated_at + Duration::from_secs(2);
		let cpu = tg::sandbox::Cpu {
			dedicated: 1,
			shared: 0,
		};
		let initial = Snapshot {
			cpu: 500_000_000,
			memory: 1_048_576,
		};
		let mut accumulator = Accumulator::new(cpu, initial, claimed_at);
		accumulator.dedicated_started_at = allocated_at;
		accumulator
			.sample(
				Snapshot {
					cpu: 600_000_000,
					memory: 1_048_576,
				},
				claimed_at + Duration::from_secs(1),
			)
			.unwrap();
		let usage = accumulator.usage().unwrap();
		assert_eq!(usage.cpu.shared, 0);
		assert_eq!(usage.cpu.dedicated, 3000);
		assert_eq!(usage.memory, 1000);
	}

	#[test]
	fn integrates_memory_and_counts_only_cpu_execution() {
		let started_at = Instant::now();
		let cpu = tg::sandbox::Cpu {
			dedicated: 2,
			shared: 4,
		};
		let initial = Snapshot {
			cpu: 100,
			memory: 1_048_576,
		};
		let mut accumulator = Accumulator::new(cpu, initial, started_at);
		accumulator
			.sample(
				Snapshot {
					cpu: 500_000_100,
					memory: 3 * 1_048_576,
				},
				started_at + Duration::from_secs(2),
			)
			.unwrap();
		accumulator
			.sample(
				Snapshot {
					cpu: 750_000_100,
					memory: 1_048_576,
				},
				started_at + Duration::from_secs(3),
			)
			.unwrap();
		let usage = accumulator.usage().unwrap();
		assert_eq!(usage.cpu.shared, 750);
		assert_eq!(usage.cpu.dedicated, 6000);
		assert_eq!(usage.memory, 6000);
	}
	#[test]
	fn rounds_once_after_accumulating_partial_intervals() {
		let started_at = Instant::now();
		let initial = Snapshot {
			cpu: 0,
			memory: 1_048_576,
		};
		let mut accumulator = Accumulator::new(1.into(), initial, started_at);
		for index in 1..=10 {
			accumulator
				.sample(
					Snapshot {
						cpu: index * 100_000,
						memory: 1_048_576,
					},
					started_at + Duration::from_micros(index * 100),
				)
				.unwrap();
		}
		let usage = accumulator.usage().unwrap();
		assert_eq!(usage.cpu.shared, 1);
		assert_eq!(usage.memory, 1);
	}
}
