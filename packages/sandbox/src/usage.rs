use {
	std::time::{Duration, Instant},
	tangram_client::prelude::*,
};

#[cfg(target_os = "linux")]
mod linux;

#[cfg(target_os = "linux")]
pub(crate) use linux::Source;

pub(crate) struct Accounting {
	requests:
		tokio::sync::mpsc::Sender<tokio::sync::oneshot::Sender<tg::Result<tg::sandbox::Usage>>>,
	result: Option<tg::Result<tg::sandbox::Usage>>,
	stop: tokio::sync::watch::Sender<bool>,
	task: Option<tokio::task::JoinHandle<tg::Result<tg::sandbox::Usage>>>,
}

#[derive(Default)]
pub(crate) struct Snapshot {
	pub(crate) cpu: u64,
	pub(crate) memory: u64,
	pub(crate) storage: u64,
	pub(crate) swap: u64,
}

struct Accumulator {
	allocated_at: Instant,
	consumption: u128,
	cpu: u128,
	cpu_duration: u128,
	memory: u128,
	previous: Snapshot,
	previous_at: Instant,
	resources: tg::runner::Capacity,
	storage: u128,
	swap: u128,
}

impl Accounting {
	pub(crate) fn new(
		source: Source,
		resources: tg::runner::Capacity,
		interval: Duration,
	) -> tg::Result<Self> {
		Self::with_source(move || source.snapshot(), resources, interval)
	}

	fn with_source(
		source: impl Fn() -> tg::Result<Snapshot> + Send + 'static,
		resources: tg::runner::Capacity,
		interval: Duration,
	) -> tg::Result<Self> {
		// Validate the sampling interval.
		if interval.is_zero() {
			return Err(tg::error!(
				"the memory sampling interval must be greater than zero"
			));
		}

		// Initialize the usage accumulator.
		let initial = source()?;
		let started_at = Instant::now();
		let mut accumulator = Accumulator::new(resources, initial, started_at);

		// Create the control channels.
		let (requests, mut request_receiver) = tokio::sync::mpsc::channel::<
			tokio::sync::oneshot::Sender<tg::Result<tg::sandbox::Usage>>,
		>(1);
		let (stop, mut receiver) = tokio::sync::watch::channel(false);

		// Start the sampling task.
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
			accumulator.finish_cpu()?;
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
	fn new(resources: tg::runner::Capacity, initial: Snapshot, started_at: Instant) -> Self {
		Self {
			allocated_at: started_at,
			consumption: 0,
			cpu: 0,
			cpu_duration: 0,
			memory: 0,
			previous: initial,
			previous_at: started_at,
			resources,
			storage: 0,
			swap: 0,
		}
	}

	fn sample(&mut self, snapshot: Snapshot, at: Instant) -> tg::Result<()> {
		// Sample the CPU consumption.
		let duration = at.duration_since(self.previous_at).as_nanos();
		let cpu = snapshot
			.cpu
			.checked_sub(self.previous.cpu)
			.ok_or_else(|| tg::error!("the CPU counter decreased"))?;
		self.sample_cpu(u128::from(cpu) * 1000, duration)?;

		// Sample the memory, storage, and swap consumption.
		for (accumulated, previous, current, minimum) in [
			(
				&mut self.memory,
				self.previous
					.memory
					.min(self.resources.memory.max.unwrap_or(u64::MAX)),
				snapshot
					.memory
					.min(self.resources.memory.max.unwrap_or(u64::MAX)),
				self.resources.memory.min,
			),
			(
				&mut self.storage,
				self.previous
					.storage
					.min(self.resources.storage.max.unwrap_or(u64::MAX)),
				snapshot
					.storage
					.min(self.resources.storage.max.unwrap_or(u64::MAX)),
				self.resources.storage.min,
			),
			(
				&mut self.swap,
				self.previous
					.swap
					.min(self.resources.swap.max.unwrap_or(u64::MAX)),
				snapshot
					.swap
					.min(self.resources.swap.max.unwrap_or(u64::MAX)),
				self.resources.swap.min,
			),
		] {
			let area = integral(previous, current, minimum, duration)?;
			*accumulated = accumulated
				.checked_add(area)
				.ok_or_else(|| tg::error!("the resource consumption overflowed"))?;
		}

		// Update the previous usage snapshot.
		self.previous = snapshot;
		self.previous_at = at;

		Ok(())
	}

	fn sample_cpu(&mut self, mut cpu: u128, mut duration: u128) -> tg::Result<()> {
		while duration > 0 {
			let chunk = duration.min(1_000_000_000 - self.cpu_duration);
			let consumed = if chunk == duration {
				cpu
			} else {
				cpu.checked_mul(chunk)
					.ok_or_else(|| tg::error!("the CPU usage overflowed"))?
					/ duration
			};
			self.cpu = self
				.cpu
				.checked_add(consumed)
				.ok_or_else(|| tg::error!("the CPU usage overflowed"))?;
			self.cpu_duration += chunk;
			cpu -= consumed;
			duration -= chunk;
			if self.cpu_duration == 1_000_000_000 {
				self.finish_cpu()?;
			}
		}
		Ok(())
	}

	fn finish_cpu(&mut self) -> tg::Result<()> {
		let minimum = u128::from(self.resources.cpu.min.millis()) * self.cpu_duration;
		let consumed = if Some(self.resources.cpu.min) == self.resources.cpu.max {
			0
		} else {
			self.cpu.saturating_sub(minimum)
		};
		self.consumption = self
			.consumption
			.checked_add(consumed)
			.ok_or_else(|| tg::error!("the CPU usage overflowed"))?;
		self.cpu = 0;
		self.cpu_duration = 0;
		Ok(())
	}

	fn usage(&self) -> tg::Result<tg::sandbox::Usage> {
		let consumption = u64::try_from(self.consumption.div_ceil(1_000_000_000))
			.map_err(|_| tg::error!("the CPU consumption is too large"))?;
		let reservation = u128::from(self.resources.cpu.min.millis())
			* self
				.previous_at
				.duration_since(self.allocated_at)
				.as_nanos();
		let reservation = u64::try_from(reservation.div_ceil(1_000_000_000))
			.map_err(|_| tg::error!("the CPU reservation is too large"))?;

		let duration = self
			.previous_at
			.duration_since(self.allocated_at)
			.as_nanos();
		let cpu = tg::sandbox::ResourceUsage {
			consumption,
			reservation,
		};
		let memory = byte_usage(self.memory, self.resources.memory.min, duration)?;
		let storage = byte_usage(self.storage, self.resources.storage.min, duration)?;
		let swap = byte_usage(self.swap, self.resources.swap.min, duration)?;

		Ok(tg::sandbox::Usage {
			cpu,
			memory,
			storage,
			swap,
		})
	}
}

fn integral(previous: u64, current: u64, minimum: u64, duration: u128) -> tg::Result<u128> {
	let low = previous.min(current);
	let high = previous.max(current);
	if high <= minimum {
		return Ok(0);
	}
	let area = if low >= minimum {
		u128::from(low - minimum) + u128::from(high - minimum)
	} else {
		let excess = u128::from(high - minimum);
		return excess
			.checked_mul(excess)
			.and_then(|area| area.checked_mul(duration))
			.map(|area| area / u128::from(high - low))
			.ok_or_else(|| tg::error!("the resource consumption overflowed"));
	};
	area.checked_mul(duration)
		.ok_or_else(|| tg::error!("the resource consumption overflowed"))
}

fn byte_usage(
	consumption: u128,
	minimum: u64,
	duration: u128,
) -> tg::Result<tg::sandbox::ResourceUsage> {
	let consumption = u64::try_from(consumption.div_ceil(2 * 1_048_576 * 1_000_000))
		.map_err(|_| tg::error!("the resource consumption is too large"))?;
	let reservation = u128::from(minimum)
		.checked_mul(duration)
		.ok_or_else(|| tg::error!("the resource reservation overflowed"))?
		.div_ceil(1_048_576 * 1_000_000);
	let reservation = u64::try_from(reservation)
		.map_err(|_| tg::error!("the resource reservation is too large"))?;
	Ok(tg::sandbox::ResourceUsage {
		consumption,
		reservation,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn charges_the_minimum_and_clips_each_consumption_interval() {
		// Create the usage accumulator.
		let started_at = Instant::now();
		let cpu = tg::sandbox::Cpu::new(
			tg::sandbox::Amount::from_millis(250),
			tg::sandbox::Amount::from_millis(1000),
		);
		let resources = tg::runner::Capacity {
			cpu,
			..Default::default()
		};
		let mut accumulator = Accumulator::new(resources, Snapshot::default(), started_at);

		// Sample the consumption intervals.
		accumulator
			.sample(
				Snapshot {
					cpu: 100_000_000,
					..Default::default()
				},
				started_at + Duration::from_secs(1),
			)
			.unwrap();
		accumulator
			.sample(
				Snapshot {
					cpu: 900_000_000,
					..Default::default()
				},
				started_at + Duration::from_secs(2),
			)
			.unwrap();

		// Verify the charged usage.
		let usage = accumulator.usage().unwrap();
		assert_eq!(usage.cpu.reservation, 500);
		assert_eq!(usage.cpu.consumption, 550);
	}

	#[test]
	fn excludes_cpu_consumed_before_assignment() {
		let started_at = Instant::now();
		let resources = tg::runner::Capacity {
			cpu: tg::sandbox::Amount::from_millis(1000).into(),
			..Default::default()
		};
		let initial = Snapshot {
			cpu: 10_000_000_000,
			..Default::default()
		};
		let snapshot = Snapshot {
			cpu: initial.cpu + 100_000_000,
			..Default::default()
		};
		let mut accumulator = Accumulator::new(resources, initial, started_at);
		accumulator
			.sample(snapshot, started_at + Duration::from_secs(1))
			.unwrap();
		let usage = accumulator.usage().unwrap();
		assert_eq!(usage.cpu.consumption, 100);
		assert_eq!(usage.cpu.reservation, 0);
	}

	#[test]
	fn fractional_cpu_bursts_are_billed_over_bandwidth_windows() {
		for (maximum, expected) in [(250, 0), (500, 250)] {
			let started_at = Instant::now();
			let cpu = tg::sandbox::Cpu::new(
				tg::sandbox::Amount::from_millis(250),
				tg::sandbox::Amount::from_millis(maximum),
			);
			let resources = tg::runner::Capacity {
				cpu,
				..Default::default()
			};
			let mut accumulator = Accumulator::new(resources, Snapshot::default(), started_at);
			for sample in 1..=20 {
				let snapshot = Snapshot {
					cpu: (sample * 50_000_000_u64).min(maximum * 1_000_000),
					..Default::default()
				};
				accumulator
					.sample(snapshot, started_at + Duration::from_millis(sample * 50))
					.unwrap();
			}
			let usage = accumulator.usage().unwrap();
			assert_eq!(usage.cpu.reservation, 250);
			assert_eq!(usage.cpu.consumption, expected);
		}
	}

	#[test]
	fn short_cpu_bursts_preserve_actual_execution() {
		let start = Instant::now();
		let resources = tg::runner::Capacity {
			cpu: tg::sandbox::Amount::from_millis(250).into(),
			..Default::default()
		};
		let mut accumulator = Accumulator::new(resources, Snapshot::default(), start);
		let snapshot = Snapshot {
			cpu: 100_000_000,
			..Default::default()
		};
		accumulator
			.sample(snapshot, start + Duration::from_millis(100))
			.unwrap();
		accumulator.finish_cpu().unwrap();
		assert_eq!(accumulator.usage().unwrap().cpu.consumption, 100);
	}

	#[test]
	fn fixed_byte_reservations_do_not_also_bill_consumption() {
		let started_at = Instant::now();
		let resource = tg::sandbox::Resource::new(1_048_576, 1_048_576);
		let resources = tg::runner::Capacity {
			memory: resource,
			storage: resource,
			swap: resource,
			..Default::default()
		};
		let mut accumulator = Accumulator::new(resources, Snapshot::default(), started_at);
		let snapshot = Snapshot {
			memory: 2_097_152,
			storage: 2_097_152,
			swap: 2_097_152,
			..Default::default()
		};
		accumulator
			.sample(snapshot, started_at + Duration::from_secs(1))
			.unwrap();
		let usage = accumulator.usage().unwrap();
		for resource in [usage.memory, usage.storage, usage.swap] {
			assert_eq!(resource.reservation, 1000);
			assert_eq!(resource.consumption, 0);
		}
	}

	#[test]
	fn integrates_only_the_triangle_above_a_crossed_minimum() {
		assert_eq!(integral(0, 4, 2, 10).unwrap(), 10);
		assert_eq!(integral(4, 0, 2, 10).unwrap(), 10);
		assert_eq!(integral(0, 1, 2, 10).unwrap(), 0);
		assert_eq!(integral(3, 4, 2, 10).unwrap(), 30);
	}
}
