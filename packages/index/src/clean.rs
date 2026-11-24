use tangram_client::prelude::*;

#[cfg(test)]
mod tests;

#[derive(Clone, Debug)]
pub struct Arg {
	pub batch_size: usize,
	pub max_object_touched_at: i64,
	pub max_process_touched_at: i64,
	pub max_sandbox_touched_at: i64,
	pub now: i64,
	pub partition_end: u64,
	pub partition_start: u64,

	/// The margin before the creation of the oldest unfinished process, which is the largest time to touch of the items that the clean removes, because an item that a process uses may have been touched up to one time to touch before the process was created.
	pub time_to_touch: std::time::Duration,
}

#[derive(Clone, Debug, Default)]
pub struct Output {
	pub checkout_bytes: u64,
	pub checkout_files: u64,
	pub checkouts: Vec<Checkout>,
	pub done: bool,
	pub object_bytes: u64,
	pub objects: Vec<Object>,
	pub permissions: usize,
	pub processes: Vec<tg::process::Id>,
	pub sandboxes: Vec<tg::sandbox::Id>,

	/// The time after which the clean kept every item because a process created then is unfinished, if this lowered a cutoff that the caller passed.
	pub unfinished_cutoff: Option<i64>,
}

/// The latest touch times of the items that a clean removes, which the oldest unfinished process lowers, so that a clean never removes an item that was touched after the oldest unfinished process was created, less the time to touch.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Cutoffs {
	pub object: i64,
	pub process: i64,
	pub sandbox: i64,

	/// The cutoff of the oldest unfinished process, if it lowered a cutoff that the caller passed.
	pub unfinished: Option<i64>,
}

#[derive(Clone, Debug)]
pub struct Checkout {
	pub id: tg::Id,
	pub put: [u8; 16],
}

#[derive(Clone, Debug)]
pub struct Object {
	pub id: tg::object::Id,
	pub put: [u8; 16],
	pub touched_at: i64,
}

impl Output {
	/// Merge the unfinished cutoff of another clean, keeping the earliest.
	pub fn merge_unfinished_cutoff(&mut self, cutoff: Option<i64>) {
		self.unfinished_cutoff = match (self.unfinished_cutoff, cutoff) {
			(Some(existing), Some(cutoff)) => Some(existing.min(cutoff)),
			(existing, cutoff) => existing.or(cutoff),
		};
	}
}

impl Cutoffs {
	/// Lower the caller's cutoffs to the creation time of the oldest unfinished process, less the time to touch and one second, or keep them if no process is unfinished. The second keeps an item touched in the same whole second as the process's creation.
	#[must_use]
	pub fn new(
		max_object_touched_at: i64,
		max_process_touched_at: i64,
		max_sandbox_touched_at: i64,
		time_to_touch: std::time::Duration,
		oldest_unfinished_created_at: Option<i64>,
	) -> Self {
		let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap_or(i64::MAX);
		let unfinished = oldest_unfinished_created_at
			.map(|created_at| created_at.saturating_sub(time_to_touch).saturating_sub(1));
		let lower = |cutoff: i64| unfinished.map_or(cutoff, |unfinished| cutoff.min(unfinished));
		let object = lower(max_object_touched_at);
		let process = lower(max_process_touched_at);
		let sandbox = lower(max_sandbox_touched_at);
		let unfinished = unfinished.filter(|unfinished| {
			*unfinished < max_object_touched_at
				|| *unfinished < max_process_touched_at
				|| *unfinished < max_sandbox_touched_at
		});
		Self {
			object,
			process,
			sandbox,
			unfinished,
		}
	}
}
