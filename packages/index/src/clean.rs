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

/// Get the cutoff that the oldest unfinished process sets, which is its creation time, less the time to touch and one second, so that a clean keeps every item that was touched after the process was created, less the time to touch. The second keeps an item that was touched in the same whole second as the process's creation.
#[must_use]
pub fn unfinished_cutoff(
	oldest_unfinished_created_at: Option<i64>,
	time_to_touch: std::time::Duration,
) -> Option<i64> {
	let time_to_touch = i64::try_from(time_to_touch.as_secs()).unwrap_or(i64::MAX);
	oldest_unfinished_created_at
		.map(|created_at| created_at.saturating_sub(time_to_touch).saturating_sub(1))
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
