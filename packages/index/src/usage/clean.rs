use std::time::Duration;

#[derive(Clone, Debug)]
pub struct Arg {
	pub batch_size: usize,
	pub day_time_to_live: Duration,
	pub delta_time_to_live: Duration,
	pub hour_time_to_live: Duration,
	pub month_time_to_live: Duration,
	pub now: jiff::Timestamp,
	pub week_time_to_live: Duration,
}

#[derive(Clone, Debug, Default)]
pub struct Output {
	pub deleted: usize,
	pub done: bool,
}
