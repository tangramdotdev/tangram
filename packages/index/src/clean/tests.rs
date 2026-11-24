use super::*;

#[test]
fn the_cutoffs_are_the_callers_when_no_process_is_unfinished() {
	let arg = arg(100, 90, 80, 10);
	let cutoffs = cutoffs(&arg, None);
	let expected = Cutoffs {
		object: 100,
		process: 90,
		sandbox: 80,
		unfinished: None,
	};
	assert_eq!(cutoffs, expected);
}

#[test]
fn the_oldest_unfinished_process_lowers_every_later_cutoff() {
	let arg = arg(100, 90, 80, 10);
	let cutoffs = cutoffs(&arg, Some(95));
	let expected = Cutoffs {
		object: 84,
		process: 84,
		sandbox: 80,
		unfinished: Some(84),
	};
	assert_eq!(cutoffs, expected);
}

#[test]
fn an_unfinished_process_created_after_every_cutoff_does_not_limit_the_clean() {
	let arg = arg(100, 90, 80, 10);
	let cutoffs = cutoffs(&arg, Some(200));
	let expected = Cutoffs {
		object: 100,
		process: 90,
		sandbox: 80,
		unfinished: None,
	};
	assert_eq!(cutoffs, expected);
}

#[test]
fn a_clean_with_a_zero_ttl_keeps_what_was_touched_within_the_time_to_touch_before_the_process() {
	let arg = arg(1000, 1000, 1000, 3600);
	let cutoffs = cutoffs(&arg, Some(500));
	assert_eq!(cutoffs.object, 500 - 3600 - 1);
	assert_eq!(cutoffs.process, 500 - 3600 - 1);
	assert_eq!(cutoffs.sandbox, 500 - 3600 - 1);
	assert_eq!(cutoffs.unfinished, Some(500 - 3600 - 1));
}

fn arg(object: i64, process: i64, sandbox: i64, time_to_touch: u64) -> Arg {
	Arg {
		batch_size: 1,
		max_object_touched_at: object,
		max_process_touched_at: process,
		max_sandbox_touched_at: sandbox,
		now: object,
		partition_end: 1,
		partition_start: 0,
		time_to_touch: std::time::Duration::from_secs(time_to_touch),
	}
}

fn cutoffs(arg: &Arg, oldest_unfinished_created_at: Option<i64>) -> Cutoffs {
	Cutoffs::new(
		arg.max_object_touched_at,
		arg.max_process_touched_at,
		arg.max_sandbox_touched_at,
		arg.time_to_touch,
		oldest_unfinished_created_at,
	)
}
