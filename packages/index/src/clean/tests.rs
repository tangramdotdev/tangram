use super::*;

#[test]
fn there_is_no_cutoff_when_no_process_is_unfinished() {
	let cutoff = unfinished_cutoff(None, std::time::Duration::from_secs(10));
	assert_eq!(cutoff, None);
}

#[test]
fn the_cutoff_is_the_creation_time_less_the_time_to_touch_and_one_second() {
	let cutoff = unfinished_cutoff(Some(500), std::time::Duration::from_secs(3600));
	assert_eq!(cutoff, Some(500 - 3600 - 1));
}

#[test]
fn the_cutoff_keeps_the_second_of_the_creation_with_no_time_to_touch() {
	let cutoff = unfinished_cutoff(Some(95), std::time::Duration::ZERO);
	assert_eq!(cutoff, Some(94));
}
