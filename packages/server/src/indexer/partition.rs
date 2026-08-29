use std::ops::Range;

pub(super) fn ranges(
	partition_start: u64,
	partition_end: u64,
	concurrency: usize,
) -> impl Iterator<Item = Range<u64>> {
	let concurrency = u64::try_from(concurrency).unwrap();
	let partition_length = partition_end - partition_start;
	(0..concurrency).filter_map(move |task_index| {
		let partitions_per_task = partition_length / concurrency;
		let extra = partition_length % concurrency;
		let start = partition_start + task_index * partitions_per_task + task_index.min(extra);
		let count = partitions_per_task + u64::from(task_index < extra);
		(count > 0).then_some(start..start + count)
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn distributes_partitions() {
		let ranges = ranges(0, 10, 3).collect::<Vec<_>>();

		assert_eq!(ranges, [0..4, 4..7, 7..10]);
	}

	#[test]
	fn omits_empty_ranges() {
		let ranges = ranges(5, 7, 4).collect::<Vec<_>>();

		assert_eq!(ranges, [5..6, 6..7]);
	}
}
