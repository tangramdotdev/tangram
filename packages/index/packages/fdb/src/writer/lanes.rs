use {
	super::{Batch, RequestTracker, ResponseSender},
	crate::{Index, Request, request::Priority},
	std::{
		collections::VecDeque,
		sync::{Arc, Mutex},
	},
};

#[cfg(test)]
mod tests;

/// The lanes of a writer. Each lane runs its transactions one after another, and the lanes run concurrently. A write's items that have an affinity always go to the lane that the affinity's stable hash chooses, so that items with the same affinity run in one transaction or in transactions that do not overlap.
pub(super) struct Lanes {
	/// The number of items in a batch of a request other than a write.
	batch_items: usize,

	entries: Vec<Lane>,

	/// The number of operations that wait in the lanes.
	pending: usize,

	/// The number of items in a batch of writes.
	write_items: usize,
}

struct Lane {
	busy: bool,

	/// The number of operations that wait in this lane.
	pending: usize,

	/// The parts that wait in this lane, in high, medium, and low priority order.
	queues: [VecDeque<Part>; 3],
}

/// A part of a request that a lane runs, which answers the request through its tracker once every part of the request is done.
struct Part {
	request: Request,
	tracker: Arc<Mutex<RequestTracker>>,
}

impl Lanes {
	pub(super) fn new(count: usize, batch_items: usize, write_items: usize) -> Self {
		let entries = (0..count.max(1))
			.map(|_| Lane {
				busy: false,
				pending: 0,
				queues: [VecDeque::new(), VecDeque::new(), VecDeque::new()],
			})
			.collect();
		Self {
			batch_items: batch_items.max(1),
			entries,
			pending: 0,
			write_items: write_items.max(1),
		}
	}

	/// Get the number of operations that wait in the lanes.
	pub(super) fn pending(&self) -> usize {
		self.pending
	}

	/// Add a request to the lanes. A write's items that have an affinity go to their affinity's lane along with the write's items that have none, and the items of a write without an affinity are spread across the least loaded lanes in chunks of a batch. Any other request goes whole to the least loaded lane.
	pub(super) fn dispatch(&mut self, request: Request, sender: ResponseSender) {
		let priority = request.priority();
		let tracker = Arc::new(Mutex::new(RequestTracker {
			remaining: 0,
			response: Ok(Index::create_initial_response(&request)),
			sender: Some(sender),
		}));
		let parts = match request {
			Request::Write(arg) => self.write_parts(arg),
			request => vec![(self.least_loaded(), request)],
		};
		tracker.lock().unwrap().remaining = parts.len();
		for (lane, request) in parts {
			let count = operation_count(&request);
			let part = Part {
				request,
				tracker: tracker.clone(),
			};
			let lane = &mut self.entries[lane];
			lane.queues[priority_index(priority)].push_back(part);
			lane.pending += count;
			self.pending += count;
		}
	}

	/// Take a batch for each idle lane that has parts waiting, and mark the lane busy until its batch completes.
	pub(super) fn next(&mut self) -> Vec<(usize, Batch)> {
		let mut batches = Vec::new();
		for index in 0..self.entries.len() {
			if self.entries[index].busy {
				continue;
			}
			let Some((batch, count)) = self.take_batch(index) else {
				continue;
			};
			let lane = &mut self.entries[index];
			lane.busy = true;
			lane.pending -= count;
			self.pending -= count;
			batches.push((index, batch));
		}
		batches
	}

	/// Mark a lane idle once its batch completes.
	pub(super) fn complete(&mut self, lane: usize) {
		self.entries[lane].busy = false;
	}

	/// Get the lane of an affinity.
	pub(super) fn lane(&self, affinity: &tangram_index::Affinity) -> usize {
		let count = u64::try_from(self.entries.len()).unwrap();
		usize::try_from(affinity.stable_hash() % count).unwrap()
	}

	fn write_parts(&mut self, arg: tangram_index::write::Arg) -> Vec<(usize, Request)> {
		// Send a write that has an affinity to its affinities' lanes, and its items without an affinity with its first affinity's items, so that a write of one affinity stays whole.
		let first = arg.items.iter().find_map(tangram_index::Item::affinity);
		if let Some(first) = first {
			let first = self.lane(&first);
			let mut parts = Vec::<(usize, Vec<tangram_index::Item>)>::new();
			for item in arg.items {
				let lane = item
					.affinity()
					.map_or(first, |affinity| self.lane(&affinity));
				match parts.iter_mut().find(|(index, _)| *index == lane) {
					Some((_, items)) => items.push(item),
					None => parts.push((lane, vec![item])),
				}
			}
			return parts
				.into_iter()
				.map(|(lane, items)| (lane, Request::Write(tangram_index::write::Arg { items })))
				.collect();
		}

		// Spread a write without an affinity across the least loaded lanes in chunks of a batch.
		let mut parts = Vec::new();
		let mut items = arg.items.into_iter().peekable();
		while items.peek().is_some() {
			let chunk = items.by_ref().take(self.write_items).collect::<Vec<_>>();
			let lane = self.least_loaded_with(&parts);
			parts.push((lane, chunk));
		}
		if parts.is_empty() {
			parts.push((self.least_loaded(), Vec::new()));
		}
		parts
			.into_iter()
			.map(|(lane, items)| (lane, Request::Write(tangram_index::write::Arg { items })))
			.collect()
	}

	fn least_loaded(&self) -> usize {
		self.least_loaded_with(&[])
	}

	/// Get the lane with the fewest operations waiting, counting the chunks that a dispatch is about to add, and the lowest index among equals.
	fn least_loaded_with(&self, parts: &[(usize, Vec<tangram_index::Item>)]) -> usize {
		(0..self.entries.len())
			.min_by_key(|index| {
				let adding = parts
					.iter()
					.filter(|(lane, _)| lane == index)
					.map(|(_, items)| items.len())
					.sum::<usize>();
				(
					self.entries[*index].pending + adding,
					usize::from(self.entries[*index].busy),
				)
			})
			.unwrap()
	}

	/// Take a lane's next batch from its highest priority parts, and return it with the number of operations that it takes from the lane.
	fn take_batch(&mut self, index: usize) -> Option<(Batch, usize)> {
		let write_items = self.write_items;
		let batch_items = self.batch_items;
		let lane = &mut self.entries[index];
		let queue = lane.queues.iter_mut().find(|queue| !queue.is_empty())?;
		let mut batch = Batch {
			requests: Vec::new(),
			trackers: Vec::new(),
		};
		let mut count = 0;

		// Run an update or a clean in a transaction of its own, because they read the update queues, and reading a queue range fails after another request in the transaction enqueues an update.
		if matches!(
			queue.front().unwrap().request,
			Request::Clean(_) | Request::Update(_)
		) {
			let part = queue.pop_front().unwrap();
			count += operation_count(&part.request);
			batch.requests.push(part.request);
			batch.trackers.push(part.tracker);
			return Some((batch, count));
		}

		// Add whole parts while they fit, and split a part that does not fit in an empty batch.
		while let Some(part) = queue.front() {
			if matches!(part.request, Request::Clean(_) | Request::Update(_)) {
				break;
			}
			let limit = if matches!(part.request, Request::Write(_)) {
				write_items
			} else {
				batch_items
			};
			let size = operation_count(&part.request);
			let space = limit.saturating_sub(count);
			if size <= space {
				let part = queue.pop_front().unwrap();
				count += size;
				batch.requests.push(part.request);
				batch.trackers.push(part.tracker);
				continue;
			}
			if !batch.requests.is_empty() {
				break;
			}
			let part = queue.pop_front().unwrap();
			let (mut operations, kind) = Index::request_into_operations(part.request);
			let rest = operations.split_off(space.max(1));
			count += operations.len();
			batch
				.requests
				.push(Index::request_from_operations(operations, &kind));
			batch.trackers.push(part.tracker.clone());
			part.tracker.lock().unwrap().remaining += 1;
			let rest = Part {
				request: Index::request_from_operations(rest, &kind),
				tracker: part.tracker,
			};
			queue.push_front(rest);
			break;
		}

		Some((batch, count))
	}
}

/// Get the number of operations of a request, which is the number of items of a write or a clean, and one for any other request.
fn operation_count(request: &Request) -> usize {
	match request {
		Request::Clean(clean) => clean.batch_size,
		Request::Write(arg) => arg.items.len(),
		_ => 1,
	}
}

fn priority_index(priority: Priority) -> usize {
	match priority {
		Priority::High => 0,
		Priority::Low => 2,
		Priority::Medium => 1,
	}
}
