use {
	std::{collections::BTreeMap, sync::Mutex},
	tangram_client::prelude::*,
};

pub struct State {
	checkpoints: Mutex<BTreeMap<String, Checkpoint>>,
}

#[derive(Default)]
struct Checkpoint {
	aborts: Vec<tg::checkpoint::Params>,
	next_watch: u64,
	panics: Vec<tg::checkpoint::Params>,
	watches: BTreeMap<u64, Watch>,
}

struct Watch {
	hits: BTreeMap<u64, Hit>,
	next_hit: u64,
	params: tg::checkpoint::Params,
	sender: tokio::sync::watch::Sender<u64>,
}

struct Hit {
	params: tg::checkpoint::Params,
	sender: Option<tokio::sync::oneshot::Sender<()>>,
}

impl State {
	#[must_use]
	pub fn new() -> Self {
		Self {
			checkpoints: Mutex::new(BTreeMap::new()),
		}
	}

	pub fn watch(&self, checkpoint: &str, params: tg::checkpoint::Params) -> u64 {
		let mut checkpoints = self.checkpoints.lock().unwrap();
		let checkpoint = checkpoints.entry(checkpoint.to_owned()).or_default();
		let watch = checkpoint.next_watch;
		checkpoint.next_watch += 1;
		let (sender, _) = tokio::sync::watch::channel(0);
		let entry = Watch {
			hits: BTreeMap::new(),
			next_hit: 0,
			params,
			sender,
		};
		checkpoint.watches.insert(watch, entry);
		watch
	}

	pub async fn wait(
		&self,
		checkpoint: &str,
		watch: u64,
		hit: u64,
	) -> Option<tg::checkpoint::Params> {
		loop {
			let mut receiver = {
				let checkpoints = self.checkpoints.lock().unwrap();
				let checkpoint = checkpoints.get(checkpoint)?;
				let watch = checkpoint.watches.get(&watch)?;
				if let Some(hit) = watch.hits.get(&hit) {
					return Some(hit.params.clone());
				}
				watch.sender.subscribe()
			};
			receiver.changed().await.ok()?;
		}
	}

	pub fn continue_hit(&self, checkpoint: &str, watch: u64, hit: u64) -> bool {
		let mut checkpoints = self.checkpoints.lock().unwrap();
		let Some(hit) = checkpoints
			.get_mut(checkpoint)
			.and_then(|checkpoint| checkpoint.watches.get_mut(&watch))
			.and_then(|watch| watch.hits.get_mut(&hit))
		else {
			return false;
		};
		if let Some(sender) = hit.sender.take() {
			sender.send(()).ok();
		}
		true
	}

	pub fn unwatch(&self, checkpoint: &str, watch: u64) -> bool {
		let mut checkpoints = self.checkpoints.lock().unwrap();
		checkpoints
			.get_mut(checkpoint)
			.and_then(|checkpoint| checkpoint.watches.remove(&watch))
			.is_some()
	}

	pub fn abort(&self, checkpoint: &str, params: tg::checkpoint::Params) {
		let mut checkpoints = self.checkpoints.lock().unwrap();
		let checkpoint = checkpoints.entry(checkpoint.to_owned()).or_default();
		checkpoint.aborts.push(params);
	}

	pub fn panic(&self, checkpoint: &str, params: tg::checkpoint::Params) {
		let mut checkpoints = self.checkpoints.lock().unwrap();
		let checkpoint = checkpoints.entry(checkpoint.to_owned()).or_default();
		checkpoint.panics.push(params);
	}

	pub async fn hit(&self, checkpoint: &str, params: tg::checkpoint::Params) {
		let panic = {
			let checkpoints = self.checkpoints.lock().unwrap();
			let Some(checkpoint) = checkpoints.get(checkpoint) else {
				return;
			};
			if checkpoint
				.aborts
				.iter()
				.any(|expected| params_match(expected, &params))
			{
				std::process::abort();
			}
			checkpoint
				.panics
				.iter()
				.any(|expected| params_match(expected, &params))
		};
		assert!(!panic, "checkpoint panic");
		let receivers = {
			let mut checkpoints = self.checkpoints.lock().unwrap();
			let Some(checkpoint) = checkpoints.get_mut(checkpoint) else {
				return;
			};
			let mut receivers = Vec::new();
			for watch in checkpoint.watches.values_mut() {
				if !params_match(&watch.params, &params) {
					continue;
				}
				let hit = watch.next_hit;
				watch.next_hit += 1;
				let (sender, receiver) = tokio::sync::oneshot::channel();
				let entry = Hit {
					params: params.clone(),
					sender: Some(sender),
				};
				watch.hits.insert(hit, entry);
				watch.sender.send_replace(watch.next_hit);
				receivers.push(receiver);
			}
			receivers
		};
		for receiver in receivers {
			receiver.await.ok();
		}
	}
}

impl Default for State {
	fn default() -> Self {
		Self::new()
	}
}

fn params_match(expected: &tg::checkpoint::Params, actual: &tg::checkpoint::Params) -> bool {
	expected
		.iter()
		.all(|(key, expected)| actual.get(key) == Some(expected))
}
