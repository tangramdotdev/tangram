use {
	super::{Request, RequestArg},
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
};

type BatchId = crate::store::index::queue::batch::Id;

#[derive(Default)]
pub(in crate::indexer) struct Limits {
	active: crate::control::requests::Requests<Kind>,
	batches: BTreeMap<BatchId, Batch>,
	fragments: BTreeMap<String, BatchId>,
}

struct Batch {
	fragments: BTreeSet<u64>,
	len: u64,
	remaining: usize,
}

#[derive(Clone, Copy, Eq, Ord, PartialEq, PartialOrd)]
enum Kind {
	Archive,
	Index,
	Wait,
}

impl Limits {
	#[must_use]
	pub(in crate::indexer) fn has_queue_requests(&self) -> bool {
		self.active.len(Kind::Archive) != 0 || self.active.len(Kind::Index) != 0
	}

	pub(super) fn try_insert(
		&mut self,
		request: &Request,
		config: &crate::config::IndexerRequest,
	) -> tg::Result<bool> {
		let inserted = match &request.arg {
			RequestArg::Archive(_) => self.active.try_insert(
				request.id.clone(),
				Kind::Archive,
				config.archive_concurrency,
			),
			RequestArg::Index(arg) => {
				if arg.fragments == 0 || arg.fragment >= arg.fragments {
					return Err(tg::error!("invalid index fragment count or position"));
				}
				let count = usize::try_from(arg.fragments)
					.ok()
					.filter(|count| *count <= config.index_concurrency)
					.ok_or_else(|| {
						tg::error!("the batch exceeds the indexer request index concurrency")
					})?;
				if !self.batches.contains_key(&arg.batch) {
					// Reserve every fragment before accepting a partial batch.
					if !self.active.try_reserve(
						format!("batch:{}", tg::id::ENCODING.encode(&arg.batch.value())),
						Kind::Index,
						count,
						config.index_concurrency,
					) {
						return Ok(false);
					}
					let batch = Batch {
						fragments: BTreeSet::new(),
						len: arg.fragments,
						remaining: 0,
					};
					self.batches.insert(arg.batch, batch);
				}
				let batch = self.batches.get_mut(&arg.batch).unwrap();
				if batch.len != arg.fragments {
					return Err(tg::error!("inconsistent index fragment counts"));
				}
				if !batch.fragments.insert(arg.fragment) {
					return Ok(false);
				}
				batch.remaining += 1;
				self.fragments.insert(request.id.clone(), arg.batch);
				true
			},
			RequestArg::Wait => {
				self.active
					.try_insert(request.id.clone(), Kind::Wait, config.wait_concurrency)
			},
		};
		if !inserted {
			return Ok(false);
		}
		Ok(true)
	}

	pub(in crate::indexer) fn remove(&mut self, id: &str) {
		let Some(batch) = self.fragments.remove(id) else {
			self.active.remove(id);
			return;
		};
		let state = self.batches.get_mut(&batch).unwrap();
		state.remaining -= 1;
		if state.remaining == 0 {
			self.batches.remove(&batch);
			self.active.remove(&format!(
				"batch:{}",
				tg::id::ENCODING.encode(&batch.value())
			));
		}
	}
}

#[cfg(test)]
mod tests {
	use {
		super::Limits,
		crate::indexer::request::{IndexRequestArg, Request, RequestArg},
	};

	fn fragment(id: &str, batch: u8, fragment: u64, fragments: u64) -> Request {
		let arg = IndexRequestArg {
			batch: crate::store::index::queue::batch::Id::new([batch; 16]),
			fragment,
			fragments,
			payload: bytes::Bytes::new(),
		};
		Request {
			arg: RequestArg::Index(arg),
			id: id.to_owned(),
		}
	}

	#[test]
	fn reserves_missing_fragments_and_releases_complete_batches() {
		let mut pending = Limits::default();
		let config = crate::config::IndexerRequest {
			index_concurrency: 3,
			..Default::default()
		};
		let last = fragment("last", 0, 2, 3);
		let other = fragment("other", 1, 0, 1);
		assert!(pending.try_insert(&last, &config).unwrap());
		assert!(!pending.try_insert(&other, &config).unwrap());
		let first = fragment("first", 0, 0, 3);
		let middle = fragment("middle", 0, 1, 3);
		assert!(pending.try_insert(&first, &config).unwrap());
		assert!(pending.try_insert(&middle, &config).unwrap());
		pending.remove("first");
		pending.remove("middle");
		assert!(!pending.try_insert(&other, &config).unwrap());
		pending.remove("last");
		assert!(pending.try_insert(&other, &config).unwrap());
	}

	#[test]
	fn releases_partial_batches_after_errors_and_rejects_duplicate_fragments() {
		let mut pending = Limits::default();
		let config = crate::config::IndexerRequest {
			index_concurrency: 2,
			..Default::default()
		};
		let first = fragment("first", 0, 0, 2);
		assert!(pending.try_insert(&first, &config).unwrap());
		let duplicate = fragment("duplicate", 0, 0, 2);
		assert!(!pending.try_insert(&duplicate, &config).unwrap());
		pending.remove("first");
		assert!(pending.try_insert(&duplicate, &config).unwrap());
		pending.remove("duplicate");
		let oversized = fragment("oversized", 1, 0, 3);
		assert!(pending.try_insert(&oversized, &config).is_err());
		assert!(pending.try_insert(&first, &config).unwrap());
	}

	#[test]
	fn keeps_wait_capacity_independent() {
		let mut pending = Limits::default();
		let config = crate::config::IndexerRequest {
			index_concurrency: 1,
			wait_concurrency: 1,
			..Default::default()
		};
		let index = fragment("index", 0, 0, 1);
		let wait = Request {
			arg: RequestArg::Wait,
			id: "wait".into(),
		};
		let other = Request {
			arg: RequestArg::Wait,
			id: "other".into(),
		};
		assert!(pending.try_insert(&index, &config).unwrap());
		assert!(pending.try_insert(&wait, &config).unwrap());
		assert!(!pending.try_insert(&other, &config).unwrap());
		pending.remove("wait");
		assert!(pending.try_insert(&other, &config).unwrap());
	}
}
