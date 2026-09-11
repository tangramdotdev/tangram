use std::collections::BTreeMap;

/// Track unfinished requests separately from their asynchronous operations.
pub(crate) struct Requests<K> {
	counts: BTreeMap<K, usize>,
	ids: BTreeMap<String, (K, usize)>,
}

impl<K: Copy + Ord> Requests<K> {
	#[must_use]
	pub fn contains(&self, id: &str) -> bool {
		self.ids.contains_key(id)
	}

	#[must_use]
	pub fn len(&self, kind: K) -> usize {
		self.counts.get(&kind).copied().unwrap_or_default()
	}

	pub fn try_insert(&mut self, id: String, kind: K, limit: usize) -> bool {
		self.try_reserve(id, kind, 1, limit)
	}

	pub fn try_reserve(&mut self, id: String, kind: K, weight: usize, limit: usize) -> bool {
		if self.ids.contains_key(&id) {
			return false;
		}
		let count = self.counts.entry(kind).or_default();
		if weight == 0 || weight > limit.saturating_sub(*count) {
			return false;
		}
		*count += weight;
		self.ids.insert(id, (kind, weight));
		true
	}

	pub fn remove(&mut self, id: &str) {
		if let Some((kind, weight)) = self.ids.remove(id) {
			*self.counts.get_mut(&kind).unwrap() -= weight;
		}
	}
}

impl<K> Default for Requests<K> {
	fn default() -> Self {
		Self {
			counts: BTreeMap::new(),
			ids: BTreeMap::new(),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::Requests;

	#[test]
	fn reserves_whole_groups_without_overflow() {
		let mut requests = Requests::default();
		assert!(requests.try_reserve("batch".into(), 0, 3, 4));
		assert!(!requests.try_reserve("other batch".into(), 0, 2, 4));
		assert!(requests.try_insert("single".into(), 0, 4));
		assert!(!requests.try_reserve("overflow".into(), 0, usize::MAX, usize::MAX));
		requests.remove("batch");
		assert!(requests.try_reserve("other batch".into(), 0, 3, 4));
	}

	#[test]
	fn bounds_each_kind_until_completion() {
		let mut requests = Requests::default();
		assert!(requests.try_insert("archive".into(), 0, 1));
		assert!(!requests.try_insert("archive".into(), 0, 1));
		assert!(!requests.try_insert("another archive".into(), 0, 1));
		assert!(requests.try_insert("wait".into(), 1, 1));
		requests.remove("archive");
		requests.remove("archive");
		assert!(requests.try_insert("another archive".into(), 0, 1));
		assert!(!requests.try_insert("another wait".into(), 1, 1));
		assert!(!requests.try_insert("disabled".into(), 2, 0));
	}
}
