use std::collections::{BTreeMap, BTreeSet};

pub struct WeightedLruCache<K, V> {
	capacity: usize,
	entries: BTreeMap<K, Entry<V>>,
	generation: u64,
	recency: BTreeSet<(u64, K)>,
	weight: usize,
}

struct Entry<V> {
	generation: u64,
	value: V,
	weight: usize,
}

impl<K, V> WeightedLruCache<K, V>
where
	K: Clone + Ord,
	V: Clone,
{
	#[must_use]
	pub fn new(capacity: usize) -> Self {
		Self {
			capacity,
			entries: BTreeMap::new(),
			generation: 0,
			recency: BTreeSet::new(),
			weight: 0,
		}
	}

	pub fn get(&mut self, key: &K) -> Option<V> {
		let old_generation = self.entries.get(key)?.generation;
		self.generation = self.generation.wrapping_add(1);
		self.recency.remove(&(old_generation, key.clone()));
		let entry = self.entries.get_mut(key).unwrap();
		entry.generation = self.generation;
		self.recency.insert((self.generation, key.clone()));

		Some(entry.value.clone())
	}

	pub fn insert(&mut self, key: K, value: V, weight: usize) -> V {
		if let Some(value) = self.get(&key) {
			return value;
		}
		if weight > self.capacity {
			return value;
		}

		self.generation = self.generation.wrapping_add(1);
		let entry = Entry {
			generation: self.generation,
			value: value.clone(),
			weight,
		};
		self.entries.insert(key.clone(), entry);
		self.recency.insert((self.generation, key));
		self.weight = self.weight.saturating_add(weight);
		while self.weight > self.capacity {
			let (generation, key) = self.recency.pop_first().unwrap();
			let entry = self.entries.remove(&key).unwrap();
			debug_assert_eq!(entry.generation, generation);
			self.weight = self.weight.saturating_sub(entry.weight);
		}

		value
	}

	pub fn remove(&mut self, key: &K) {
		let Some(entry) = self.entries.remove(key) else {
			return;
		};
		self.recency.remove(&(entry.generation, key.clone()));
		self.weight = self.weight.saturating_sub(entry.weight);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn is_weighted_and_lru() {
		let mut cache = WeightedLruCache::new(8);
		cache.insert(1, "one", 4);
		cache.insert(2, "two", 4);
		assert_eq!(cache.get(&1), Some("one"));
		cache.insert(3, "three", 4);

		assert!(cache.entries.contains_key(&1));
		assert!(!cache.entries.contains_key(&2));
		assert!(cache.entries.contains_key(&3));
		assert!(cache.weight <= cache.capacity);
	}

	#[test]
	fn oversized_values_bypass_admission() {
		let mut cache = WeightedLruCache::new(4);
		assert_eq!(cache.insert(1, "value", 5), "value");
		assert!(!cache.entries.contains_key(&1));
		assert_eq!(cache.weight, 0);
	}
}
