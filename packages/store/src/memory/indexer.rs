use {super::Store, crate::indexer, tangram_client::prelude::*};

impl Store {
	pub fn delete_indexer(&self, arg: &indexer::delete::Arg) {
		self.state().indexers.remove(&arg.id);
	}

	#[must_use]
	pub fn get_indexers(&self) -> Vec<indexer::Indexer> {
		self.state().indexers.values().cloned().collect()
	}

	pub fn put_indexer(&self, arg: indexer::put::Arg) {
		let indexer = arg.indexer;
		self.state().indexers.insert(indexer.id.clone(), indexer);
	}

	#[must_use]
	pub fn try_get_indexer(&self, arg: &indexer::get::Arg) -> Option<indexer::Indexer> {
		self.state().indexers.get(&arg.id).cloned()
	}

	pub fn update_indexer(&self, arg: &indexer::update::Arg) -> tg::Result<()> {
		let mut state = self.state();
		let indexer = state
			.indexers
			.get_mut(&arg.id)
			.ok_or_else(|| tg::error!(id = %arg.id, "the indexer does not exist"))?;
		match &arg.value {
			indexer::update::Value::ArchiveReadSequence(value) => {
				indexer.archive_read_sequence = *value;
			},
			indexer::update::Value::ArchiveWriteSequence(value) => {
				indexer.archive_write_sequence = *value;
			},
			indexer::update::Value::Available(value) => indexer.available = *value,
			indexer::update::Value::IndexReadSequence(value) => {
				indexer.index_read_sequence = *value;
			},
			indexer::update::Value::IndexWriteSequence(value) => {
				indexer.index_write_sequence = *value;
			},
		}

		Ok(())
	}
}

#[cfg(test)]
mod tests {
	use {super::Store, crate::indexer};

	#[test]
	fn lifecycle() {
		let store = Store::new();
		let id = tangram_client::indexer::Id::new();
		let indexer = indexer::Indexer::new(id.clone());
		store.put_indexer(indexer::put::Arg { indexer });

		let values = [
			indexer::update::Value::ArchiveReadSequence(1),
			indexer::update::Value::ArchiveWriteSequence(2),
			indexer::update::Value::Available(true),
			indexer::update::Value::IndexReadSequence(3),
			indexer::update::Value::IndexWriteSequence(4),
		];
		for value in values {
			let arg = indexer::update::Arg {
				id: id.clone(),
				value,
			};
			store.update_indexer(&arg).unwrap();
		}

		let arg = indexer::get::Arg { id: id.clone() };
		let indexer = store.try_get_indexer(&arg).unwrap();
		assert_eq!(indexer.archive_read_sequence, 1);
		assert_eq!(indexer.archive_write_sequence, 2);
		assert!(indexer.available);
		assert_eq!(indexer.index_read_sequence, 3);
		assert_eq!(indexer.index_write_sequence, 4);
		assert_eq!(store.get_indexers(), vec![indexer]);

		let arg = indexer::delete::Arg { id: id.clone() };
		store.delete_indexer(&arg);
		let arg = indexer::get::Arg { id };
		assert!(store.try_get_indexer(&arg).is_none());
	}
}
