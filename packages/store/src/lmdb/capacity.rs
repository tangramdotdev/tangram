use {super::Store, crate::capacity::Capacity, tangram_client::prelude::*};

impl Store {
	pub fn try_get_capacity(&self) -> tg::Result<Capacity> {
		let available = 1;
		let total = 1;
		let capacity = Capacity { available, total };

		Ok(capacity)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn reports_full_capacity() {
		let temp = tangram_util::fs::Temp::new().unwrap();
		std::fs::create_dir(temp.path()).unwrap();
		let config = super::super::Config {
			map_size: 1024 * 1024 * 10,
			path: temp.path().join("test.lmdb"),
			posix_sem_prefix: None,
			read_batch_size: 64,
			read_concurrency: 1,
			write_batch_size: 8_000,
		};
		let store = Store::new(&config).unwrap();
		let capacity = store.try_get_capacity().unwrap();
		assert_eq!(capacity.available, 1);
		assert_eq!(capacity.total, 1);
	}
}
