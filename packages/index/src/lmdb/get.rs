use {
	super::{Index, Key},
	crate::{Object, Process},
	foundationdb_tuple::TuplePack as _,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_objects(&self, ids: &[tg::object::Id]) -> tg::Result<Vec<Option<Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let ids = ids.to_owned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in &ids {
					let key = Key::Object(id.clone()).pack_to_vec();
					let value = db
						.get(&transaction, &key)
						.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
					let value = value.map(Object::deserialize).transpose()?;
					outputs.push(value);
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}

	pub async fn try_get_processes(
		&self,
		ids: &[tg::process::Id],
	) -> tg::Result<Vec<Option<Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		tokio::task::spawn_blocking({
			let db = self.db;
			let env = self.env.clone();
			let ids = ids.to_owned();
			move || {
				let transaction = env
					.read_txn()
					.map_err(|source| tg::error!(!source, "failed to begin a transaction"))?;
				let mut outputs = Vec::with_capacity(ids.len());
				for id in &ids {
					let key = Key::Process(id.clone()).pack_to_vec();
					let value = db
						.get(&transaction, &key)
						.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?;
					let value = value.map(Process::deserialize).transpose()?;
					outputs.push(value);
				}
				Ok(outputs)
			}
		})
		.await
		.map_err(|source| tg::error!(!source, "failed to join the task"))?
	}
}
