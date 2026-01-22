use {
	super::{Db, Index, Key, Request, Response},
	crate::{Object, Process},
	foundationdb_tuple::TuplePack as _,
	heed as lmdb,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchObjects {
			ids: ids.to_vec(),
			touched_at,
		};
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		match response {
			Response::Objects(objects) => Ok(objects),
			_ => Err(tg::error!("unexpected response")),
		}
	}

	pub async fn touch_processes(
		&self,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Process>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let (sender, receiver) = tokio::sync::oneshot::channel();
		let request = Request::TouchProcesses {
			ids: ids.to_vec(),
			touched_at,
		};
		self.sender
			.send((request, sender))
			.await
			.map_err(|source| tg::error!(!source, "failed to send the request"))?;
		let response = receiver
			.await
			.map_err(|_| tg::error!("the task panicked"))??;
		match response {
			Response::Processes(processes) => Ok(processes),
			_ => Err(tg::error!("unexpected response")),
		}
	}

	pub(super) fn task_touch_objects(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::object::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Object>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			let key = Key::Object(id).pack_to_vec();
			let existing = db
				.get(transaction, &key)
				.map_err(|source| tg::error!(!source, %id, "failed to get the object"))?;
			let existing = existing
				.map(tangram_serialize::from_slice::<Object>)
				.transpose()
				.map_err(|source| tg::error!(!source, %id, "failed to deserialize the object"))?;

			let Some(mut object) = existing else {
				outputs.push(None);
				continue;
			};

			// Update touched_at using max.
			object.touched_at = object.touched_at.max(touched_at);

			let value_bytes = tangram_serialize::to_vec(&object)
				.map_err(|source| tg::error!(!source, "failed to serialize the object"))?;
			db.put(transaction, &key, &value_bytes)
				.map_err(|source| tg::error!(!source, %id, "failed to put the object"))?;

			outputs.push(Some(object));
		}
		Ok(outputs)
	}

	pub(super) fn task_touch_processes(
		db: &Db,
		transaction: &mut lmdb::RwTxn<'_>,
		ids: &[tg::process::Id],
		touched_at: i64,
	) -> tg::Result<Vec<Option<Process>>> {
		let mut outputs = Vec::with_capacity(ids.len());
		for id in ids {
			let key = Key::Process(id).pack_to_vec();
			let existing = db
				.get(transaction, &key)
				.map_err(|source| tg::error!(!source, %id, "failed to get the process"))?;
			let existing = existing
				.map(tangram_serialize::from_slice::<Process>)
				.transpose()
				.map_err(|source| tg::error!(!source, %id, "failed to deserialize the process"))?;

			let Some(mut process) = existing else {
				outputs.push(None);
				continue;
			};

			// Update touched_at using max.
			process.touched_at = process.touched_at.max(touched_at);

			let value_bytes = tangram_serialize::to_vec(&process)
				.map_err(|source| tg::error!(!source, "failed to serialize the process"))?;
			db.put(transaction, &key, &value_bytes)
				.map_err(|source| tg::error!(!source, %id, "failed to put the process"))?;

			outputs.push(Some(process));
		}
		Ok(outputs)
	}
}
