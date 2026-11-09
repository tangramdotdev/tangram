use {
	crate::{Db, Index, Key as IndexKey, Kind, Request, Response},
	foundationdb_tuple as fdbt, heed as lmdb,
	num_traits::ToPrimitive as _,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub enum Key {
	Scheduler(tg::scheduler::Id),
}

impl Index {
	pub async fn get_schedulers(&self) -> tg::Result<Vec<tangram_index::scheduler::Scheduler>> {
		let response = self
			.send_read_request(tangram_index::read::Request::GetSchedulers)
			.await?;
		let tangram_index::read::Response::GetSchedulers(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_scheduler(&self, arg: tangram_index::scheduler::put::Arg) -> tg::Result<()> {
		let request = Request::PutScheduler(arg);
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub async fn try_get_scheduler(
		&self,
		arg: tangram_index::scheduler::get::Arg,
	) -> tg::Result<Option<tangram_index::scheduler::Scheduler>> {
		let request = tangram_index::read::Request::TryGetScheduler(arg);
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetScheduler(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) fn get_schedulers_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
	) -> tg::Result<Vec<tangram_index::scheduler::Scheduler>> {
		let prefix = Self::pack(subspace, &(Kind::Scheduler.to_i32().unwrap(),));
		let entries = db
			.prefix_iter(transaction, &prefix)
			.map_err(|error| tg::error!(!error, "failed to iterate the schedulers"))?;
		entries
			.map(|entry| {
				let (key, value) = entry
					.map_err(|error| tg::error!(!error, "failed to get an scheduler entry"))?;
				let key = Self::unpack(subspace, key)?;
				let IndexKey::Scheduler(Key::Scheduler(id)) = key else {
					return Err(tg::error!("unexpected key type"));
				};
				tangram_index::scheduler::Scheduler::deserialize(id, value)
			})
			.collect()
	}

	pub(crate) fn put_scheduler_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &mut lmdb::RwTxn<'_>,
		arg: &tangram_index::scheduler::put::Arg,
	) -> tg::Result<()> {
		let key = IndexKey::Scheduler(Key::Scheduler(arg.scheduler.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = arg.scheduler.serialize()?;
		db.put(transaction, &key, &value).map_err(
			|error| tg::error!(!error, id = %arg.scheduler.id, "failed to put the scheduler"),
		)?;

		Ok(())
	}

	pub(crate) fn try_get_scheduler_with_transaction(
		db: &Db,
		subspace: &fdbt::Subspace,
		transaction: &lmdb::RoTxn<'_>,
		arg: &tangram_index::scheduler::get::Arg,
	) -> tg::Result<Option<tangram_index::scheduler::Scheduler>> {
		let key = IndexKey::Scheduler(Key::Scheduler(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = db
			.get(transaction, &key)
			.map_err(|error| tg::error!(!error, id = %arg.id, "failed to get the scheduler"))?;
		value
			.map(|value| tangram_index::scheduler::Scheduler::deserialize(arg.id.clone(), value))
			.transpose()
	}
}

impl tangram_index::scheduler::Index for Index {
	async fn get_schedulers(&self) -> tg::Result<Vec<tangram_index::scheduler::Scheduler>> {
		self.get_schedulers().await
	}

	async fn put_scheduler(&self, arg: tangram_index::scheduler::put::Arg) -> tg::Result<()> {
		self.put_scheduler(arg).await
	}

	async fn try_get_scheduler(
		&self,
		arg: tangram_index::scheduler::get::Arg,
	) -> tg::Result<Option<tangram_index::scheduler::Scheduler>> {
		self.try_get_scheduler(arg).await
	}
}
