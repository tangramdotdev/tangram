#![allow(clippy::unnecessary_wraps)]

use {
	crate::{Index, Key as IndexKey, Kind, Request, Response},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	futures::{StreamExt as _, pin_mut},
	num_traits::ToPrimitive as _,
	std::ops::ControlFlow,
	tangram_client::prelude::*,
};

#[derive(Clone, Debug)]
pub enum Key {
	Scheduler(tg::scheduler::Id),
}

impl Index {
	pub async fn delete_scheduler(
		&self,
		arg: tangram_index::scheduler::delete::Arg,
	) -> tg::Result<()> {
		let request = Request::DeleteScheduler(arg);
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

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

	pub async fn update_scheduler(
		&self,
		arg: tangram_index::scheduler::update::Arg,
	) -> tg::Result<()> {
		let request = Request::UpdateScheduler(arg);
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub(crate) async fn delete_scheduler_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &tangram_index::scheduler::delete::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = IndexKey::Scheduler(Key::Scheduler(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		txn.clear(&key);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn get_schedulers_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
	) -> tg::Result<ControlFlow<Vec<tangram_index::scheduler::Scheduler>, fdb::FdbError>> {
		let prefix = Self::pack(subspace, &(Kind::Scheduler.to_i32().unwrap(),));
		let range = fdb::RangeOption {
			mode: fdb::options::StreamingMode::WantAll,
			..fdb::RangeOption::from(&Subspace::from_bytes(prefix))
		};
		let entries = txn.get_ranges_keyvalues(range, false);
		pin_mut!(entries);
		let mut schedulers = Vec::new();
		while let Some(result) = entries.next().await {
			let entry = crate::retry!(result);
			let key = Self::unpack(subspace, entry.key())?;
			let IndexKey::Scheduler(Key::Scheduler(id)) = key else {
				return Err(tg::error!("unexpected key type"));
			};
			let scheduler = tangram_index::scheduler::Scheduler::deserialize(id, entry.value())?;
			schedulers.push(scheduler);
		}

		Ok(ControlFlow::Break(schedulers))
	}

	pub(crate) async fn put_scheduler_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &tangram_index::scheduler::put::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = IndexKey::Scheduler(Key::Scheduler(arg.scheduler.id.clone()));
		let key = Self::pack(subspace, &key);
		let value = arg.scheduler.serialize()?;
		txn.set(&key, &value);

		Ok(ControlFlow::Break(()))
	}

	pub(crate) async fn try_get_scheduler_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &tangram_index::scheduler::get::Arg,
	) -> tg::Result<ControlFlow<Option<tangram_index::scheduler::Scheduler>, fdb::FdbError>> {
		let key = IndexKey::Scheduler(Key::Scheduler(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result);
		let scheduler = value
			.map(|value| tangram_index::scheduler::Scheduler::deserialize(arg.id.clone(), &value))
			.transpose()?;

		Ok(ControlFlow::Break(scheduler))
	}

	pub(crate) async fn update_scheduler_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		arg: &tangram_index::scheduler::update::Arg,
	) -> tg::Result<ControlFlow<(), fdb::FdbError>> {
		let key = IndexKey::Scheduler(Key::Scheduler(arg.id.clone()));
		let key = Self::pack(subspace, &key);
		let result = txn.get(&key, false).await;
		let value = crate::retry!(result)
			.ok_or_else(|| tg::error!(id = %arg.id, "the scheduler does not exist"))?;
		let mut scheduler =
			tangram_index::scheduler::Scheduler::deserialize(arg.id.clone(), &value)?;
		match &arg.value {
			tangram_index::scheduler::update::Value::Available(value) => {
				scheduler.available = *value;
			},
		}
		let value = scheduler.serialize()?;
		txn.set(&key, &value);

		Ok(ControlFlow::Break(()))
	}
}

impl tangram_index::scheduler::Index for Index {
	async fn delete_scheduler(&self, arg: tangram_index::scheduler::delete::Arg) -> tg::Result<()> {
		self.delete_scheduler(arg).await
	}

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

	async fn update_scheduler(&self, arg: tangram_index::scheduler::update::Arg) -> tg::Result<()> {
		self.update_scheduler(arg).await
	}
}
