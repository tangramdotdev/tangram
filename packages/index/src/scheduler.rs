use tangram_client::prelude::*;

pub mod delete;
pub mod get;
pub mod put;
pub mod update;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Scheduler {
	pub available: bool,
	pub id: tg::scheduler::Id,
}

#[derive(tangram_serialize::Deserialize, tangram_serialize::Serialize)]
struct Data {
	#[tangram_serialize(id = 0)]
	available: bool,
}

pub trait Index {
	fn delete_scheduler(
		&self,
		arg: crate::scheduler::delete::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_schedulers(
		&self,
	) -> impl Future<Output = tg::Result<Vec<crate::scheduler::Scheduler>>> + Send;

	fn put_scheduler(
		&self,
		arg: crate::scheduler::put::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn try_get_scheduler(
		&self,
		arg: crate::scheduler::get::Arg,
	) -> impl Future<Output = tg::Result<Option<crate::scheduler::Scheduler>>> + Send;

	fn update_scheduler(
		&self,
		arg: crate::scheduler::update::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;
}

impl Scheduler {
	#[must_use]
	pub fn new(id: tg::scheduler::Id) -> Self {
		Self {
			available: false,
			id,
		}
	}

	pub fn deserialize(id: tg::scheduler::Id, bytes: &[u8]) -> tg::Result<Self> {
		let data: Data = tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the scheduler"))?;
		let scheduler = Self {
			available: data.available,
			id,
		};

		Ok(scheduler)
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		let data = Data {
			available: self.available,
		};
		let bytes = tangram_serialize::to_vec(&data)
			.map_err(|error| tg::error!(!error, "failed to serialize the scheduler"))?;

		Ok(bytes)
	}
}
