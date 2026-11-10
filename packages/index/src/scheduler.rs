use tangram_client::prelude::*;

pub mod get;
pub mod put;
pub mod sandbox;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Scheduler {
	pub id: tg::scheduler::Id,
}

#[derive(tangram_serialize::Deserialize, tangram_serialize::Serialize)]
struct Data {}

pub trait Index {
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
}

impl Scheduler {
	#[must_use]
	pub fn new(id: tg::scheduler::Id) -> Self {
		Self { id }
	}

	pub fn deserialize(id: tg::scheduler::Id, bytes: &[u8]) -> tg::Result<Self> {
		let Data {} = tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the scheduler"))?;
		let scheduler = Self { id };

		Ok(scheduler)
	}

	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		let data = Data {};
		let bytes = tangram_serialize::to_vec(&data)
			.map_err(|error| tg::error!(!error, "failed to serialize the scheduler"))?;

		Ok(bytes)
	}
}
