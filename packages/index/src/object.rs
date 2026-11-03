use {
	futures::FutureExt as _, std::time::Duration, tangram_client::prelude::*,
	tangram_util::serde::is_default,
};

pub use tg::object::storage;

pub mod put;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Object {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "is_default")]
	pub checkout: Option<tg::artifact::Id>,

	#[tangram_serialize(default, id = 1, skip_serializing_if = "is_default")]
	pub metadata: tg::object::Metadata,

	#[tangram_serialize(id = 5)]
	pub put: [u8; 16],

	#[tangram_serialize(default, id = 2, skip_serializing_if = "is_default")]
	pub reference_count: u64,

	#[tangram_serialize(default, id = 3, skip_serializing_if = "is_default")]
	pub storage: storage::Set,

	#[tangram_serialize(id = 4)]
	pub touched_at: i64,
}

pub trait Index {
	fn try_get_object_children(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<Vec<tg::object::Id>>>> + Send;

	fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::object::Object>>>> + Send;

	fn try_get_object(
		&self,
		id: &tg::object::Id,
	) -> impl Future<Output = tg::Result<Option<crate::object::Object>>> + Send {
		self.try_get_objects(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::object::Object>>>> + Send;

	fn touch_objects_with_account(
		&self,
		ids: &[tg::object::Id],
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Vec<Option<crate::object::Object>>>> + Send;

	fn touch_object(
		&self,
		id: &tg::object::Id,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::object::Object>>> + Send {
		self.touch_objects(std::slice::from_ref(id), touched_at, time_to_touch)
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn touch_object_with_account(
		&self,
		id: &tg::object::Id,
		account: Option<&crate::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> impl Future<Output = tg::Result<Option<crate::object::Object>>> + Send {
		self.touch_objects_with_account(
			std::slice::from_ref(id),
			account,
			touched_at,
			time_to_touch,
		)
		.map(|result| result.map(|mut output| output.pop().unwrap()))
	}
}

impl Object {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the object"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the object"))
	}
}
