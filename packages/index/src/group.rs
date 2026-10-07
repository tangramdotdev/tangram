use {futures::FutureExt as _, tangram_client::prelude::*};

pub mod member;
pub mod put;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Group {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::Id>,

	#[tangram_serialize(id = 1)]
	pub specifier: tg::Specifier,
}

pub trait Index {
	fn try_get_groups(
		&self,
		ids: &[tg::group::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::group::Group>>>> + Send;

	fn try_get_group(
		&self,
		id: &tg::group::Id,
	) -> impl Future<Output = tg::Result<Option<crate::group::Group>>> + Send {
		self.try_get_groups(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn put_groups(
		&self,
		args: &[crate::group::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_groups(&self, ids: &[tg::group::Id]) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_group_members(
		&self,
		args: &[crate::group::member::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_group_members(
		&self,
		args: &[crate::group::member::delete::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;
}

impl Group {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the group"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the group"))
	}
}
