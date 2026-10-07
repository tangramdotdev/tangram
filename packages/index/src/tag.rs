use {futures::FutureExt as _, tangram_client::prelude::*};

pub mod put;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Tag {
	#[tangram_serialize(default, id = 5, skip_serializing_if = "Option::is_none")]
	pub account: Option<crate::usage::Account>,

	#[tangram_serialize(id = 1)]
	pub name: String,

	#[tangram_serialize(default, id = 2, skip_serializing_if = "Option::is_none")]
	pub parent: Option<tg::Id>,

	#[tangram_serialize(id = 3)]
	pub specifier: tg::Specifier,

	#[tangram_serialize(id = 0)]
	pub target: tg::Either<tg::object::Id, tg::process::Id>,
	#[tangram_serialize(id = 6)]
	pub version: String,
}

pub trait Index {
	fn try_get_tags(
		&self,
		ids: &[tg::tag::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::tag::Tag>>>> + Send;

	fn try_get_tag(
		&self,
		id: &tg::tag::Id,
	) -> impl Future<Output = tg::Result<Option<crate::tag::Tag>>> + Send {
		self.try_get_tags(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn put_tags(
		&self,
		args: &[crate::tag::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_tags(&self, ids: &[tg::tag::Id]) -> impl Future<Output = tg::Result<()>> + Send;
}

impl Tag {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the tag"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the tag"))
	}
}
