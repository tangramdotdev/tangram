use {futures::FutureExt as _, tangram_client::prelude::*};

pub mod member;
pub mod put;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct Organization {
	#[tangram_serialize(id = 1)]
	pub billing_ready: bool,

	#[tangram_serialize(id = 0)]
	pub specifier: tg::Specifier,
}

pub trait Index {
	fn try_get_organizations(
		&self,
		ids: &[tg::organization::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::organization::Organization>>>> + Send;

	fn try_get_organization(
		&self,
		id: &tg::organization::Id,
	) -> impl Future<Output = tg::Result<Option<crate::organization::Organization>>> + Send {
		self.try_get_organizations(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn put_organizations(
		&self,
		args: &[crate::organization::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_organizations(
		&self,
		ids: &[tg::organization::Id],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_organization_members(
		&self,
		args: &[crate::organization::member::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_organization_members(
		&self,
		args: &[crate::organization::member::delete::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;
}

impl Organization {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the organization"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the organization"))
	}
}
