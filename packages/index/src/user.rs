use {futures::FutureExt as _, tangram_client::prelude::*};

pub mod put;

#[derive(
	Clone, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub struct User {
	#[tangram_serialize(id = 1)]
	pub billing_ready: bool,

	#[tangram_serialize(id = 0)]
	pub specifier: tg::Specifier,
}

pub trait Index {
	fn try_get_users(
		&self,
		ids: &[tg::user::Id],
	) -> impl Future<Output = tg::Result<Vec<Option<crate::user::User>>>> + Send;

	fn try_get_user(
		&self,
		id: &tg::user::Id,
	) -> impl Future<Output = tg::Result<Option<crate::user::User>>> + Send {
		self.try_get_users(std::slice::from_ref(id))
			.map(|result| result.map(|mut output| output.pop().unwrap()))
	}

	fn put_users(
		&self,
		args: &[crate::user::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_users(&self, ids: &[tg::user::Id]) -> impl Future<Output = tg::Result<()>> + Send;
}

impl User {
	pub fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the user"))
	}

	pub fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the user"))
	}
}
