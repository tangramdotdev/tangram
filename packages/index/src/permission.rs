use tangram_client::prelude::*;

pub mod capture;
pub mod delete;
pub mod get;
pub mod put;

#[derive(
	Clone, Copy, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub enum Source {
	#[tangram_serialize(id = 0)]
	Direct {
		#[tangram_serialize(id = 0)]
		expires_at: Option<i64>,
	},
	#[tangram_serialize(id = 1)]
	Grant,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Expiration {
	At(i64),
	Never,
}

#[derive(Clone, Debug)]
pub struct Fact {
	pub direct: bool,
	pub permission: tg::authorization::Permission,
	pub resource: tg::Id,
	pub subject: tg::authorization::Subject,
}

pub trait Index {
	fn enqueue_permission_capture(
		&self,
		arg: crate::permission::capture::enqueue::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn permission_capture_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> impl Future<Output = tg::Result<Vec<crate::permission::capture::Entry>>> + Send;

	fn complete_permission_capture(
		&self,
		entry: &crate::permission::capture::Entry,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn get_permissions(
		&self,
		args: &[crate::permission::get::Arg],
	) -> impl Future<Output = tg::Result<Vec<Vec<tg::authorization::Permission>>>> + Send;

	fn put_permissions(
		&self,
		args: &[crate::permission::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_permissions(
		&self,
		args: &[crate::permission::delete::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;
}

impl Expiration {
	#[must_use]
	pub fn timestamp(self) -> Option<i64> {
		match self {
			Self::At(timestamp) => Some(timestamp),
			Self::Never => None,
		}
	}
}

impl From<Option<i64>> for Expiration {
	fn from(timestamp: Option<i64>) -> Self {
		timestamp.map_or(Self::Never, Self::At)
	}
}

impl<'de> tangram_serialize::Deserialize<'de> for Expiration {
	fn deserialize(
		deserializer: &mut tangram_serialize::Deserializer<'de>,
	) -> std::io::Result<Self> {
		<Option<i64> as tangram_serialize::Deserialize>::deserialize(deserializer).map(Self::from)
	}
}

impl tangram_serialize::Serialize for Expiration {
	fn serialize(&self, serializer: &mut tangram_serialize::Serializer<'_>) -> std::io::Result<()> {
		tangram_serialize::Serialize::serialize(&self.timestamp(), serializer)
	}
}

#[cfg(test)]
mod tests {
	use super::Expiration;

	#[test]
	fn expiration_encoding() {
		for (expiration, timestamp) in [
			(Expiration::Never, None),
			(Expiration::At(42), Some(42_i64)),
		] {
			let bytes = tangram_serialize::to_vec(&expiration).unwrap();
			assert_eq!(tangram_serialize::to_vec(&timestamp).unwrap(), bytes);
			assert_eq!(
				tangram_serialize::from_slice::<Expiration>(&bytes).unwrap(),
				expiration
			);
		}
	}
}
