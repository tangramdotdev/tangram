use {super::Index, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn try_get_users(
		&self,
		ids: &[tg::user::Id],
	) -> tg::Result<Vec<Option<index::user::User>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_users(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_users(ids).await,
		}
	}

	pub async fn put_users(&self, args: &[index::user::put::Arg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_users(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_users(args).await,
		}
	}

	pub async fn delete_users(&self, ids: &[tg::user::Id]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_users(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_users(ids).await,
		}
	}
}

impl index::user::Index for Index {
	async fn try_get_users(
		&self,
		ids: &[tg::user::Id],
	) -> tg::Result<Vec<Option<index::user::User>>> {
		self.try_get_users(ids).await
	}

	async fn put_users(&self, args: &[index::user::put::Arg]) -> tg::Result<()> {
		self.put_users(args).await
	}

	async fn delete_users(&self, ids: &[tg::user::Id]) -> tg::Result<()> {
		self.delete_users(ids).await
	}
}
