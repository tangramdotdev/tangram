use {super::Index, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn try_get_tags(
		&self,
		ids: &[tg::tag::Id],
	) -> tg::Result<Vec<Option<index::tag::Tag>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_tags(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_tags(ids).await,
		}
	}

	pub async fn put_tags(&self, args: &[index::tag::put::Arg]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_tags(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_tags(args).await,
		}
	}

	pub async fn delete_tags(&self, ids: &[tg::tag::Id]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_tags(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_tags(ids).await,
		}
	}
}

impl index::tag::Index for Index {
	async fn try_get_tags(&self, ids: &[tg::tag::Id]) -> tg::Result<Vec<Option<index::tag::Tag>>> {
		self.try_get_tags(ids).await
	}

	async fn put_tags(&self, args: &[index::tag::put::Arg]) -> tg::Result<()> {
		self.put_tags(args).await
	}

	async fn delete_tags(&self, ids: &[tg::tag::Id]) -> tg::Result<()> {
		self.delete_tags(ids).await
	}
}
