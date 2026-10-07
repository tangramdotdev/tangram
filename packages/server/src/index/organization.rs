use {super::Index, tangram_client::prelude::*, tangram_index as index};

impl Index {
	pub async fn try_get_organizations(
		&self,
		ids: &[tg::organization::Id],
	) -> tg::Result<Vec<Option<index::organization::Organization>>> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.try_get_organizations(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.try_get_organizations(ids).await,
		}
	}

	pub async fn put_organizations(
		&self,
		args: &[index::organization::put::Arg],
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_organizations(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_organizations(args).await,
		}
	}

	pub async fn delete_organizations(&self, ids: &[tg::organization::Id]) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_organizations(ids).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_organizations(ids).await,
		}
	}

	pub async fn put_organization_members(
		&self,
		args: &[index::organization::member::put::Arg],
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.put_organization_members(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.put_organization_members(args).await,
		}
	}

	pub async fn delete_organization_members(
		&self,
		args: &[index::organization::member::delete::Arg],
	) -> tg::Result<()> {
		match self {
			#[cfg(feature = "foundationdb")]
			Self::Fdb(index) => index.delete_organization_members(args).await,
			#[cfg(feature = "lmdb")]
			Self::Lmdb(index) => index.delete_organization_members(args).await,
		}
	}
}

impl index::organization::Index for Index {
	async fn try_get_organizations(
		&self,
		ids: &[tg::organization::Id],
	) -> tg::Result<Vec<Option<index::organization::Organization>>> {
		self.try_get_organizations(ids).await
	}

	async fn put_organizations(&self, args: &[index::organization::put::Arg]) -> tg::Result<()> {
		self.put_organizations(args).await
	}

	async fn delete_organizations(&self, ids: &[tg::organization::Id]) -> tg::Result<()> {
		self.delete_organizations(ids).await
	}

	async fn put_organization_members(
		&self,
		args: &[index::organization::member::put::Arg],
	) -> tg::Result<()> {
		self.put_organization_members(args).await
	}

	async fn delete_organization_members(
		&self,
		args: &[index::organization::member::delete::Arg],
	) -> tg::Result<()> {
		self.delete_organization_members(args).await
	}
}
