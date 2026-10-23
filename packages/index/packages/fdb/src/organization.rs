use {
	crate::{Index, Request, Response},
	tangram_client::prelude::*,
};

mod delete;
mod get;
mod key;
mod put;

pub(super) use key::Key;

impl Index {
	pub async fn try_get_organizations(
		&self,
		ids: &[tg::organization::Id],
	) -> tg::Result<Vec<Option<tangram_index::organization::Organization>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = tangram_index::read::Request::TryGetOrganizations {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetOrganizations(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_organizations(
		&self,
		args: &[tangram_index::organization::put::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::PutOrganizations(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub async fn delete_organizations(&self, ids: &[tg::organization::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteOrganizations(ids.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub async fn put_organization_members(
		&self,
		args: &[tangram_index::organization::member::put::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::PutOrganizationMembers(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub async fn delete_organization_members(
		&self,
		args: &[tangram_index::organization::member::delete::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteOrganizationMembers(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}
}

impl tangram_index::organization::Index for Index {
	async fn try_get_organizations(
		&self,
		ids: &[tg::organization::Id],
	) -> tg::Result<Vec<Option<tangram_index::organization::Organization>>> {
		self.try_get_organizations(ids).await
	}

	async fn put_organizations(
		&self,
		args: &[tangram_index::organization::put::Arg],
	) -> tg::Result<()> {
		self.put_organizations(args).await
	}

	async fn delete_organizations(&self, ids: &[tg::organization::Id]) -> tg::Result<()> {
		self.delete_organizations(ids).await
	}

	async fn put_organization_members(
		&self,
		args: &[tangram_index::organization::member::put::Arg],
	) -> tg::Result<()> {
		self.put_organization_members(args).await
	}

	async fn delete_organization_members(
		&self,
		args: &[tangram_index::organization::member::delete::Arg],
	) -> tg::Result<()> {
		self.delete_organization_members(args).await
	}
}
