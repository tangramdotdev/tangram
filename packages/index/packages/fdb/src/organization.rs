use {crate::Index, tangram_client::prelude::*};

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
		let items = args
			.iter()
			.cloned()
			.map(|arg| tangram_index::Item::PutOrganization(Box::new(arg)))
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

		Ok(())
	}

	pub async fn delete_organizations(&self, ids: &[tg::organization::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let items = ids
			.iter()
			.map(|id| {
				let arg = tangram_index::organization::delete::Arg { id: id.clone() };
				tangram_index::Item::DeleteOrganization(Box::new(arg))
			})
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

		Ok(())
	}

	pub async fn put_organization_members(
		&self,
		args: &[tangram_index::organization::member::put::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let items = args
			.iter()
			.cloned()
			.map(|arg| tangram_index::Item::PutOrganizationMember(Box::new(arg)))
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

		Ok(())
	}

	pub async fn delete_organization_members(
		&self,
		args: &[tangram_index::organization::member::delete::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let items = args
			.iter()
			.cloned()
			.map(|arg| tangram_index::Item::DeleteOrganizationMember(Box::new(arg)))
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

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
