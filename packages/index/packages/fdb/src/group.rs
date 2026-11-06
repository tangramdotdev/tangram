use {crate::Index, tangram_client::prelude::*};

mod delete;
mod get;
mod key;
mod put;

pub(super) use key::Key;

impl Index {
	pub async fn try_get_groups(
		&self,
		ids: &[tg::group::Id],
	) -> tg::Result<Vec<Option<tangram_index::group::Group>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = tangram_index::read::Request::TryGetGroups {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetGroups(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_groups(&self, args: &[tangram_index::group::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let items = args
			.iter()
			.cloned()
			.map(|arg| tangram_index::Item::PutGroup(Box::new(arg)))
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

		Ok(())
	}

	pub async fn delete_groups(&self, ids: &[tg::group::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let items = ids
			.iter()
			.map(|id| {
				let arg = tangram_index::group::delete::Arg { id: id.clone() };
				tangram_index::Item::DeleteGroup(Box::new(arg))
			})
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

		Ok(())
	}

	pub async fn put_group_members(
		&self,
		args: &[tangram_index::group::member::put::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let items = args
			.iter()
			.cloned()
			.map(|arg| tangram_index::Item::PutGroupMember(Box::new(arg)))
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

		Ok(())
	}

	pub async fn delete_group_members(
		&self,
		args: &[tangram_index::group::member::delete::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let items = args
			.iter()
			.cloned()
			.map(|arg| tangram_index::Item::DeleteGroupMember(Box::new(arg)))
			.collect();
		let arg = tangram_index::write::Arg { items };
		self.write(arg).await??;

		Ok(())
	}
}

impl tangram_index::group::Index for Index {
	async fn try_get_groups(
		&self,
		ids: &[tg::group::Id],
	) -> tg::Result<Vec<Option<tangram_index::group::Group>>> {
		self.try_get_groups(ids).await
	}

	async fn put_groups(&self, args: &[tangram_index::group::put::Arg]) -> tg::Result<()> {
		self.put_groups(args).await
	}

	async fn delete_groups(&self, ids: &[tg::group::Id]) -> tg::Result<()> {
		self.delete_groups(ids).await
	}

	async fn put_group_members(
		&self,
		args: &[tangram_index::group::member::put::Arg],
	) -> tg::Result<()> {
		self.put_group_members(args).await
	}

	async fn delete_group_members(
		&self,
		args: &[tangram_index::group::member::delete::Arg],
	) -> tg::Result<()> {
		self.delete_group_members(args).await
	}
}
