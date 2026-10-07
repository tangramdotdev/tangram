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
	pub async fn try_get_users(
		&self,
		ids: &[tg::user::Id],
	) -> tg::Result<Vec<Option<tangram_index::user::User>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = tangram_index::read::Request::TryGetUsers {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetUsers(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn put_users(&self, args: &[tangram_index::user::put::Arg]) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::PutUsers(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}

	pub async fn delete_users(&self, ids: &[tg::user::Id]) -> tg::Result<()> {
		if ids.is_empty() {
			return Ok(());
		}
		let request = Request::DeleteUsers(ids.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(())
	}
}

impl tangram_index::user::Index for Index {
	async fn try_get_users(
		&self,
		ids: &[tg::user::Id],
	) -> tg::Result<Vec<Option<tangram_index::user::User>>> {
		self.try_get_users(ids).await
	}

	async fn put_users(&self, args: &[tangram_index::user::put::Arg]) -> tg::Result<()> {
		self.put_users(args).await
	}

	async fn delete_users(&self, ids: &[tg::user::Id]) -> tg::Result<()> {
		self.delete_users(ids).await
	}
}
