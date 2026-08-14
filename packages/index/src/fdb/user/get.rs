use {
	crate::fdb::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	tangram_client::prelude::*,
};

impl Index {
	pub async fn try_get_users(
		&self,
		ids: &[tg::user::Id],
	) -> tg::Result<Vec<Option<crate::user::User>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = crate::read::Request::TryGetUsers {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let crate::read::Response::TryGetUsers(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub(crate) async fn try_get_users_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		ids: &[tg::user::Id],
	) -> crate::fdb::Result<Vec<Option<crate::user::User>>> {
		futures::future::try_join_all(
			ids.iter()
				.map(|id| Self::try_get_user_with_transaction(txn, subspace, id)),
		)
		.await
	}

	pub(crate) async fn try_get_user_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::user::Id,
	) -> crate::fdb::Result<Option<crate::user::User>> {
		let key = Key::User(crate::fdb::user::Key::User(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = txn.get(&key, false).await?;
		let Some(bytes) = bytes else {
			return Ok(None);
		};
		Ok(Some(
			crate::user::User::deserialize(&bytes).map_err(crate::fdb::custom_error)?,
		))
	}
}
