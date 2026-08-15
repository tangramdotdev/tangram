use {
	crate::fdb::{Index, Key},
	foundationdb as fdb,
	foundationdb_tuple::Subspace,
	std::ops::ControlFlow,
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
	) -> tg::Result<ControlFlow<Vec<Option<crate::user::User>>, fdb::FdbError>> {
		let users = {
			let result = futures::future::try_join_all(
				ids.iter()
					.map(|id| Self::try_get_user_with_transaction(txn, subspace, id)),
			)
			.await;
			let results = result?;
			let mut values = Vec::new();
			for result in results {
				let value = match result {
					ControlFlow::Break(value) => value,
					ControlFlow::Continue(error) => return Ok(ControlFlow::Continue(error)),
				};
				values.push(value);
			}
			values
		};

		Ok(ControlFlow::Break(users))
	}

	pub(crate) async fn try_get_user_with_transaction(
		txn: &fdb::Transaction,
		subspace: &Subspace,
		id: &tg::user::Id,
	) -> tg::Result<ControlFlow<Option<crate::user::User>, fdb::FdbError>> {
		let key = Key::User(crate::fdb::user::Key::User(id.clone()));
		let key = Self::pack(subspace, &key);
		let bytes = crate::fdb::retry!(txn.get(&key, false).await);
		let Some(bytes) = bytes else {
			return Ok(ControlFlow::Break(None));
		};
		let user = Some(crate::user::User::deserialize(&bytes)?);

		Ok(ControlFlow::Break(user))
	}
}
