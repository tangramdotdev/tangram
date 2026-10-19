use {
	crate::{Index, Request, Response},
	std::time::Duration,
	tangram_client::prelude::*,
};

mod get;
mod key;
mod put;
mod touch;

pub(super) use key::Key;

impl Index {
	pub async fn try_get_object_children(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<Vec<tg::object::Id>>> {
		let request = tangram_index::read::Request::TryGetObjectChildren { id: id.clone() };
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetObjectChildren(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tangram_index::object::Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = tangram_index::read::Request::TryGetObjects {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetObjects(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::object::Object>>> {
		self.touch_objects_with_account(ids, None, touched_at, time_to_touch)
			.await
	}

	pub async fn touch_objects_with_account(
		&self,
		ids: &[tg::object::Id],
		account: Option<&tangram_index::usage::Account>,
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::object::Object>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = Request::TouchObjects(crate::TouchObjects {
			account: account.cloned(),
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		});
		let response = self.send_write_request(request).await?;
		let Response::Objects(objects) = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(objects)
	}
}

impl tangram_index::object::Index for Index {
	async fn try_get_object_children(
		&self,
		id: &tg::object::Id,
	) -> tg::Result<Option<Vec<tg::object::Id>>> {
		self.try_get_object_children(id).await
	}

	async fn try_get_objects(
		&self,
		ids: &[tg::object::Id],
	) -> tg::Result<Vec<Option<tangram_index::object::Object>>> {
		self.try_get_objects(ids).await
	}

	async fn touch_objects(
		&self,
		ids: &[tg::object::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<tangram_index::object::Object>>> {
		self.touch_objects(ids, touched_at, time_to_touch).await
	}

	async fn touch_objects_with_account(
		&self,
		ids: &[tg::object::Id],
		account: Option<&tangram_index::usage::Account>,
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<tangram_index::object::Object>>> {
		self.touch_objects_with_account(ids, account, touched_at, time_to_touch)
			.await
	}
}
