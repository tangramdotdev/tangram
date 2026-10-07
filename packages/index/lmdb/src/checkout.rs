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
	pub async fn try_get_checkouts(
		&self,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<tangram_index::checkout::Checkout>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = tangram_index::read::Request::TryGetCheckouts {
			ids: ids.to_owned(),
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::TryGetCheckouts(output) = response else {
			return Err(tg::error!("unexpected read response"));
		};

		Ok(output)
	}

	pub async fn touch_checkouts(
		&self,
		ids: &[tg::Id],
		touched_at: i64,
		time_to_touch: Duration,
	) -> tg::Result<Vec<Option<tangram_index::checkout::Checkout>>> {
		if ids.is_empty() {
			return Ok(vec![]);
		}
		let request = Request::TouchCheckouts(crate::TouchCheckouts {
			ids: ids.to_vec(),
			time_to_touch,
			touched_at,
		});
		let response = self.send_write_request(request).await?;
		let Response::Checkouts(checkouts) = response else {
			return Err(tg::error!("unexpected write response"));
		};
		Ok(checkouts)
	}
}

impl tangram_index::checkout::Index for Index {
	async fn try_get_checkouts(
		&self,
		ids: &[tg::Id],
	) -> tg::Result<Vec<Option<tangram_index::checkout::Checkout>>> {
		self.try_get_checkouts(ids).await
	}

	async fn touch_checkouts(
		&self,
		ids: &[tg::Id],
		touched_at: i64,
		time_to_touch: std::time::Duration,
	) -> tg::Result<Vec<Option<tangram_index::checkout::Checkout>>> {
		self.touch_checkouts(ids, touched_at, time_to_touch).await
	}
}
