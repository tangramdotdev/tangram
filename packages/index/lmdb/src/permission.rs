use {
	crate::{Index, Request, Response},
	tangram_client::prelude::*,
	tangram_index::permission::capture::{Entry, enqueue},
};

mod capture;
mod delete;
mod get;
mod key;
mod put;

pub(super) use key::Key;

#[derive(
	Clone, Copy, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub(crate) enum PermissionSource {
	#[tangram_serialize(id = 1)]
	Direct,
	#[tangram_serialize(id = 0)]
	Grant,
	#[tangram_serialize(id = 2)]
	Materialized,
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	PartialEq,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[allow(clippy::option_option)]
pub(crate) struct PermissionValue {
	#[tangram_serialize(default, id = 0, skip_serializing_if = "tangram_util::serde::is_false")]
	pub grant: bool,

	#[tangram_serialize(
		default,
		id = 1,
		skip_serializing_if = "Option::is_none",
		with = "tangram_serialize::with::unwrap_or_skip"
	)]
	pub direct: Option<Option<i64>>,

	#[tangram_serialize(
		default,
		id = 2,
		skip_serializing_if = "Option::is_none",
		with = "tangram_serialize::with::unwrap_or_skip"
	)]
	pub materialized: Option<Option<i64>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
#[allow(clippy::option_option)]
pub(crate) struct PermissionEntry {
	pub creator: Option<tg::Principal>,
	pub grant: bool,
	pub direct: Option<Option<i64>>,
	pub materialized: Option<Option<i64>>,
	pub permission: tg::authorization::Permission,
	pub subject: tg::authorization::Subject,
}

#[derive(Clone)]
pub(crate) struct PermissionIndexEntry<'a> {
	pub creator: Option<&'a tg::Principal>,
	pub expires_at: Option<i64>,
	pub permission: tg::authorization::Permission,
	pub subject: &'a tg::authorization::Subject,
	pub resource: &'a tg::Id,
}

impl Index {
	pub async fn enqueue_permission_capture(&self, arg: enqueue::Arg) -> tg::Result<()> {
		let arg = tangram_index::batch::Arg {
			items: vec![tangram_index::batch::Item::EnqueuePermissionCapture(arg)],
		};
		self.batch(arg).await??;
		Ok(())
	}

	pub async fn permission_capture_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<Entry>> {
		let request = tangram_index::read::Request::PermissionCaptureBatch {
			batch_size,
			partition_end,
			partition_start,
		};
		let response = self.send_read_request(request).await?;
		let tangram_index::read::Response::PermissionCaptureBatch(entries) = response else {
			return Err(tg::error!("unexpected permission capture batch response"));
		};
		Ok(entries)
	}

	pub async fn complete_permission_capture(&self, entry: &Entry) -> tg::Result<()> {
		let request = Request::CompletePermissionCapture(entry.clone());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!(
				"unexpected permission capture completion response"
			));
		};
		Ok(())
	}

	pub async fn put_permissions(
		&self,
		args: &[tangram_index::permission::put::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::PutPermissions(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}

	pub async fn delete_permissions(
		&self,
		args: &[tangram_index::permission::delete::Arg],
	) -> tg::Result<()> {
		if args.is_empty() {
			return Ok(());
		}
		let request = Request::DeletePermissions(args.to_vec());
		let response = self.send_write_request(request).await?;
		let Response::Unit = response else {
			return Err(tg::error!("unexpected write response"));
		};

		Ok(())
	}
}

impl PermissionValue {
	pub(crate) fn deserialize(bytes: &[u8]) -> tg::Result<Self> {
		tangram_serialize::from_slice(bytes)
			.map_err(|error| tg::error!(!error, "failed to deserialize the permission value"))
	}

	pub(crate) fn is_empty(&self) -> bool {
		!self.grant && self.direct.is_none() && self.materialized.is_none()
	}

	pub(crate) fn serialize(&self) -> tg::Result<Vec<u8>> {
		tangram_serialize::to_vec(self)
			.map_err(|error| tg::error!(!error, "failed to serialize the permission value"))
	}

	#[allow(clippy::option_option)]
	pub(crate) fn source_expires_at(&self, source: PermissionSource) -> Option<Option<i64>> {
		match source {
			PermissionSource::Direct => self.direct,
			PermissionSource::Grant => self.grant.then_some(None),
			PermissionSource::Materialized => self.materialized,
		}
	}

	pub(crate) fn put(
		&mut self,
		source: PermissionSource,
		expires_at: Option<i64>,
		time_to_touch: Option<std::time::Duration>,
	) -> bool {
		match source {
			PermissionSource::Direct => {
				if self.direct == Some(None) {
					return false;
				}
				let time_to_touch = time_to_touch
					.map(|value| i64::try_from(value.as_secs()).unwrap())
					.unwrap_or_default();
				if let (Some(Some(current)), Some(expires_at)) = (self.direct, expires_at)
					&& (current >= expires_at || expires_at.saturating_sub(current) < time_to_touch)
				{
					return false;
				}
				self.direct = Some(expires_at);
				true
			},
			PermissionSource::Grant => {
				if self.grant {
					false
				} else {
					self.grant = true;
					true
				}
			},
			PermissionSource::Materialized => {
				if self.materialized == Some(expires_at) {
					false
				} else {
					self.materialized = Some(expires_at);
					true
				}
			},
		}
	}

	pub(crate) fn delete(&mut self, source: PermissionSource, expires_at: Option<i64>) -> bool {
		match source {
			PermissionSource::Direct => {
				if self.direct == Some(expires_at) {
					self.direct = None;
					true
				} else {
					false
				}
			},
			PermissionSource::Grant => {
				if expires_at.is_some() || !self.grant {
					false
				} else {
					self.grant = false;
					true
				}
			},
			PermissionSource::Materialized => {
				if self.materialized == Some(expires_at) {
					self.materialized = None;
					true
				} else {
					false
				}
			},
		}
	}
}

impl PermissionSource {
	pub(crate) fn from_i32(value: i32) -> Option<Self> {
		match value {
			0 => Some(Self::Grant),
			1 => Some(Self::Direct),
			2 => Some(Self::Materialized),
			_ => None,
		}
	}

	pub(crate) fn to_i32(self) -> i32 {
		match self {
			Self::Direct => 1,
			Self::Grant => 0,
			Self::Materialized => 2,
		}
	}
}

impl PermissionEntry {
	#[allow(clippy::option_option)]
	pub(crate) fn effective_expires_at(&self) -> Option<Option<i64>> {
		let mut output = None;
		if self.grant {
			output = Some(None);
		}
		if let Some(expires_at) = self.direct {
			output = Some(match output {
				Some(output) => max_expires_at(output, expires_at),
				None => expires_at,
			});
		}
		if let Some(expires_at) = self.materialized {
			output = Some(match output {
				Some(output) => max_expires_at(output, expires_at),
				None => expires_at,
			});
		}
		output
	}

	pub(crate) fn has_non_materialized_cover(&self, expires_at: Option<i64>) -> bool {
		self.grant
			|| self
				.direct
				.is_some_and(|direct| max_expires_at(direct, expires_at) == direct)
	}

	pub(crate) fn is_non_expiring_process_direct(&self) -> bool {
		self.direct == Some(None)
			&& tangram_index::permission::is_process_direct(
				self.creator.as_ref(),
				self.direct.is_some(),
				&self.subject,
			)
	}
}

pub(crate) fn max_expires_at(left: Option<i64>, right: Option<i64>) -> Option<i64> {
	match (left, right) {
		(None, _) | (_, None) => None,
		(Some(left), Some(right)) => Some(left.max(right)),
	}
}

impl tangram_index::permission::Index for Index {
	async fn enqueue_permission_capture(
		&self,
		arg: tangram_index::permission::capture::enqueue::Arg,
	) -> tg::Result<()> {
		self.enqueue_permission_capture(arg).await
	}

	async fn permission_capture_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> tg::Result<Vec<tangram_index::permission::capture::Entry>> {
		self.permission_capture_batch(batch_size, partition_start, partition_end)
			.await
	}

	async fn complete_permission_capture(
		&self,
		entry: &tangram_index::permission::capture::Entry,
	) -> tg::Result<()> {
		self.complete_permission_capture(entry).await
	}

	async fn put_permissions(
		&self,
		args: &[tangram_index::permission::put::Arg],
	) -> tg::Result<()> {
		self.put_permissions(args).await
	}

	async fn delete_permissions(
		&self,
		args: &[tangram_index::permission::delete::Arg],
	) -> tg::Result<()> {
		self.delete_permissions(args).await
	}
}

#[cfg(test)]
mod tests {
	use super::{PermissionSource, PermissionValue};

	#[test]
	fn permission_source_ids_are_stable() {
		for (id, source) in [
			PermissionSource::Grant,
			PermissionSource::Direct,
			PermissionSource::Materialized,
		]
		.into_iter()
		.enumerate()
		{
			let id = i32::try_from(id).unwrap();
			assert_eq!(source.to_i32(), id);
			assert_eq!(PermissionSource::from_i32(id), Some(source));
		}
	}

	#[test]
	fn direct_permissions_upgrade_to_non_expiring() {
		let mut value = PermissionValue::default();
		assert!(value.put(PermissionSource::Direct, Some(10), None));
		assert_eq!(value.direct, Some(Some(10)));
		assert!(value.put(PermissionSource::Direct, None, None));
		assert_eq!(value.direct, Some(None));
		assert!(!value.put(PermissionSource::Direct, Some(20), None));
		assert_eq!(value.direct, Some(None));
	}
}
