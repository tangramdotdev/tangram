use tangram_client::prelude::*;

pub mod capture;
pub mod delete;
pub mod put;

#[derive(
	Clone, Copy, Debug, Eq, PartialEq, tangram_serialize::Deserialize, tangram_serialize::Serialize,
)]
pub enum Source {
	#[tangram_serialize(id = 0)]
	Direct {
		#[tangram_serialize(id = 0)]
		expires_at: Option<i64>,
	},
	#[tangram_serialize(id = 1)]
	Grant,
}

#[derive(Clone, Debug)]
pub struct Fact {
	pub creator: Option<tg::Principal>,
	pub direct: bool,
	pub permission: tg::authorization::Permission,
	pub resource: tg::Id,
	pub subject: tg::authorization::Subject,
}

#[must_use]
pub fn is_process_direct(
	creator: Option<&tg::Principal>,
	direct: bool,
	subject: &tg::authorization::Subject,
) -> bool {
	direct
		&& matches!(
			(creator, subject),
			(
				Some(tg::Principal::Process(creator)),
				tg::authorization::Subject::Process(subject),
			) if creator == subject
		)
}

pub trait Index {
	fn enqueue_permission_capture(
		&self,
		arg: crate::permission::capture::enqueue::Arg,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn permission_capture_batch(
		&self,
		batch_size: usize,
		partition_start: u64,
		partition_end: u64,
	) -> impl Future<Output = tg::Result<Vec<crate::permission::capture::Entry>>> + Send;

	fn complete_permission_capture(
		&self,
		entry: &crate::permission::capture::Entry,
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn put_permissions(
		&self,
		args: &[crate::permission::put::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;

	fn delete_permissions(
		&self,
		args: &[crate::permission::delete::Arg],
	) -> impl Future<Output = tg::Result<()>> + Send;
}

impl Fact {
	#[must_use]
	pub fn is_process_direct(&self) -> bool {
		is_process_direct(self.creator.as_ref(), self.direct, &self.subject)
	}
}
