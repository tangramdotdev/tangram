use tangram_client::prelude::*;

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
pub(crate) struct Fact {
	pub creator: Option<tg::Principal>,
	pub direct: bool,
	pub permission: tg::authorization::Permission,
	pub resource: tg::Id,
	pub subject: tg::authorization::Subject,
}

#[must_use]
pub(crate) fn is_process_direct(
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

impl Fact {
	#[must_use]
	pub(crate) fn is_process_direct(&self) -> bool {
		is_process_direct(self.creator.as_ref(), self.direct, &self.subject)
	}
}
