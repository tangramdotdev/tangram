use tangram_client::prelude::*;

pub mod delete;
pub mod put;

#[derive(Clone, Debug)]
pub(crate) struct Fact {
	pub creator: Option<tg::Principal>,
	pub implicit: bool,
	pub permission: tg::authorization::Permission,
	pub resource: tg::Id,
	pub subject: tg::authorization::Subject,
}

#[must_use]
pub(crate) fn is_process_implicit(
	creator: Option<&tg::Principal>,
	implicit: bool,
	subject: &tg::authorization::Subject,
) -> bool {
	implicit
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
	pub(crate) fn is_process_implicit(&self) -> bool {
		is_process_implicit(self.creator.as_ref(), self.implicit, &self.subject)
	}
}
