use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Group(tg::group::Id),
	GroupMember {
		group: tg::group::Id,
		member: tg::group::Member,
	},
	MemberGroup {
		member: tg::group::Member,
		group: tg::group::Id,
	},
}
