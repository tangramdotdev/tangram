use tangram_client::prelude::*;

#[derive(Clone, Debug)]
pub enum Key {
	Organization(tg::organization::Id),
	OrganizationMember {
		organization: tg::organization::Id,
		member: tg::organization::Member,
	},
	MemberOrganization {
		member: tg::organization::Member,
		organization: tg::organization::Id,
	},
}
