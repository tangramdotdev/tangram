use {
	super::Store,
	crate::{Grant, GrantArg},
	tangram_client::prelude::*,
};

impl Store {
	pub fn grant(&self, arg: GrantArg) {
		self.put_grant(arg.id, &arg.principal, arg.subtree, arg.created_at);
	}

	pub fn grant_batch(&self, args: Vec<GrantArg>) {
		for arg in args {
			self.grant(arg);
		}
	}

	pub(super) fn put_grant(
		&self,
		id: tg::object::Id,
		principal: &tg::Principal,
		subtree: bool,
		created_at: i64,
	) {
		let key = (id, principal.to_string());
		self.grants
			.entry(key)
			.and_modify(|grant| {
				grant.created_at = created_at;
				grant.subtree = grant.subtree || subtree;
			})
			.or_insert(Grant {
				created_at,
				subtree,
			});
	}
}
