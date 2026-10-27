use {
	std::collections::BTreeMap,
	tangram_client::prelude::*,
	tangram_server_core::authorization::{Outcome, Output},
};

#[cfg(test)]
mod tests;

#[derive(Default)]
pub(crate) struct Proofs {
	known: bool,
	permissions: BTreeMap<tg::authorization::Permission, Option<i64>>,
}

impl Proofs {
	pub(crate) fn insert(&mut self, requested: tg::authorization::permission::Set, output: Output) {
		if output.permissions.kind() != requested.kind() {
			return;
		}
		self.known = true;
		for permission in requested.iter().filter(|permission| {
			output
				.permissions
				.iter()
				.any(|proof| proof.implies(*permission))
		}) {
			self.permissions
				.entry(permission)
				.and_modify(|expires_at| {
					*expires_at = match (*expires_at, output.expires_at) {
						(Some(left), Some(right)) => Some(left.max(right)),
						_ => None,
					};
				})
				.or_insert(output.expires_at);
		}
	}

	#[must_use]
	pub(crate) fn output(&self, requested: tg::authorization::permission::Set) -> Option<Output> {
		let mut permissions = requested.empty_like();
		let mut expires_at = None;
		for (permission, expiration) in &self.permissions {
			permissions.insert(tg::authorization::permission::Set::from_permission(
				*permission,
			));
			if let Some(expiration) = expiration {
				expires_at =
					Some(expires_at.map_or(*expiration, |value: i64| value.min(*expiration)));
			}
		}
		self.known.then_some(Output {
			expires_at,
			outcome: if permissions.contains(requested) {
				Outcome::Satisfied
			} else {
				Outcome::Unsatisfied
			},
			permissions,
		})
	}
}
