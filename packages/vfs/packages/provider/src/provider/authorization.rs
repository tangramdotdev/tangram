use {
	super::now,
	std::collections::{BTreeMap, BTreeSet},
	tangram_client::prelude::*,
};

#[derive(Default)]
pub(super) struct Authorizations {
	nodes: BTreeMap<tg::object::Id, i64>,
	parents: BTreeMap<tg::object::Id, BTreeSet<tg::object::Id>>,
	subtrees: BTreeMap<tg::object::Id, i64>,
	tokens: BTreeMap<tg::Id, tg::authorization::Tokens>,
}

impl Authorizations {
	pub(super) fn insert(
		&mut self,
		id: &tg::Id,
		tokens: &tg::authorization::Tokens,
		verified: bool,
	) {
		let now = now();
		for (location, entry) in tokens.iter() {
			for token in &entry.authorization {
				if token.body.expires_at < now {
					continue;
				}
				let resource = if token.body.resource.kind() == tg::id::Kind::Sync {
					id
				} else {
					&token.body.resource
				};
				self.tokens
					.entry(resource.clone())
					.or_default()
					.insert_authorization(location.clone(), token.clone());
				if verified
					&& location.is_local()
					&& let Ok(object) = tg::object::Id::try_from(token.body.resource.clone())
				{
					let subtree = tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Subtree,
					);
					let node = tg::authorization::Permission::Object(
						tg::authorization::permission::object::Permission::Node,
					);
					let expirations = if token.body.authorizes(subtree) {
						&mut self.subtrees
					} else if token.body.authorizes(node) {
						&mut self.nodes
					} else {
						continue;
					};
					expirations
						.entry(object)
						.and_modify(|value| *value = (*value).max(token.body.expires_at))
						.or_insert(token.body.expires_at);
				}
			}
		}
		self.prune(now);
	}

	pub(super) fn inherit(&mut self, parent: &tg::object::Id, child: &tg::object::Id) {
		if parent != child {
			self.parents
				.entry(child.clone())
				.or_default()
				.insert(parent.clone());
		}
	}

	pub(super) fn tokens(&self, id: &tg::object::Id) -> tg::authorization::Tokens {
		let mut tokens = tg::authorization::Tokens::default();
		for id in self.path_ids(id) {
			if let Some(existing) = self.tokens.get(&id.into()) {
				for (location, entry) in existing.iter() {
					for token in entry
						.authorization
						.iter()
						.filter(|token| token.body.expires_at >= now())
					{
						tokens.insert_authorization(location.clone(), token.clone());
					}
				}
			}
		}

		tokens
	}

	pub(super) fn authorized(&self, id: &tg::object::Id) -> bool {
		self.nodes
			.get(id)
			.is_some_and(|expires_at| *expires_at >= now())
			|| self.path_ids(id).into_iter().any(|id| {
				self.subtrees
					.get(&id)
					.is_some_and(|expires_at| *expires_at >= now())
			})
	}

	fn path_ids(&self, id: &tg::object::Id) -> BTreeSet<tg::object::Id> {
		let mut ids = BTreeSet::new();
		let mut stack = vec![id.clone()];
		while let Some(id) = stack.pop() {
			if ids.insert(id.clone())
				&& let Some(parents) = self.parents.get(&id)
			{
				stack.extend(parents.iter().cloned());
			}
		}

		ids
	}

	fn prune(&mut self, now: i64) {
		self.nodes.retain(|_, expires_at| *expires_at >= now);
		self.subtrees.retain(|_, expires_at| *expires_at >= now);
		self.tokens.retain(|_, tokens| {
			let mut retained = tg::authorization::Tokens::default();
			for (location, entry) in tokens.iter() {
				for token in entry
					.authorization
					.iter()
					.filter(|token| token.body.expires_at >= now)
				{
					retained.insert_authorization(location.clone(), token.clone());
				}
			}
			*tokens = retained;
			!tokens.is_empty()
		});
	}
}

#[cfg(test)]
mod tests {
	use {
		super::{Authorizations, now},
		tangram_client::prelude::*,
	};

	#[test]
	fn renewed_parent_proofs_reach_existing_descendants_and_cycles() {
		let parent: tg::object::Id = tg::directory::Id::new(b"parent").into();
		let child: tg::object::Id = tg::file::Id::new(b"child").into();
		let mut authorizations = Authorizations::default();
		authorizations.inherit(&parent, &child);
		authorizations.inherit(&child, &parent);
		let expired = tokens(
			parent.clone().into(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
			now() - 1,
		);
		authorizations.insert(&parent.clone().into(), &expired, true);
		assert!(!authorizations.authorized(&child));

		let fresh = tokens(
			parent.clone().into(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
			now() + 60,
		);
		authorizations.insert(&parent.clone().into(), &fresh, true);
		assert!(authorizations.authorized(&child));
		assert_eq!(authorizations.tokens(&child), fresh);
	}

	#[test]
	fn node_proofs_do_not_authorize_descendants() {
		let parent: tg::object::Id = tg::directory::Id::new(b"parent").into();
		let child: tg::object::Id = tg::file::Id::new(b"child").into();
		let mut authorizations = Authorizations::default();
		let tokens = tokens(
			parent.clone().into(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			),
			now() + 60,
		);
		authorizations.insert(&parent.clone().into(), &tokens, true);
		authorizations.inherit(&parent, &child);

		assert!(authorizations.authorized(&parent));
		assert!(!authorizations.authorized(&child));
		assert_eq!(authorizations.tokens(&child), tokens);
	}

	#[test]
	fn unverified_proofs_are_forwarded_without_authorizing_cached_reads() {
		let id: tg::object::Id = tg::file::Id::new(b"file").into();
		let mut authorizations = Authorizations::default();
		let tokens = tokens(
			id.clone().into(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
			now() + 60,
		);
		authorizations.insert(&id.clone().into(), &tokens, false);

		assert!(!authorizations.authorized(&id));
		assert_eq!(authorizations.tokens(&id), tokens);
	}

	#[test]
	fn sync_proofs_follow_the_object_they_were_returned_with() {
		let parent: tg::object::Id = tg::directory::Id::new(b"parent").into();
		let child: tg::object::Id = tg::file::Id::new(b"child").into();
		let unrelated: tg::object::Id = tg::file::Id::new(b"unrelated").into();
		let mut authorizations = Authorizations::default();
		let tokens = tokens(
			tg::sync::Id::new().into(),
			tg::authorization::Permission::Sync(
				tg::authorization::permission::sync::Permission::Read,
			),
			now() + 60,
		);
		authorizations.insert(&parent.clone().into(), &tokens, true);
		authorizations.inherit(&parent, &child);

		assert!(!authorizations.authorized(&child));
		assert_eq!(authorizations.tokens(&child), tokens);
		assert!(authorizations.tokens(&unrelated).is_empty());
	}

	fn tokens(
		resource: tg::Id,
		permission: tg::authorization::Permission,
		expires_at: i64,
	) -> tg::authorization::Tokens {
		let token = tg::authorization::Token {
			body: tg::authorization::Body {
				expires_at,
				permissions: vec![permission],
				resource,
			},
			secret_id: 0,
			signature: vec![0; 16],
		};
		tg::authorization::Tokens::with_authorization(vec![token])
	}
}
