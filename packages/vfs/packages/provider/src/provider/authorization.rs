use {
	super::now,
	std::{
		collections::{BTreeMap, BTreeSet},
		sync::{
			Arc, Mutex,
			atomic::{AtomicBool, AtomicI64, Ordering},
		},
	},
	tangram_client::prelude::*,
};

#[derive(Default)]
pub(super) struct Authorizations {
	nodes: BTreeMap<tg::object::Id, i64>,
	pruned: usize,
	subtrees: BTreeMap<tg::object::Id, i64>,
	tokens: BTreeMap<tg::Id, tg::authorization::Tokens>,
}

/// An authorization path retained by the nodes and handles reached through it.
pub(super) struct Authority {
	expires_at: AtomicI64,
	has_previous: AtomicBool,
	id: tg::object::Id,
	parent: Mutex<Option<Arc<Authority>>>,
	previous: Mutex<Vec<Arc<Authority>>>,
	proven: AtomicBool,
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

	pub(super) fn tokens(
		&self,
		authority: &Arc<Authority>,
		id: &tg::object::Id,
	) -> tg::authorization::Tokens {
		let mut tokens = tg::authorization::Tokens::default();
		for id in Self::path_ids(authority, id) {
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

	pub(super) fn authorized(&self, authority: &Arc<Authority>, id: &tg::object::Id) -> bool {
		let now = now();
		if self
			.subtrees
			.get(id)
			.is_some_and(|expiration| *expiration >= now)
		{
			authority.release(id);
			return true;
		}
		if authority.id == *id {
			authority.proven.store(false, Ordering::Relaxed);
		}
		if self
			.nodes
			.get(id)
			.is_some_and(|expiration| *expiration >= now)
		{
			return true;
		}
		let expiration = Self::path_ids(authority, id)
			.iter()
			.filter_map(|id| self.subtrees.get(id))
			.copied()
			.max()
			.unwrap_or(i64::MIN);
		if expiration >= now {
			authority.expires_at.store(expiration, Ordering::Relaxed);
			authority.clear_previous(id);
			return true;
		}
		false
	}

	fn path_ids(authority: &Arc<Authority>, id: &tg::object::Id) -> BTreeSet<tg::object::Id> {
		let mut ids = BTreeSet::from([id.clone()]);
		let mut visited = BTreeSet::new();
		let mut stack = vec![authority.clone()];
		while let Some(authority) = stack.pop() {
			if visited.insert(Arc::as_ptr(&authority)) {
				ids.insert(authority.id.clone());
				stack.extend(authority.parents());
			}
		}
		ids
	}

	/// Prune expired proofs once the retained state has doubled since the last scan.
	fn prune(&mut self, now: i64) {
		let count = self.nodes.len() + self.subtrees.len() + self.tokens.len();
		if count < self.pruned.saturating_mul(2).max(1024) {
			return;
		}
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
		self.pruned = self.nodes.len() + self.subtrees.len() + self.tokens.len();
	}
}

impl Authority {
	#[must_use]
	pub(super) fn new(id: tg::object::Id, parent: Option<&Arc<Self>>) -> Arc<Self> {
		Arc::new(Self {
			expires_at: AtomicI64::new(i64::MIN),
			has_previous: AtomicBool::new(false),
			id,
			parent: Mutex::new(parent.cloned()),
			previous: Mutex::new(Vec::new()),
			proven: AtomicBool::new(false),
		})
	}

	fn parent(&self) -> Option<Arc<Self>> {
		self.parent.lock().unwrap().clone()
	}

	/// Release the paths that reached this authority's object once its subtree is proven, because the object's own tokens authorize the objects reached through it.
	fn release(&self, id: &tg::object::Id) {
		if self.id != *id
			|| self.proven.load(Ordering::Relaxed)
			|| self.proven.swap(true, Ordering::Relaxed)
		{
			return;
		}
		self.parent.lock().unwrap().take();
		self.clear_previous(id);
	}

	/// Release the other paths that reached this authority's object once it is authorized, keeping the current path.
	fn clear_previous(&self, id: &tg::object::Id) {
		if self.id != *id || !self.has_previous.load(Ordering::Relaxed) {
			return;
		}
		let mut previous = self.previous.lock().unwrap();
		previous.clear();
		self.has_previous.store(false, Ordering::Relaxed);
	}

	/// Determine whether this authority is known to have been authorized, through its own proof or its current path.
	fn authorized(&self) -> bool {
		self.proven.load(Ordering::Relaxed) || self.expires_at.load(Ordering::Relaxed) != i64::MIN
	}

	/// Get the parents of every path that reached this authority.
	fn parents(&self) -> Vec<Arc<Self>> {
		let mut parents = self.previous.lock().unwrap().clone();
		parents.extend(self.parent());
		parents
	}

	/// Authorize this authority through the path that reached another authority for the same object, unless this authority is proven or that path passes through it.
	pub(super) fn adopt(&self, incoming: &Self) {
		if std::ptr::eq(self, incoming) || self.proven.load(Ordering::Relaxed) {
			return;
		}
		let Some(parent) = incoming.parent() else {
			return;
		};
		let mut link = Some(parent.clone());
		while let Some(current) = link {
			if std::ptr::eq(current.as_ref(), self) {
				return;
			}
			link = current.parent();
		}

		// Replace the path, and retain the replaced path so that its tokens still apply unless the new path is known to be authorized.
		if let Some(replaced) = self.parent.lock().unwrap().replace(parent.clone())
			&& !Arc::ptr_eq(&replaced, &parent)
			&& !parent.authorized()
		{
			let mut previous = self.previous.lock().unwrap();
			if !previous
				.iter()
				.any(|existing| Arc::ptr_eq(existing, &replaced))
			{
				previous.push(replaced);
				self.has_previous.store(true, Ordering::Relaxed);
			}
		}
		self.expires_at.store(i64::MIN, Ordering::Relaxed);
	}
}

#[cfg(test)]
mod tests {
	use {
		super::{Authority, Authorizations, now},
		tangram_client::prelude::*,
	};

	#[test]
	fn renewed_parent_proofs_reach_existing_descendants_and_cycles() {
		let parent: tg::object::Id = tg::directory::Id::new(b"parent").into();
		let child: tg::object::Id = tg::file::Id::new(b"child").into();
		let mut authorizations = Authorizations::default();
		let parent_authority = Authority::new(parent.clone(), None);
		let child_authority = Authority::new(child.clone(), Some(&parent_authority));
		let cycle = Authority::new(parent.clone(), Some(&child_authority));
		parent_authority.adopt(&cycle);
		let expired = tokens(
			parent.clone().into(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
			now() - 1,
		);
		authorizations.insert(&parent.clone().into(), &expired, true);
		assert!(!authorizations.authorized(&child_authority, &child));

		let fresh = tokens(
			parent.clone().into(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
			now() + 60,
		);
		authorizations.insert(&parent.clone().into(), &fresh, true);
		assert!(authorizations.authorized(&child_authority, &child));
		assert_eq!(authorizations.tokens(&child_authority, &child), fresh);
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
		let parent_authority = Authority::new(parent.clone(), None);
		let child_authority = Authority::new(child.clone(), Some(&parent_authority));

		assert!(authorizations.authorized(&parent_authority, &parent));
		assert!(!authorizations.authorized(&child_authority, &child));
		assert_eq!(authorizations.tokens(&child_authority, &child), tokens);
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

		assert!(!authorizations.authorized(&Authority::new(id.clone(), None), &id));
		assert_eq!(
			authorizations.tokens(&Authority::new(id.clone(), None), &id),
			tokens
		);
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
		let parent_authority = Authority::new(parent.clone(), None);
		let child_authority = Authority::new(child.clone(), Some(&parent_authority));

		assert!(!authorizations.authorized(&child_authority, &child));
		assert_eq!(authorizations.tokens(&child_authority, &child), tokens);
		assert!(
			authorizations
				.tokens(&Authority::new(unrelated.clone(), None), &unrelated)
				.is_empty()
		);
	}

	#[test]
	fn exact_subtree_proofs_release_parent_paths() {
		let parent = Authority::new(tg::directory::Id::new(b"parent").into(), None);
		let weak = std::sync::Arc::downgrade(&parent);
		let id: tg::object::Id = tg::file::Id::new(b"child").into();
		let child = Authority::new(id.clone(), Some(&parent));
		let mut authorizations = Authorizations::default();
		let proofs = tokens(
			id.clone().into(),
			tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			),
			now() + 60,
		);
		authorizations.insert(&id.clone().into(), &proofs, true);
		drop(parent);
		assert!(weak.upgrade().is_some());
		assert!(authorizations.authorized(&child, &id));
		assert!(weak.upgrade().is_none());
		assert_eq!(authorizations.tokens(&child, &id), proofs);
	}

	#[test]
	fn pruning_is_amortized_and_removes_expired_proofs() {
		let mut authorizations = Authorizations::default();
		let permission = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		let expires_at = now() + 60;
		for index in 0..1024_u64 {
			let id: tg::Id = tg::file::Id::new(&index.to_le_bytes()).into();
			authorizations.insert(&id, &tokens(id.clone(), permission, expires_at), true);
			if index < 511 {
				assert_eq!(authorizations.pruned, 0);
			} else if index < 1023 {
				assert_eq!(authorizations.pruned, 1024);
			}
		}
		assert_eq!(authorizations.pruned, 2048);
		authorizations.pruned = 0;
		authorizations.prune(expires_at + 1);
		assert!(authorizations.tokens.is_empty());
		assert!(authorizations.subtrees.is_empty());
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
