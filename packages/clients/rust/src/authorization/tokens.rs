use {crate::prelude::*, std::collections::BTreeMap};

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(transparent)]
#[tangram_serialize(transparent)]
pub struct Tokens {
	map: BTreeMap<tg::Location, Entry>,
}

#[derive(
	Clone,
	Debug,
	Default,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(transparent)]
#[tangram_serialize(transparent)]
pub struct Entry {
	pub authorization: Vec<tg::authorization::Token>,
}

impl Tokens {
	#[must_use]
	pub fn with_local(token: Option<Entry>) -> Self {
		let mut tokens = Self::default();
		if let Some(token) = token {
			tokens.set_local(token);
		}
		tokens
	}

	#[must_use]
	pub fn with_local_entry(entry: Entry) -> Self {
		Self::with_local((!entry.is_empty()).then_some(entry))
	}

	#[must_use]
	pub fn local_entry(&self) -> Entry {
		self.local().cloned().unwrap_or_default()
	}

	#[must_use]
	pub fn remote_entry(&self) -> Entry {
		self.map
			.iter()
			.find(|(location, _)| location.is_remote())
			.map(|(_, entry)| entry.clone())
			.unwrap_or_default()
	}

	#[must_use]
	pub fn get(&self, location: &tg::Location) -> Option<&Entry> {
		let location = location.clone().without_region();
		self.map.get(&location)
	}

	pub fn iter(&self) -> impl Iterator<Item = (&tg::Location, &Entry)> {
		self.map.iter()
	}

	#[must_use]
	pub fn local(&self) -> Option<&Entry> {
		self.get(&tg::Location::Local(tg::location::Local::default()))
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.map.values().all(Entry::is_empty)
	}

	pub fn clear(&mut self) {
		self.map.clear();
	}

	pub fn clear_authorization(&mut self) {
		for entry in self.map.values_mut() {
			entry.authorization.clear();
		}
		self.map.retain(|_, entry| !entry.is_empty());
	}

	pub fn set(&mut self, location: tg::Location, mut token: Entry) {
		token.normalize(None);
		let location = location.without_region();
		self.map.insert(location, token);
	}

	pub fn set_local(&mut self, token: Entry) {
		self.set(tg::Location::Local(tg::location::Local::default()), token);
	}

	pub fn inherit(&mut self, parent: &Self) {
		self.inherit_with_resource(parent, None);
	}

	pub fn inherit_with_resource(&mut self, parent: &Self, resource: Option<&tg::Id>) {
		for (location, token) in parent.iter() {
			if token.is_empty() {
				continue;
			}
			let entry = self.map.entry(location.clone()).or_default();
			entry
				.authorization
				.extend(token.authorization.iter().cloned());
		}
		self.normalize(resource);
	}

	/// Normalize each location independently, optionally pruning proofs redundant for the receiving object.
	pub fn normalize(&mut self, resource: Option<&tg::Id>) {
		for entry in self.map.values_mut() {
			entry.normalize(resource);
		}
		self.map.retain(|_, entry| !entry.is_empty());
	}

	#[must_use]
	pub fn for_location(&self, location: &tg::Location) -> Self {
		Self::with_local(self.get(location).cloned())
	}

	pub fn remove_local(&mut self) -> Option<Entry> {
		self.map
			.remove(&tg::Location::Local(tg::location::Local::default()))
	}

	#[must_use]
	pub fn with_authorization(
		authorization: impl IntoIterator<Item = tg::authorization::Token>,
	) -> Self {
		let entry = Entry {
			authorization: authorization.into_iter().collect(),
		};
		Self::with_local_entry(entry)
	}

	#[must_use]
	pub fn authorization(&self, location: &tg::Location) -> &[tg::authorization::Token] {
		self.get(location)
			.map_or(&[], |entry| entry.authorization.as_slice())
	}

	#[must_use]
	pub fn local_authorization(&self) -> &[tg::authorization::Token] {
		self.authorization(&tg::Location::Local(tg::location::Local::default()))
	}

	pub fn insert_authorization(
		&mut self,
		location: tg::Location,
		token: tg::authorization::Token,
	) {
		let entry = self.map.entry(location.without_region()).or_default();
		entry.authorization.push(token);
		entry.normalize(None);
	}

	pub fn insert_local_authorization(&mut self, token: tg::authorization::Token) {
		self.insert_authorization(tg::Location::Local(tg::location::Local::default()), token);
	}

	pub fn remove_local_authorization(&mut self) -> Vec<tg::authorization::Token> {
		let location = tg::Location::Local(tg::location::Local::default());
		let Some(entry) = self.map.get_mut(&location) else {
			return Vec::new();
		};
		let tokens = std::mem::take(&mut entry.authorization);
		if entry.is_empty() {
			self.map.remove(&location);
		}
		tokens
	}
}

impl Entry {
	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.authorization.is_empty()
	}

	pub fn inherit(&mut self, parent: &Self) {
		self.authorization
			.extend(parent.authorization.iter().cloned());
		self.normalize(None);
	}

	pub fn normalize(&mut self, resource: Option<&tg::Id>) {
		// Compare proofs only within the same resource, using the encoded token to break ties.
		let mut authorization = std::mem::take(&mut self.authorization);
		authorization.sort_by_cached_key(ToString::to_string);
		let mut resources = BTreeMap::<tg::Id, Vec<tg::authorization::Token>>::new();
		for token in authorization {
			let proofs = resources.entry(token.body.resource.clone()).or_default();
			if proofs.iter().any(|existing| existing.covers(&token)) {
				continue;
			}
			proofs.retain(|existing| !token.covers(existing));
			proofs.push(token);
		}
		self.authorization = resources.into_values().flatten().collect();

		// Keep sync tokens so readers can wait for objects that are still being transferred.
		if let Some(resource) = resource
			&& self
				.authorization
				.iter()
				.any(|token| token.grants_object_subtree(resource))
		{
			self.authorization.retain(|token| {
				token.body.resource.kind() == tg::id::Kind::Sync
					|| token.grants_object_subtree(resource)
			});
		}
		self.authorization.sort_by_cached_key(ToString::to_string);
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn collects_loaded_descendant_tokens_without_mutating_handles() {
		let key =
			tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
				.unwrap();
		let first = tg::Directory::with_id(tg::directory::Id::new(b"first"));
		let second = tg::Directory::with_id(tg::directory::Id::new(b"second"));
		let mut expected = Tokens::default();
		for child in [&first, &second] {
			let id: tg::Id = child.id().into();
			let body = tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![tg::authorization::Permission::Sync(
					tg::authorization::permission::sync::Permission::Read,
				)],
				resource: tg::sync::Id::new().into(),
			};
			let sync = tg::authorization::Token::sign(body, &key).unwrap();
			let body = tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Node,
				)],
				resource: id,
			};
			let authorization = tg::authorization::Token::sign(body, &key).unwrap();
			let entry = Entry {
				authorization: vec![authorization, sync],
			};
			let tokens = Tokens::with_local_entry(entry);
			expected.inherit(&tokens);
			child.state().set_tokens(tokens);
		}
		let inputs = tg::Directory::with_entries(std::collections::BTreeMap::from([
			("first".into(), first.clone().into()),
			("second".into(), second.clone().into()),
			("shared".into(), first.clone().into()),
		]));
		let wrapper = tg::Directory::with_entries(std::collections::BTreeMap::from([(
			"inputs".into(),
			inputs.clone().into(),
		)]));
		let tokens = wrapper.to_referent().options.tokens;
		let entry = tokens.local().unwrap();
		assert_eq!(entry.authorization.len(), 4);
		for token in expected.local_authorization() {
			assert!(entry.authorization.contains(token));
		}
		assert!(wrapper.state().tokens().is_empty());
		assert!(inputs.state().tokens().is_empty());
		assert_eq!(first.state().tokens().local_authorization().len(), 2);
		assert_eq!(second.state().tokens().local_authorization().len(), 2);
	}

	#[test]
	fn prunes_descendant_tokens_only_for_the_same_location() {
		let key =
			tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
				.unwrap();
		let local = tg::Location::Local(tg::location::Local::default());
		let remote = tg::Location::Remote(tg::location::Remote {
			name: "default".into(),
			region: None,
		});
		let authorization = |resource| {
			let body = tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)],
				resource,
			};
			tg::authorization::Token::sign(body, &key).unwrap()
		};
		for (covered, uncovered) in [(local.clone(), remote.clone()), (remote, local)] {
			let child = tg::Directory::with_id(tg::directory::Id::new(b"child"));
			let parent = tg::Directory::with_entries(BTreeMap::from([(
				"child".into(),
				child.clone().into(),
			)]));
			let wrapper = tg::Directory::with_entries(BTreeMap::from([(
				"parent".into(),
				parent.clone().into(),
			)]));
			let child_authorization = authorization(child.id().into());
			let parent_authorization = authorization(parent.id().into());
			let mut child_tokens = Tokens::default();
			for location in [&covered, &uncovered] {
				child_tokens.insert_authorization(location.clone(), child_authorization.clone());
			}
			child.state().set_tokens(child_tokens.clone());
			let mut parent_tokens = Tokens::default();
			parent_tokens.insert_authorization(covered.clone(), parent_authorization.clone());
			parent.state().set_tokens(parent_tokens.clone());

			let tokens = wrapper.to_referent().options.tokens;
			assert_eq!(
				tokens.authorization(&covered),
				std::slice::from_ref(&parent_authorization)
			);
			assert_eq!(
				tokens.authorization(&uncovered),
				std::slice::from_ref(&child_authorization)
			);
			assert_eq!(child.state().tokens(), child_tokens);
			assert_eq!(parent.state().tokens(), parent_tokens);
			assert!(wrapper.state().tokens().is_empty());

			// Preserve a shared descendant reached through an uncovered sibling path.
			let shared = tg::Directory::with_entries(BTreeMap::from([
				("child".into(), child.into()),
				("parent".into(), parent.into()),
			]));
			let tokens = shared.to_referent().options.tokens;
			assert!(
				tokens
					.authorization(&covered)
					.contains(&child_authorization)
			);
			assert!(
				tokens
					.authorization(&covered)
					.contains(&parent_authorization)
			);
		}
	}

	#[test]
	fn inherits_each_kind_and_rebases_locations() {
		let id = tg::object::Id::new(tg::object::Kind::File, &bytes::Bytes::new());
		let key =
			tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
				.unwrap();
		let body = tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: vec![tg::authorization::Permission::Sync(
				tg::authorization::permission::sync::Permission::Read,
			)],
			resource: tg::sync::Id::new().into(),
		};
		let sync = tg::authorization::Token::sign(body, &key).unwrap();
		let body = tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			)],
			resource: id.clone().into(),
		};
		let authorization = tg::authorization::Token::sign(body, &key).unwrap();
		let entry = Entry {
			authorization: vec![sync.clone()],
		};
		let parent = Tokens::with_local(Some(entry));
		let mut child = Tokens::with_authorization(Some(authorization.clone()));
		child.inherit(&parent);
		assert_eq!(child.local_authorization().len(), 2);
		assert!(child.local_authorization().contains(&authorization));
		assert!(child.local_authorization().contains(&sync));
		let remote = tg::Location::Remote(tg::location::Remote {
			name: "cloud".into(),
			region: Some("west".into()),
		});
		let mut tokens = Tokens::default();
		tokens.set(remote.clone(), child.local().unwrap().clone());
		assert_eq!(tokens.for_location(&remote), child);
		let json = serde_json::to_value(&tokens).unwrap();
		assert_eq!(serde_json::from_value::<Tokens>(json).unwrap(), tokens);
		let bytes = tangram_serialize::to_vec(&tokens).unwrap();
		assert_eq!(
			tangram_serialize::from_slice::<Tokens>(&bytes).unwrap(),
			tokens
		);
		let reference = tg::Referent::with_node_and_tokens(tg::Id::from(id), tokens);
		assert_eq!(
			reference
				.to_string()
				.parse::<tg::Referent<tg::Id>>()
				.unwrap(),
			reference
		);
		assert_eq!(
			sync.to_string()
				.parse::<tg::authorization::Token>()
				.unwrap(),
			sync
		);
		let mut tokens = child.clone();
		let removed = tokens.remove_local_authorization();
		assert_eq!(removed, child.local_authorization());
		assert!(tokens.is_empty());
		tokens.inherit(&child);
		tokens.clear_authorization();
		assert!(tokens.local_authorization().is_empty());

		let body = tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: vec![tg::authorization::Permission::Sync(
				tg::authorization::permission::sync::Permission::Read,
			)],
			resource: tg::sync::Id::new().into(),
		};
		let other_sync = tg::authorization::Token::sign(body, &key).unwrap();
		let mut other_authorization = authorization.clone();
		other_authorization.body.resource = tg::Id::new_blake3(tg::id::Kind::File, b"other");
		let entry = Entry {
			authorization: vec![other_authorization.clone(), other_sync.clone()],
		};
		let mut tokens = child;
		tokens.inherit(&Tokens::with_local_entry(entry));
		let mut expected = [authorization, other_authorization, sync, other_sync];
		expected.sort_by_cached_key(ToString::to_string);
		assert_eq!(tokens.local_authorization(), &expected);
		let json = serde_json::to_value(&tokens).unwrap();
		assert_eq!(json["local"].as_array().unwrap().len(), 4);
		assert_eq!(serde_json::from_value::<Tokens>(json).unwrap(), tokens);
		let bytes = tangram_serialize::to_vec(&tokens).unwrap();
		assert_eq!(
			tangram_serialize::from_slice::<Tokens>(&bytes).unwrap(),
			tokens
		);
	}

	#[test]
	fn object_normalization_is_independent_of_arrival_order() {
		let id = tg::file::Id::new(b"file");
		let direct = proof(id.clone().into(), 20);
		let inherited = proof(tg::directory::Id::new(b"parent").into(), 20);
		for proofs in [
			[direct.clone(), inherited.clone()],
			[inherited.clone(), direct.clone()],
		] {
			let file = tg::File::with_id(id.clone());
			for proof in &proofs {
				file.state()
					.inherit_tokens(&Tokens::with_authorization([proof.clone()]));
			}
			assert_eq!(
				file.state().tokens().local_authorization(),
				std::slice::from_ref(&direct)
			);
			file.state().set_tokens(Tokens::with_authorization(proofs));
			assert_eq!(
				file.state().tokens().local_authorization(),
				std::slice::from_ref(&direct)
			);
		}
		let file = tg::File::with_id(id.clone());
		file.state()
			.set_tokens(Tokens::with_authorization([inherited]));
		let referent = tg::Referent::with_node_and_local_tokens(id.into(), vec![direct.clone()]);
		file.state().finish_store(referent).unwrap();
		assert_eq!(file.state().tokens().local_authorization(), &[direct]);
	}

	#[test]
	fn collection_preserves_sync_proofs_covered_by_subtree() {
		let child = tg::Directory::with_id(tg::directory::Id::new(b"child"));
		let parent =
			tg::Directory::with_entries(BTreeMap::from([("child".into(), child.clone().into())]));
		let parent_proof = proof(parent.id().into(), 120);
		parent
			.state()
			.set_tokens(Tokens::with_authorization([parent_proof.clone()]));
		let key =
			tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
				.unwrap();
		let body = tg::authorization::Body {
			expires_at: 30,
			permissions: vec![tg::authorization::Permission::Sync(
				tg::authorization::permission::sync::Permission::Read,
			)],
			resource: tg::sync::Id::new().into(),
		};
		let sync = tg::authorization::Token::sign(body, &key).unwrap();
		for expiration in [60, 120, 121, 179, 180, 181, 240] {
			let child_proof = proof(child.id().into(), expiration);
			let mut child_tokens = Tokens::with_authorization([child_proof.clone()]);
			child_tokens.insert_authorization(
				tg::Location::Local(tg::location::Local::default()),
				sync.clone(),
			);
			child.state().set_tokens(child_tokens.clone());
			let collected = parent.to_referent().options.tokens;
			assert!(collected.local_authorization().contains(&parent_proof));
			assert!(!collected.local_authorization().contains(&child_proof));
			assert!(collected.local_authorization().contains(&sync));
			assert_eq!(collected.local_authorization().len(), 2);
			assert_eq!(child.state().tokens(), child_tokens);
		}
	}

	#[test]
	fn collection_prunes_descendants_regardless_of_expiration() {
		let leaf = tg::Directory::with_id(tg::directory::Id::new(b"leaf"));
		let middle =
			tg::Directory::with_entries(BTreeMap::from([("leaf".into(), leaf.clone().into())]));
		let root =
			tg::Directory::with_entries(BTreeMap::from([("middle".into(), middle.clone().into())]));
		let root_proof = proof(root.id().into(), 120);
		let middle_proof = proof(middle.id().into(), 179);
		let leaf_proof = proof(leaf.id().into(), 238);
		for (object, token) in [
			(&root, &root_proof),
			(&middle, &middle_proof),
			(&leaf, &leaf_proof),
		] {
			object
				.state()
				.set_tokens(Tokens::with_authorization([token.clone()]));
		}
		let tokens = root.to_referent().options.tokens;
		assert_eq!(tokens.local_authorization(), &[root_proof]);
	}

	#[test]
	fn pruning_preserves_merge_laws() {
		let resource: tg::Id = tg::file::Id::new(b"file").into();
		let other: tg::Id = tg::directory::Id::new(b"other").into();
		let mut inputs = Vec::new();
		for id in [&resource, &other] {
			for expiration in [120, 179, 180, 181] {
				for permission in [
					tg::authorization::permission::object::Permission::Node,
					tg::authorization::permission::object::Permission::Subtree,
				] {
					let mut token = proof(id.clone(), expiration);
					token.body.permissions =
						vec![tg::authorization::Permission::Object(permission)];
					inputs.push(Tokens::with_authorization([token]));
				}
			}
		}
		let merge = |a: &Tokens, b: &Tokens, context: Option<&tg::Id>| {
			let mut output = a.clone();
			output.inherit_with_resource(b, context);
			output
		};
		let proofs = |tokens: &Tokens| {
			tokens
				.local_authorization()
				.iter()
				.map(ToString::to_string)
				.collect::<std::collections::BTreeSet<_>>()
		};
		for context in [None, Some(&resource)] {
			for a in &inputs {
				assert_eq!(proofs(&merge(a, a, context)), proofs(a));
				for b in &inputs {
					let ab = merge(a, b, context);
					assert_eq!(proofs(&ab), proofs(&merge(b, a, context)));
					assert_eq!(proofs(&merge(&ab, &ab, context)), proofs(&ab));
					for c in &inputs {
						assert_eq!(
							proofs(&merge(&ab, c, context)),
							proofs(&merge(a, &merge(b, c, context), context))
						);
					}
				}
			}
		}
	}

	#[test]
	fn normalization_ignores_expiration() {
		use tg::authorization::permission::process::Permission;
		let resource = tg::Id::new_uuidv7(tg::id::Kind::Process);
		let mut tokens = Vec::new();
		for (expiration, permission) in [
			(120, Permission::Parent),
			(179, Permission::Subtree),
			(238, Permission::Node),
		] {
			let mut token = proof(resource.clone(), expiration);
			token.body.permissions = vec![tg::authorization::Permission::Process(permission)];
			tokens.push(token);
		}
		for order in [
			[0, 1, 2],
			[0, 2, 1],
			[1, 0, 2],
			[1, 2, 0],
			[2, 0, 1],
			[2, 1, 0],
		] {
			let authorization = order.map(|index| tokens[index].clone());
			let normalized = Tokens::with_authorization(authorization.clone());
			assert_eq!(
				normalized.local_authorization(),
				std::slice::from_ref(&tokens[0])
			);
			let mut entry = Entry {
				authorization: authorization.to_vec(),
			};
			entry.normalize(None);
			assert_eq!(entry.authorization, normalized.local_authorization());
			let mut sequential = Tokens::default();
			for index in order {
				sequential.inherit(&Tokens::with_authorization([tokens[index].clone()]));
			}
			assert_eq!(sequential, normalized);
		}
	}

	#[test]
	fn equivalent_proofs_use_the_encoded_token_to_break_ties() {
		let resource = tg::file::Id::new(b"file").into();
		let earlier = proof(resource, 120);
		let mut later = earlier.clone();
		later.body.expires_at = 121;
		let expected = [earlier.clone(), later.clone()]
			.into_iter()
			.min_by_key(ToString::to_string)
			.unwrap();
		for authorization in [[earlier.clone(), later.clone()], [later.clone(), earlier]] {
			let tokens = Tokens::with_authorization(authorization);
			assert_eq!(
				tokens.local_authorization(),
				std::slice::from_ref(&expected)
			);
		}
	}

	fn proof(resource: tg::Id, expires_at: i64) -> tg::authorization::Token {
		tg::authorization::Token {
			body: tg::authorization::Body {
				expires_at,
				permissions: vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)],
				resource,
			},
			metadata: tg::authorization::Metadata {
				algorithm: tg::authorization::Algorithm::Ed25519,
				key: "test".into(),
			},
			signature: vec![0; 64],
		}
	}
}
