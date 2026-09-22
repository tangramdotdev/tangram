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
pub struct Entry {
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 0, skip_serializing_if = "Vec::is_empty")]
	pub authorization: Vec<tg::authorization::Token>,
	#[serde(default, skip_serializing_if = "Vec::is_empty")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Vec::is_empty")]
	pub sync: Vec<tg::sync::Token>,
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

	pub fn set(&mut self, location: tg::Location, token: Entry) {
		let location = location.without_region();
		self.map.insert(location, token);
	}

	pub fn set_local(&mut self, token: Entry) {
		self.set(tg::Location::Local(tg::location::Local::default()), token);
	}

	pub fn inherit(&mut self, parent: &Self) {
		for (location, token) in parent.iter() {
			if token.is_empty() {
				continue;
			}
			self.map.entry(location.clone()).or_default().inherit(token);
		}
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
		let mut tokens = Self::default();
		for token in authorization {
			tokens.insert_local_authorization(token);
		}
		tokens
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

	#[must_use]
	pub fn local_sync(&self) -> &[tg::sync::Token] {
		self.local().map_or(&[], |entry| entry.sync.as_slice())
	}

	pub fn insert_authorization(
		&mut self,
		location: tg::Location,
		token: tg::authorization::Token,
	) {
		let tokens = &mut self
			.map
			.entry(location.without_region())
			.or_default()
			.authorization;
		if !tokens.contains(&token) {
			tokens.push(token);
		}
	}

	pub fn insert_sync(&mut self, location: tg::Location, token: tg::sync::Token) {
		let tokens = &mut self.map.entry(location.without_region()).or_default().sync;
		if !tokens.contains(&token) {
			tokens.push(token);
		}
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
		self.authorization.is_empty() && self.sync.is_empty()
	}

	pub fn inherit(&mut self, parent: &Self) {
		for token in &parent.authorization {
			if self
				.authorization
				.iter()
				.any(|existing| existing.covers(token))
			{
				continue;
			}
			self.authorization
				.retain(|existing| !token.covers(existing));
			self.authorization.push(token.clone());
		}
		for token in &parent.sync {
			if !self.sync.contains(token) {
				self.sync.push(token.clone());
			}
		}
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
			let body = tg::sync::token::Body::new(i64::MAX);
			let sync = tg::sync::Token::sign(body, &key).unwrap();
			let body = tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)],
				resource: id,
			};
			let authorization = tg::authorization::Token::sign(body, &key).unwrap();
			let entry = Entry {
				authorization: vec![authorization],
				sync: vec![sync],
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
		assert_eq!(entry.sync.len(), 2);
		assert_eq!(entry.authorization.len(), 2);
		for token in expected.local_sync() {
			assert!(entry.sync.contains(token));
		}
		for token in expected.local_authorization() {
			assert!(entry.authorization.contains(token));
		}
		assert!(wrapper.state().tokens().is_empty());
		assert!(inputs.state().tokens().is_empty());
		assert_eq!(first.state().tokens().local_sync().len(), 1);
		assert_eq!(second.state().tokens().local_sync().len(), 1);
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
		let sync = tg::sync::Token::sign(tg::sync::token::Body::new(i64::MAX), &key).unwrap();
		let body = tg::authorization::Body {
			expires_at: i64::MAX,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Node,
			)],
			resource: id.clone().into(),
		};
		let authorization = tg::authorization::Token::sign(body, &key).unwrap();
		let entry = Entry {
			authorization: Vec::new(),
			sync: vec![sync.clone()],
		};
		let parent = Tokens::with_local(Some(entry));
		let mut child = Tokens::with_authorization(Some(authorization.clone()));
		child.inherit(&parent);
		assert_eq!(
			child.local_authorization(),
			std::slice::from_ref(&authorization)
		);
		assert_eq!(
			child.local().unwrap().sync.as_slice(),
			std::slice::from_ref(&sync)
		);
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
		assert_eq!(sync.to_string().parse::<tg::sync::Token>().unwrap(), sync);
		let mut tokens = child.clone();
		let removed = tokens.remove_local_authorization();
		assert_eq!(removed, vec![authorization.clone()]);
		assert_eq!(tokens.local_sync(), std::slice::from_ref(&sync));
		assert!(!tokens.is_empty());
		tokens.inherit(&child);
		tokens.clear_authorization();
		assert!(tokens.local_authorization().is_empty());
		assert_eq!(tokens.local_sync(), std::slice::from_ref(&sync));

		let other_sync = tg::sync::Token::sign(tg::sync::token::Body::new(i64::MAX), &key).unwrap();
		let mut other_authorization = authorization.clone();
		other_authorization.body.resource = tg::Id::new_blake3(tg::id::Kind::File, b"other");
		let entry = Entry {
			authorization: vec![other_authorization.clone()],
			sync: vec![other_sync.clone()],
		};
		let mut tokens = child;
		tokens.inherit(&Tokens::with_local_entry(entry));
		assert_eq!(
			tokens.local_authorization(),
			&[authorization, other_authorization]
		);
		assert_eq!(tokens.local_sync(), &[sync, other_sync]);
		let json = serde_json::to_value(&tokens).unwrap();
		assert_eq!(json["local"]["authorization"].as_array().unwrap().len(), 2);
		assert_eq!(serde_json::from_value::<Tokens>(json).unwrap(), tokens);
		let bytes = tangram_serialize::to_vec(&tokens).unwrap();
		assert_eq!(
			tangram_serialize::from_slice::<Tokens>(&bytes).unwrap(),
			tokens
		);
	}
}
