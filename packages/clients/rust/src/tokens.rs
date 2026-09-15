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
	#[serde(default, skip_serializing_if = "Option::is_none")]
	#[tangram_serialize(default, id = 1, skip_serializing_if = "Option::is_none")]
	pub sync: Option<tg::sync::Token>,
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
	pub fn local_sync(&self) -> Option<&tg::sync::Token> {
		self.local().and_then(|entry| entry.sync.as_ref())
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

	pub fn set_sync(&mut self, location: tg::Location, token: tg::sync::Token) {
		self.map.entry(location.without_region()).or_default().sync = Some(token);
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
		self.authorization.is_empty() && self.sync.is_none()
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
		if self.sync.is_none() {
			self.sync.clone_from(&parent.sync);
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

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
			sync: Some(sync.clone()),
		};
		let parent = Tokens::with_local(Some(entry));
		let mut child = Tokens::with_authorization(Some(authorization.clone()));
		child.inherit(&parent);
		assert_eq!(
			child.local_authorization(),
			std::slice::from_ref(&authorization)
		);
		assert_eq!(child.local().unwrap().sync.as_ref(), Some(&sync));
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
		assert_eq!(tokens.local_sync(), Some(&sync));
		assert!(!tokens.is_empty());
		tokens.inherit(&child);
		tokens.clear_authorization();
		assert!(tokens.local_authorization().is_empty());
		assert_eq!(tokens.local_sync(), Some(&sync));

		let other_sync = tg::sync::Token::sign(tg::sync::token::Body::new(i64::MAX), &key).unwrap();
		let mut other_authorization = authorization.clone();
		other_authorization.body.resource = tg::Id::new_blake3(tg::id::Kind::File, b"other");
		let entry = Entry {
			authorization: vec![other_authorization.clone()],
			sync: Some(other_sync),
		};
		let mut tokens = child;
		tokens.inherit(&Tokens::with_local_entry(entry));
		assert_eq!(
			tokens.local_authorization(),
			&[authorization, other_authorization]
		);
		assert_eq!(tokens.local_sync(), Some(&sync));
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
