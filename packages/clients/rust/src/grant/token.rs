use {crate::prelude::*, std::collections::BTreeMap};

const VERSION: &str = "0";

#[derive(
	Clone,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Token {
	pub body: Body,
	pub metadata: Metadata,
	pub signature: Vec<u8>,
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
pub struct Tokens(pub BTreeMap<tg::Location, Token>);

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Body {
	pub expires_at: i64,
	pub permissions: Vec<tg::grant::Permission>,
	pub resource: tg::grant::Resource,
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Metadata {
	pub algorithm: Algorithm,
	pub key: String,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Display,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Algorithm {
	#[display("ed25519")]
	Ed25519,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PrivateKey {
	pub algorithm: Algorithm,
	pub bytes: Vec<u8>,
	pub name: String,
}

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PublicKey {
	pub algorithm: Algorithm,
	pub bytes: Vec<u8>,
	pub name: String,
}

impl PrivateKey {
	#[must_use]
	pub fn new(name: impl Into<String>, algorithm: Algorithm, bytes: impl Into<Vec<u8>>) -> Self {
		Self {
			algorithm,
			bytes: bytes.into(),
			name: name.into(),
		}
	}

	pub fn generate(name: impl Into<String>, algorithm: Algorithm) -> tg::Result<Self> {
		let bytes = match algorithm {
			Algorithm::Ed25519 => {
				use aws_lc_rs::encoding::AsBigEndian as _;
				let key_pair = aws_lc_rs::signature::Ed25519KeyPair::generate()
					.map_err(|_| tg::error!("failed to generate the private key"))?;
				key_pair
					.seed()
					.and_then(|seed| seed.as_be_bytes())
					.map_err(|_| tg::error!("failed to export the private key"))?
					.as_ref()
					.to_vec()
			},
		};
		Ok(Self::new(name, algorithm, bytes))
	}
}

impl PublicKey {
	#[must_use]
	pub fn new(name: impl Into<String>, algorithm: Algorithm, bytes: impl Into<Vec<u8>>) -> Self {
		Self {
			algorithm,
			bytes: bytes.into(),
			name: name.into(),
		}
	}

	pub fn from_private_key(private_key: &PrivateKey) -> tg::Result<Self> {
		let bytes = match private_key.algorithm {
			Algorithm::Ed25519 => {
				use aws_lc_rs::signature::KeyPair as _;
				let key_pair =
					aws_lc_rs::signature::Ed25519KeyPair::from_seed_unchecked(&private_key.bytes)
						.map_err(|_| tg::error!("invalid private key"))?;
				key_pair.public_key().as_ref().to_vec()
			},
		};
		Ok(Self::new(
			private_key.name.clone(),
			private_key.algorithm,
			bytes,
		))
	}
}

impl Token {
	pub fn sign(body: Body, private_key: &PrivateKey) -> tg::Result<Self> {
		body.validate()?;
		let metadata = Metadata {
			algorithm: private_key.algorithm,
			key: private_key.name.clone(),
		};
		let body_string = serde_json::to_vec(&body)
			.map_err(|error| tg::error!(!error, "failed to serialize the body"))
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let metadata_string = serde_json::to_vec(&metadata)
			.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let input = format!("{VERSION}.{body_string}.{metadata_string}");
		let signature = match metadata.algorithm {
			Algorithm::Ed25519 => {
				let key =
					aws_lc_rs::signature::Ed25519KeyPair::from_seed_unchecked(&private_key.bytes)
						.map_err(|_| tg::error!("invalid private key"))?;
				key.sign(input.as_bytes()).as_ref().to_vec()
			},
		};
		Ok(Self {
			body,
			metadata,
			signature,
		})
	}

	pub fn verify(&self, public_key: &PublicKey) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		self.verify_at(public_key, now)
	}

	pub fn verify_at(&self, public_key: &PublicKey, now: i64) -> tg::Result<()> {
		self.verify_key(public_key)?;
		self.verify_signature(public_key)?;
		self.body.validate_at(now)?;
		Ok(())
	}

	pub fn verify_signature(&self, public_key: &PublicKey) -> tg::Result<()> {
		let body = serde_json::to_vec(&self.body)
			.map_err(|error| tg::error!(!error, "failed to serialize the body"))
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let metadata = serde_json::to_vec(&self.metadata)
			.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let input = format!("{VERSION}.{body}.{metadata}");
		match self.metadata.algorithm {
			Algorithm::Ed25519 => {
				let key = aws_lc_rs::signature::UnparsedPublicKey::new(
					&aws_lc_rs::signature::ED25519,
					&public_key.bytes,
				);
				key.verify(input.as_bytes(), &self.signature)
					.map_err(|_| tg::error!("invalid signature"))?;
			},
		}
		Ok(())
	}

	fn verify_key(&self, public_key: &PublicKey) -> tg::Result<()> {
		if self.metadata.algorithm != public_key.algorithm {
			return Err(tg::error!("invalid algorithm"));
		}
		if self.metadata.key != public_key.name {
			return Err(tg::error!("invalid key"));
		}
		Ok(())
	}
}

impl Tokens {
	#[must_use]
	pub fn with_local(token: Option<Token>) -> Self {
		let mut tokens = Self::default();
		if let Some(token) = token {
			tokens.insert_local(token);
		}
		tokens
	}

	#[must_use]
	pub fn get(&self, location: &tg::Location) -> Option<&Token> {
		self.0.get(location)
	}

	#[must_use]
	pub fn local(&self) -> Option<&Token> {
		self.get(&tg::Location::Local(tg::location::Local::default()))
	}

	#[must_use]
	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn clear(&mut self) {
		self.0.clear();
	}

	pub fn insert(&mut self, location: tg::Location, token: Token) -> Option<Token> {
		self.0.insert(location, token)
	}

	pub fn insert_local(&mut self, token: Token) -> Option<Token> {
		self.insert(tg::Location::Local(tg::location::Local::default()), token)
	}

	pub fn inherit(&mut self, parent: &Self) {
		for (location, token) in &parent.0 {
			self.0
				.entry(location.clone())
				.or_insert_with(|| token.clone());
		}
	}

	#[must_use]
	pub fn for_location(&self, location: &tg::Location) -> Self {
		Self::with_local(self.get(location).cloned())
	}

	pub fn remove_local(&mut self) -> Option<Token> {
		self.0
			.remove(&tg::Location::Local(tg::location::Local::default()))
	}
}

impl Body {
	pub fn validate(&self) -> tg::Result<()> {
		let tg::grant::Resource::Id(id) = &self.resource else {
			return Err(tg::error!("invalid resource"));
		};
		let Some(kind) = tg::grant::resource::Kind::from_id_kind(id.kind()) else {
			return Err(tg::error!("invalid resource"));
		};
		if !self
			.permissions
			.iter()
			.all(|permission| permission.kind() == kind)
		{
			return Err(tg::error!("invalid permissions"));
		}
		Ok(())
	}

	pub fn validate_at(&self, now: i64) -> tg::Result<()> {
		self.validate()?;
		if now > self.expires_at {
			return Err(tg::error!("expired token"));
		}
		Ok(())
	}

	#[must_use]
	pub fn grants(&self, permission: tg::grant::Permission) -> bool {
		self.permissions
			.iter()
			.any(|granted| granted.implies(permission))
	}
}

impl std::str::FromStr for Algorithm {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		match value {
			"ed25519" => Ok(Self::Ed25519),
			_ => Err(tg::error!("invalid algorithm")),
		}
	}
}

impl std::fmt::Display for Token {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let body = serde_json::to_vec(&self.body)
			.map_err(|_| std::fmt::Error)
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let metadata = serde_json::to_vec(&self.metadata)
			.map_err(|_| std::fmt::Error)
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let signature = data_encoding::BASE64.encode(&self.signature);
		write!(f, "{VERSION}.{body}.{metadata}.{signature}")
	}
}

impl std::str::FromStr for Token {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let mut parts = value.split('.');
		let version = parts
			.next()
			.ok_or_else(|| tg::error!("missing the version"))?;
		if version != VERSION {
			return Err(tg::error!("unsupported token version"));
		}
		let body = parts.next().ok_or_else(|| tg::error!("missing the body"))?;
		let metadata = parts
			.next()
			.ok_or_else(|| tg::error!("missing the metadata"))?;
		let signature = parts
			.next()
			.ok_or_else(|| tg::error!("missing the signature"))?;
		if parts.next().is_some() {
			return Err(tg::error!("invalid token"));
		}
		let body_bytes = data_encoding::BASE64
			.decode(body.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid body"))?;
		let body = serde_json::from_slice(&body_bytes)
			.map_err(|error| tg::error!(!error, "invalid body"))?;
		let metadata_bytes = data_encoding::BASE64
			.decode(metadata.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid metadata"))?;
		let metadata: Metadata = serde_json::from_slice(&metadata_bytes)
			.map_err(|error| tg::error!(!error, "invalid metadata"))?;
		let bytes = data_encoding::BASE64
			.decode(signature.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid signature"))?;
		let token = Self {
			body,
			metadata,
			signature: bytes,
		};
		token.body.validate()?;
		Ok(token)
	}
}

#[cfg(test)]
mod tests {
	use crate as tg;

	#[test]
	fn algorithm_round_trips() {
		assert_eq!(tg::grant::Algorithm::Ed25519.to_string(), "ed25519");
		assert_eq!(
			"ed25519".parse::<tg::grant::Algorithm>().unwrap(),
			tg::grant::Algorithm::Ed25519,
		);
	}

	#[test]
	fn body_accepts_matching_permissions_for_all_resource_kinds() {
		let cases = [
			(
				tg::id::Kind::Group,
				tg::grant::Permission::Group(tg::grant::permission::group::Permission::Read),
			),
			(
				tg::id::Kind::File,
				tg::grant::Permission::Object(tg::grant::permission::object::Permission::Node),
			),
			(
				tg::id::Kind::Organization,
				tg::grant::Permission::Organization(
					tg::grant::permission::organization::Permission::Read,
				),
			),
			(
				tg::id::Kind::Process,
				tg::grant::Permission::Process(tg::grant::permission::process::Permission::Read),
			),
			(
				tg::id::Kind::Sandbox,
				tg::grant::Permission::Sandbox(tg::grant::permission::sandbox::Permission::Read),
			),
			(
				tg::id::Kind::Tag,
				tg::grant::Permission::Tag(tg::grant::permission::tag::Permission::Read),
			),
			(
				tg::id::Kind::User,
				tg::grant::Permission::User(tg::grant::permission::user::Permission::Read),
			),
		];
		for (kind, permission) in cases {
			let body = tg::grant::Body {
				expires_at: 20,
				permissions: vec![permission],
				resource: tg::grant::Resource::Id(tg::Id::new_uuidv7(kind)),
			};

			body.validate().unwrap();
		}
	}

	#[test]
	fn token_round_trips_and_verifies() {
		let private_key =
			tg::grant::PrivateKey::generate("default", tg::grant::Algorithm::Ed25519).unwrap();
		let public_key = tg::grant::PublicKey::from_private_key(&private_key).unwrap();
		let body = tg::grant::Body {
			expires_at: 20,
			permissions: vec![tg::grant::Permission::Object(
				tg::grant::permission::object::Permission::Subtree,
			)],
			resource: tg::grant::Resource::Id(tg::Id::new_uuidv7(tg::id::Kind::File)),
		};
		let token = tg::grant::Token::sign(body.clone(), &private_key).unwrap();
		let string = token.to_string();

		let parsed = string.parse::<tg::grant::Token>().unwrap();

		assert_eq!(parsed.body, body);
		assert_eq!(parsed.metadata.algorithm, tg::grant::Algorithm::Ed25519);
		assert_eq!(parsed.metadata.key, "default");
		parsed.verify_at(&public_key, 15).unwrap();
	}

	#[test]
	fn token_rejects_an_invalid_signature() {
		let private_key =
			tg::grant::PrivateKey::generate("default", tg::grant::Algorithm::Ed25519).unwrap();
		let other_private_key =
			tg::grant::PrivateKey::generate("default", tg::grant::Algorithm::Ed25519).unwrap();
		let other_public_key = tg::grant::PublicKey::from_private_key(&other_private_key).unwrap();
		let body = tg::grant::Body {
			expires_at: 20,
			permissions: vec![tg::grant::Permission::Object(
				tg::grant::permission::object::Permission::Subtree,
			)],
			resource: tg::grant::Resource::Id(tg::Id::new_uuidv7(tg::id::Kind::File)),
		};
		let token = tg::grant::Token::sign(body, &private_key).unwrap();

		let error = token.verify_at(&other_public_key, 15).unwrap_err();

		assert_eq!(error.to_string(), "invalid signature");
	}

	#[test]
	fn token_rejects_an_expired_body() {
		let private_key =
			tg::grant::PrivateKey::generate("default", tg::grant::Algorithm::Ed25519).unwrap();
		let public_key = tg::grant::PublicKey::from_private_key(&private_key).unwrap();
		let body = tg::grant::Body {
			expires_at: 20,
			permissions: vec![tg::grant::Permission::Process(
				tg::grant::permission::process::Permission::SubtreeOutput,
			)],
			resource: tg::grant::Resource::Id(tg::Id::new_uuidv7(tg::id::Kind::Process)),
		};
		let token = tg::grant::Token::sign(body, &private_key).unwrap();

		let error = token.verify_at(&public_key, 21).unwrap_err();

		assert_eq!(error.to_string(), "expired token");
	}
}
