use crate::prelude::*;

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
pub struct Body {
	#[tangram_serialize(id = 0)]
	pub expires_at: i64,
	#[tangram_serialize(id = 1)]
	pub permissions: Vec<tg::authorization::Permission>,
	#[tangram_serialize(id = 2)]
	pub resource: tg::Id,
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
	#[display("blake3")]
	Blake3,

	#[display("hmac_sha256")]
	HmacSha256,
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Secret {
	pub algorithm: Algorithm,
	pub bytes: [u8; 32],
	pub name: String,
}

impl Secret {
	#[must_use]
	pub fn new(name: impl Into<String>, algorithm: Algorithm, bytes: [u8; 32]) -> Self {
		Self {
			algorithm,
			bytes,
			name: name.into(),
		}
	}

	#[must_use]
	pub fn generate(name: impl Into<String>, algorithm: Algorithm) -> Self {
		Self::new(name, algorithm, rand::random())
	}

	pub fn from_slice(
		name: impl Into<String>,
		algorithm: Algorithm,
		bytes: &[u8],
	) -> tg::Result<Self> {
		let bytes = bytes.try_into().map_err(|_| tg::error!("invalid secret"))?;
		Ok(Self::new(name, algorithm, bytes))
	}

	#[must_use]
	pub fn sign(&self, input: &[u8]) -> Vec<u8> {
		match self.algorithm {
			Algorithm::Blake3 => blake3::keyed_hash(&self.bytes, input).as_bytes().to_vec(),
			Algorithm::HmacSha256 => {
				let key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, &self.bytes);
				aws_lc_rs::hmac::sign(&key, input).as_ref().to_vec()
			},
		}
	}

	/// Verify the signature in constant time.
	pub fn verify(&self, input: &[u8], signature: &[u8]) -> tg::Result<()> {
		match self.algorithm {
			Algorithm::Blake3 => {
				let signature: [u8; 32] = signature
					.try_into()
					.map_err(|_| tg::error!("invalid signature"))?;
				if blake3::keyed_hash(&self.bytes, input) != blake3::Hash::from_bytes(signature) {
					return Err(tg::error!("invalid signature"));
				}
			},
			Algorithm::HmacSha256 => {
				let key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, &self.bytes);
				aws_lc_rs::hmac::verify(&key, input, signature)
					.map_err(|_| tg::error!("invalid signature"))?;
			},
		}
		Ok(())
	}
}

impl Token {
	/// Compare the resources and permissions without considering expiration or verifying signatures.
	#[must_use]
	pub fn covers(&self, other: &Self) -> bool {
		self == other
			|| (self.body.resource == other.body.resource
				&& other
					.body
					.permissions
					.iter()
					.all(|permission| self.authorizes(&other.body.resource, *permission)))
	}

	/// Check the resource and implied permission without verifying the signature or expiration.
	#[must_use]
	pub fn authorizes(&self, resource: &tg::Id, permission: tg::authorization::Permission) -> bool {
		self.body.resource == *resource && self.body.authorizes(permission)
	}

	/// Check object containment coverage through the same permission rules as other resource kinds.
	#[must_use]
	pub fn authorizes_object_subtree(&self, resource: &tg::Id) -> bool {
		let subtree = tg::authorization::Permission::Object(
			tg::authorization::permission::object::Permission::Subtree,
		);
		self.authorizes(resource, subtree)
	}

	pub fn sign(body: Body, secret: &Secret) -> tg::Result<Self> {
		body.validate()?;
		let metadata = Metadata {
			algorithm: secret.algorithm,
			key: secret.name.clone(),
		};
		let body_string = serde_json::to_vec(&body)
			.map_err(|error| tg::error!(!error, "failed to serialize the body"))
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let metadata_string = serde_json::to_vec(&metadata)
			.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let input = format!("{VERSION}.{body_string}.{metadata_string}");
		let signature = secret.sign(input.as_bytes());
		Ok(Self {
			body,
			metadata,
			signature,
		})
	}

	pub fn verify(&self, secret: &Secret) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		self.verify_at(secret, now)
	}

	pub fn verify_at(&self, secret: &Secret, now: i64) -> tg::Result<()> {
		self.verify_signature(secret)?;
		self.body.validate_at(now)?;
		Ok(())
	}

	pub fn verify_signature(&self, secret: &Secret) -> tg::Result<()> {
		if self.metadata.algorithm != secret.algorithm {
			return Err(tg::error!("invalid algorithm"));
		}
		if self.metadata.key != secret.name {
			return Err(tg::error!("invalid key"));
		}
		let body = serde_json::to_vec(&self.body)
			.map_err(|error| tg::error!(!error, "failed to serialize the body"))
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let metadata = serde_json::to_vec(&self.metadata)
			.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let input = format!("{VERSION}.{body}.{metadata}");
		secret.verify(input.as_bytes(), &self.signature)?;
		Ok(())
	}
}

impl Body {
	pub fn validate(&self) -> tg::Result<()> {
		let Some(kind) = tg::authorization::ResourceKind::from_id_kind(self.resource.kind()) else {
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
	pub fn authorizes(&self, permission: tg::authorization::Permission) -> bool {
		self.permissions
			.iter()
			.any(|granted| granted.implies(permission))
	}
}

impl std::str::FromStr for Algorithm {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		match value {
			"blake3" => Ok(Self::Blake3),
			"hmac_sha256" => Ok(Self::HmacSha256),
			_ => Err(tg::error!("invalid algorithm")),
		}
	}
}

impl std::fmt::Debug for Secret {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		// Omit the bytes so that the secret is not written to logs.
		f.debug_struct("Secret")
			.field("algorithm", &self.algorithm)
			.field("name", &self.name)
			.finish_non_exhaustive()
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
		for algorithm in [
			tg::authorization::Algorithm::Blake3,
			tg::authorization::Algorithm::HmacSha256,
		] {
			let string = algorithm.to_string();
			assert_eq!(
				string.parse::<tg::authorization::Algorithm>().unwrap(),
				algorithm
			);
		}
	}

	#[test]
	fn body_accepts_matching_permissions_for_all_resource_kinds() {
		let cases = [
			(
				tg::id::Kind::Group,
				tg::authorization::Permission::Group(
					tg::authorization::permission::group::Permission::Read,
				),
			),
			(
				tg::id::Kind::File,
				tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Node,
				),
			),
			(
				tg::id::Kind::Organization,
				tg::authorization::Permission::Organization(
					tg::authorization::permission::organization::Permission::Read,
				),
			),
			(
				tg::id::Kind::Process,
				tg::authorization::Permission::Process(
					tg::authorization::permission::process::Permission::Node,
				),
			),
			(
				tg::id::Kind::Sandbox,
				tg::authorization::Permission::Sandbox(
					tg::authorization::permission::sandbox::Permission::Node,
				),
			),
			(
				tg::id::Kind::Sync,
				tg::authorization::Permission::Sync(
					tg::authorization::permission::sync::Permission::Read,
				),
			),
			(
				tg::id::Kind::Tag,
				tg::authorization::Permission::Tag(
					tg::authorization::permission::tag::Permission::Read,
				),
			),
			(
				tg::id::Kind::User,
				tg::authorization::Permission::User(
					tg::authorization::permission::user::Permission::Read,
				),
			),
		];
		for (kind, permission) in cases {
			let body = tg::authorization::Body {
				expires_at: 20,
				permissions: vec![permission],
				resource: tg::Id::new_uuidv7(kind),
			};

			body.validate().unwrap();
		}
	}

	#[test]
	fn authorizes_checks_resources_and_permission_implications() {
		use tg::authorization::permission::{object, process};
		let key = tg::authorization::Secret::generate("test", tg::authorization::Algorithm::Blake3);
		let object_node = tg::authorization::Permission::Object(object::Permission::Node);
		let object_subtree = tg::authorization::Permission::Object(object::Permission::Subtree);
		let process_node = tg::authorization::Permission::Process(process::Permission::Node);
		let process_output =
			tg::authorization::Permission::Process(process::Permission::NodeOutputObjects);
		let process_subtree = tg::authorization::Permission::Process(process::Permission::Subtree);
		for (kind, granted, needed, unrelated) in [
			(
				tg::id::Kind::File,
				object_subtree,
				object_node,
				process_node,
			),
			(
				tg::id::Kind::Process,
				process_subtree,
				process_node,
				process_output,
			),
		] {
			let resource = tg::Id::new_uuidv7(kind);
			let other = tg::Id::new_uuidv7(kind);
			let body = tg::authorization::Body {
				expires_at: 120,
				permissions: vec![granted],
				resource: resource.clone(),
			};
			let token = tg::authorization::Token::sign(body, &key).unwrap();
			assert!(token.authorizes(&resource, granted));
			assert!(token.authorizes(&resource, needed));
			assert!(!token.authorizes(&other, needed));
			assert!(!token.authorizes(&resource, unrelated));
			assert_eq!(
				token.authorizes_object_subtree(&resource),
				kind == tg::id::Kind::File
			);
		}
	}

	#[test]
	fn token_round_trips_and_verifies() {
		for algorithm in [
			tg::authorization::Algorithm::Blake3,
			tg::authorization::Algorithm::HmacSha256,
		] {
			let secret = tg::authorization::Secret::generate("default", algorithm);
			let body = tg::authorization::Body {
				expires_at: 20,
				permissions: vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)],
				resource: tg::Id::new_uuidv7(tg::id::Kind::File),
			};
			let token = tg::authorization::Token::sign(body.clone(), &secret).unwrap();
			let string = token.to_string();

			let parsed = string.parse::<tg::authorization::Token>().unwrap();

			assert_eq!(parsed.body, body);
			assert_eq!(parsed.metadata.algorithm, algorithm);
			assert_eq!(parsed.metadata.key, "default");
			parsed.verify_at(&secret, 15).unwrap();
		}
	}

	#[test]
	fn secret_debug_omits_the_bytes() {
		let secret = tg::authorization::Secret::new(
			"default",
			tg::authorization::Algorithm::Blake3,
			[0xab; 32],
		);
		let output = format!("{secret:?}");
		assert_eq!(
			output,
			r#"Secret { algorithm: Blake3, name: "default", .. }"#
		);
	}

	#[test]
	fn token_rejects_a_different_algorithm() {
		let secret =
			tg::authorization::Secret::generate("default", tg::authorization::Algorithm::Blake3);
		let other_secret = tg::authorization::Secret::new(
			"default",
			tg::authorization::Algorithm::HmacSha256,
			secret.bytes,
		);
		let body = tg::authorization::Body {
			expires_at: 20,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			resource: tg::Id::new_uuidv7(tg::id::Kind::File),
		};
		let token = tg::authorization::Token::sign(body, &secret).unwrap();

		let error = token.verify_at(&other_secret, 15).unwrap_err();

		assert_eq!(error.to_string(), "invalid algorithm");
	}

	#[test]
	fn token_rejects_an_invalid_signature() {
		let secret =
			tg::authorization::Secret::generate("default", tg::authorization::Algorithm::Blake3);
		let other_secret =
			tg::authorization::Secret::generate("default", tg::authorization::Algorithm::Blake3);
		let body = tg::authorization::Body {
			expires_at: 20,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			resource: tg::Id::new_uuidv7(tg::id::Kind::File),
		};
		let token = tg::authorization::Token::sign(body, &secret).unwrap();

		let error = token.verify_at(&other_secret, 15).unwrap_err();

		assert_eq!(error.to_string(), "invalid signature");
	}

	#[test]
	fn token_rejects_an_expired_body() {
		let secret =
			tg::authorization::Secret::generate("default", tg::authorization::Algorithm::Blake3);
		let body = tg::authorization::Body {
			expires_at: 20,
			permissions: vec![tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::SubtreeOutputObjects,
			)],
			resource: tg::Id::new_uuidv7(tg::id::Kind::Process),
		};
		let token = tg::authorization::Token::sign(body, &secret).unwrap();

		let error = token.verify_at(&secret, 21).unwrap_err();

		assert_eq!(error.to_string(), "expired token");
	}

	#[test]
	fn tokens_strip_regions() {
		let secret =
			tg::authorization::Secret::generate("default", tg::authorization::Algorithm::Blake3);
		let body = tg::authorization::Body {
			expires_at: 20,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			resource: tg::Id::new_uuidv7(tg::id::Kind::File),
		};
		let token = tg::authorization::Token::sign(body, &secret).unwrap();
		let mut tokens = tg::authorization::Tokens::default();
		let local = tg::Location::Local(tg::location::Local {
			region: Some("east".into()),
		});
		tokens.insert_authorization(local, token.clone());
		let remote = tg::Location::Remote(tg::location::Remote {
			name: "default".into(),
			region: Some("east".into()),
		});
		tokens.insert_authorization(remote, token.clone());

		let local = tg::Location::Local(tg::location::Local {
			region: Some("west".into()),
		});
		let remote = tg::Location::Remote(tg::location::Remote {
			name: "default".into(),
			region: Some("west".into()),
		});
		assert_eq!(tokens.authorization(&local), std::slice::from_ref(&token));
		assert_eq!(tokens.authorization(&remote), std::slice::from_ref(&token));
		assert!(
			tokens
				.iter()
				.all(|(location, _)| location.clone().without_region() == *location)
		);
	}

	#[test]
	fn inheritance_retains_complementary_permissions() {
		use tg::authorization::permission::process::Permission;
		let resource = tg::Id::new_uuidv7(tg::id::Kind::Process);
		let token = |permissions: Vec<Permission>, expires_at: i64| tg::authorization::Token {
			body: tg::authorization::Body {
				expires_at: expires_at * 60,
				permissions: permissions
					.into_iter()
					.map(tg::authorization::Permission::Process)
					.collect(),
				resource: resource.clone(),
			},
			metadata: tg::authorization::Metadata {
				algorithm: tg::authorization::Algorithm::Blake3,
				key: "default".into(),
			},
			signature: vec![0; 32],
		};
		let node = token(vec![Permission::Node], 30);
		let output = token(vec![Permission::NodeOutputObjects], 20);
		let log = token(vec![Permission::NodeLogObjects], 20);
		let mut tokens =
			tg::authorization::Tokens::with_authorization([node.clone(), output.clone()]);
		tokens.inherit(&tg::authorization::Tokens::with_authorization([
			log.clone(),
			output.clone(),
		]));
		let mut expected = [node, output, log];
		expected.sort_by_cached_key(ToString::to_string);
		assert_eq!(tokens.local_authorization(), &expected);
		let broad = token(
			vec![
				Permission::Subtree,
				Permission::SubtreeOutputObjects,
				Permission::SubtreeLogObjects,
			],
			20,
		);
		tokens.inherit(&tg::authorization::Tokens::with_authorization([
			broad.clone()
		]));
		assert_eq!(tokens.local_authorization(), &[broad]);
		let broad = token(vec![Permission::Parent], 30);
		tokens.inherit(&tg::authorization::Tokens::with_authorization([
			broad.clone()
		]));
		assert_eq!(tokens.local_authorization(), &[broad]);
	}

	#[test]
	fn inheritance_ignores_keys_and_retains_resources_and_locations() {
		let token = |resource, key: &str| tg::authorization::Token {
			body: tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)],
				resource,
			},
			metadata: tg::authorization::Metadata {
				algorithm: tg::authorization::Algorithm::Blake3,
				key: key.into(),
			},
			signature: vec![0; 32],
		};
		let first = tg::Id::new_blake3(tg::id::Kind::File, b"first");
		let second = tg::Id::new_blake3(tg::id::Kind::File, b"second");
		let a = token(first.clone(), "a");
		let b = token(first, "b");
		assert!(a.covers(&b));
		assert!(b.covers(&a));
		let file = tg::File::with_id(a.body.resource.clone().try_into().unwrap());
		file.state()
			.set_tokens(tg::authorization::Tokens::with_authorization([a.clone()]));
		let mut parent = b.clone();
		parent.body.resource = tg::Id::new_uuidv7(tg::id::Kind::Process);
		parent.body.permissions = vec![tg::authorization::Permission::Process(
			tg::authorization::permission::process::Permission::Parent,
		)];
		file.state()
			.inherit_tokens(&tg::authorization::Tokens::with_authorization([parent]));
		assert_eq!(
			file.state().tokens().local_authorization(),
			std::slice::from_ref(&a)
		);
		let c = token(second, "a");
		let mut tokens = tg::authorization::Tokens::with_authorization([a.clone()]);
		let mut incoming = tg::authorization::Tokens::with_authorization([b.clone(), c.clone()]);
		let remote = tg::Location::Remote(tg::location::Remote {
			name: "default".into(),
			region: None,
		});
		incoming.insert_authorization(remote.clone(), a.clone());
		tokens.inherit(&incoming);
		assert_eq!(tokens.local_authorization(), &[a.clone(), c]);
		assert_eq!(tokens.for_location(&remote).local_authorization(), &[a]);
	}
}
