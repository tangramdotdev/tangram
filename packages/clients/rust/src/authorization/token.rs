use {
	crate::prelude::*,
	tangram_util::varint::{ReadExt as _, WriteExt as _},
};

const VERSION: u8 = 0;

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
	pub secret_id: u64,
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
	pub id: u64,
}

impl Secret {
	#[must_use]
	pub fn new(id: u64, algorithm: Algorithm, bytes: [u8; 32]) -> Self {
		Self {
			algorithm,
			bytes,
			id,
		}
	}

	#[must_use]
	pub fn generate(id: u64, algorithm: Algorithm) -> Self {
		Self::new(id, algorithm, rand::random())
	}

	pub fn from_slice(id: u64, algorithm: Algorithm, bytes: &[u8]) -> tg::Result<Self> {
		let bytes = bytes.try_into().map_err(|_| tg::error!("invalid secret"))?;
		Ok(Self::new(id, algorithm, bytes))
	}

	/// Compute the MAC of the input, truncated to 16 bytes.
	#[must_use]
	pub fn sign(&self, input: &[u8]) -> Vec<u8> {
		match self.algorithm {
			Algorithm::Blake3 => blake3::keyed_hash(&self.bytes, input).as_bytes()[..16].to_vec(),
			Algorithm::HmacSha256 => {
				let key = aws_lc_rs::hmac::Key::new(aws_lc_rs::hmac::HMAC_SHA256, &self.bytes);
				aws_lc_rs::hmac::sign(&key, input).as_ref()[..16].to_vec()
			},
		}
	}

	/// Verify the signature in constant time.
	pub fn verify(&self, input: &[u8], signature: &[u8]) -> tg::Result<()> {
		let expected = self.sign(input);
		aws_lc_rs::constant_time::verify_slices_are_equal(&expected, signature)
			.map_err(|_| tg::error!("invalid signature"))?;
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

	pub fn sign(mut body: Body, secret: &Secret) -> tg::Result<Self> {
		// Order the permissions as the encoding does, so that the token equals its decoded form.
		body.validate()?;
		body.permissions.sort_by_key(|permission| permission.id());
		body.permissions.dedup();

		// Sign the body.
		let input = body.to_bytes()?;
		let signature = secret.sign(&input);
		let token = Self {
			body,
			secret_id: secret.id,
			signature,
		};

		Ok(token)
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
		if self.secret_id != secret.id {
			return Err(tg::error!("invalid secret"));
		}
		let input = self.body.to_bytes()?;
		secret.verify(&input, &self.signature)?;
		Ok(())
	}

	/// Encode the token as the signed body, followed by the secret ID and the signature.
	pub fn to_bytes(&self) -> tg::Result<Vec<u8>> {
		let mut bytes = self.body.to_bytes()?;
		bytes.write_uvarint(self.secret_id).unwrap();
		bytes.extend_from_slice(&self.signature);
		Ok(bytes)
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		// Read the version.
		let Some((&version, mut reader)) = bytes.split_first() else {
			return Err(tg::error!("missing the version"));
		};
		if version != VERSION {
			return Err(tg::error!("unsupported token version"));
		}

		// Read the expiration, the permissions, and the resource.
		let expires_at = reader
			.read_uvarint()
			.map_err(|error| tg::error!(!error, "failed to read the expiration"))?;
		let expires_at = i64::try_from(expires_at).map_err(|_| tg::error!("invalid expiration"))?;
		let permissions = reader
			.read_uvarint()
			.map_err(|error| tg::error!(!error, "failed to read the permissions"))?;
		let resource = tg::Id::from_reader(&mut reader)?;
		let Some(kind) = tg::authorization::ResourceKind::from_id_kind(resource.kind()) else {
			return Err(tg::error!("invalid resource"));
		};
		let permissions = (0..64u8)
			.filter(|id| permissions & (1 << id) != 0)
			.map(|id| {
				tg::authorization::Permission::with_kind_and_id(kind, id)
					.ok_or_else(|| tg::error!("invalid permissions"))
			})
			.collect::<tg::Result<Vec<_>>>()?;

		// Read the secret ID and the signature.
		let secret_id = reader
			.read_uvarint()
			.map_err(|error| tg::error!(!error, "failed to read the secret ID"))?;
		if reader.is_empty() {
			return Err(tg::error!("missing the signature"));
		}
		let signature = reader.to_vec();

		let body = Body {
			expires_at,
			permissions,
			resource,
		};
		let token = Self {
			body,
			secret_id,
			signature,
		};

		Ok(token)
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

	/// Encode the version, the expiration, the permissions, and the resource, which are the bytes that the signature covers.
	pub fn to_bytes(&self) -> tg::Result<Vec<u8>> {
		let expires_at =
			u64::try_from(self.expires_at).map_err(|_| tg::error!("invalid expiration"))?;
		let permissions = self
			.permissions
			.iter()
			.fold(0u64, |mask, permission| mask | (1 << permission.id()));
		let mut bytes = Vec::new();
		bytes.push(VERSION);
		bytes.write_uvarint(expires_at).unwrap();
		bytes.write_uvarint(permissions).unwrap();
		self.resource.to_writer(&mut bytes)?;
		Ok(bytes)
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
			.field("id", &self.id)
			.finish_non_exhaustive()
	}
}

impl std::fmt::Display for Token {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let bytes = self.to_bytes().map_err(|_| std::fmt::Error)?;
		write!(f, "{}", tg::id::ENCODING.encode(&bytes))
	}
}

impl std::str::FromStr for Token {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let bytes = tg::id::ENCODING
			.decode(value.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid token"))?;
		let token = Self::from_slice(&bytes)?;
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
		let key = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
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
			let secret = tg::authorization::Secret::generate(0, algorithm);
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

			assert_eq!(parsed, token);
			assert_eq!(parsed.body, body);
			assert_eq!(parsed.secret_id, 0);
			parsed.verify_at(&secret, 15).unwrap();
		}
	}

	#[test]
	fn secret_debug_omits_the_bytes() {
		let secret =
			tg::authorization::Secret::new(0, tg::authorization::Algorithm::Blake3, [0xab; 32]);
		let output = format!("{secret:?}");
		assert_eq!(output, "Secret { algorithm: Blake3, id: 0, .. }");
	}

	#[test]
	fn token_rejects_a_secret_with_a_different_algorithm() {
		let secret = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
		let other_secret = tg::authorization::Secret::new(
			0,
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

		assert_eq!(error.to_string(), "invalid signature");
	}

	#[test]
	fn token_rejects_an_invalid_signature() {
		let secret = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
		let other_secret =
			tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
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
	fn tokens_match_the_shared_fixtures() {
		#[derive(serde::Deserialize)]
		struct Fixture {
			expires_at: String,
			permissions: Vec<String>,
			resource: String,
			secret_id: u64,
			token: String,
		}
		let fixtures: Vec<Fixture> =
			serde_json::from_str(include_str!("../../fixtures/tokens.json")).unwrap();
		for fixture in fixtures {
			let secret = tg::authorization::Secret::new(
				fixture.secret_id,
				tg::authorization::Algorithm::Blake3,
				[0; 32],
			);
			let body = tg::authorization::Body {
				expires_at: fixture.expires_at.parse().unwrap(),
				permissions: fixture
					.permissions
					.iter()
					.map(|permission| permission.parse().unwrap())
					.collect(),
				resource: fixture.resource.parse().unwrap(),
			};
			let token = tg::authorization::Token::sign(body, &secret).unwrap();
			assert_eq!(token.to_string(), fixture.token);
			let parsed = fixture.token.parse::<tg::authorization::Token>().unwrap();
			assert_eq!(parsed, token);
			parsed.verify_at(&secret, 0).unwrap();
		}
	}

	#[test]
	fn token_rejects_a_different_secret_id() {
		let secret = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
		let other_secret =
			tg::authorization::Secret::new(1, tg::authorization::Algorithm::Blake3, secret.bytes);
		let body = tg::authorization::Body {
			expires_at: 20,
			permissions: vec![tg::authorization::Permission::Object(
				tg::authorization::permission::object::Permission::Subtree,
			)],
			resource: tg::Id::new_uuidv7(tg::id::Kind::File),
		};
		let token = tg::authorization::Token::sign(body, &secret).unwrap();

		let error = token.verify_at(&other_secret, 15).unwrap_err();

		assert_eq!(error.to_string(), "invalid secret");
	}

	#[test]
	fn token_rejects_malformed_bytes() {
		let secret = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
		let body = tg::authorization::Body {
			expires_at: 20,
			permissions: vec![tg::authorization::Permission::Process(
				tg::authorization::permission::process::Permission::Subtree,
			)],
			resource: tg::Id::new_uuidv7(tg::id::Kind::Process),
		};
		let token = tg::authorization::Token::sign(body, &secret).unwrap();
		let bytes = token.to_bytes().unwrap();

		let mut version = bytes.clone();
		version[0] = 1;
		assert!(tg::authorization::Token::from_slice(&version).is_err());
		let signature = token.signature.len();
		assert!(tg::authorization::Token::from_slice(&bytes[..bytes.len() - signature]).is_err());
		assert!(tg::authorization::Token::from_slice(&bytes[..4]).is_err());
		assert!(tg::authorization::Token::from_slice(&[]).is_err());
	}

	#[test]
	fn token_rejects_permissions_of_another_resource_kind() {
		let mut bytes = vec![0, 20];
		bytes.push(1 << 6);
		tg::Id::new_uuidv7(tg::id::Kind::Sandbox)
			.to_writer(&mut bytes)
			.unwrap();
		bytes.push(0);
		bytes.extend_from_slice(&[0; 16]);

		let error = tg::authorization::Token::from_slice(&bytes).unwrap_err();

		assert_eq!(error.to_string(), "invalid permissions");
	}

	#[test]
	fn token_rejects_an_expired_body() {
		let secret = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
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
		let secret = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
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
			secret_id: 0,
			signature: vec![0; 16],
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
		let token = |resource, secret_id: u64| tg::authorization::Token {
			body: tg::authorization::Body {
				expires_at: i64::MAX,
				permissions: vec![tg::authorization::Permission::Object(
					tg::authorization::permission::object::Permission::Subtree,
				)],
				resource,
			},
			secret_id,
			signature: vec![0; 16],
		};
		let first = tg::Id::new_blake3(tg::id::Kind::File, b"first");
		let second = tg::Id::new_blake3(tg::id::Kind::File, b"second");
		let a = token(first.clone(), 0);
		let b = token(first, 1);
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
		let c = token(second, 0);
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
