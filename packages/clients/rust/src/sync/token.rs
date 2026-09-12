use crate::prelude::*;

const VERSION: &str = "0";

/// A signed identifier for an incoming sync. It conveys no permissions.
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
	pub metadata: tg::authorization::Metadata,
	pub signature: Vec<u8>,
}

#[derive(
	Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct Body {
	pub expires_at: i64,
	pub id: String,
}

impl Token {
	pub fn sign(body: Body, private_key: &tg::authorization::PrivateKey) -> tg::Result<Self> {
		let metadata = tg::authorization::Metadata {
			algorithm: private_key.algorithm,
			key: private_key.name.clone(),
		};
		let input = Self::input(&body, &metadata)?;
		let signature = match metadata.algorithm {
			tg::authorization::Algorithm::Ed25519 => {
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

	pub fn verify_at(&self, public_key: &tg::authorization::PublicKey, now: i64) -> tg::Result<()> {
		if self.metadata.algorithm != public_key.algorithm {
			return Err(tg::error!("invalid algorithm"));
		}
		if self.metadata.key != public_key.name {
			return Err(tg::error!("invalid key"));
		}
		let input = Self::input(&self.body, &self.metadata)?;
		match self.metadata.algorithm {
			tg::authorization::Algorithm::Ed25519 => {
				let key = aws_lc_rs::signature::UnparsedPublicKey::new(
					&aws_lc_rs::signature::ED25519,
					&public_key.bytes,
				);
				key.verify(input.as_bytes(), &self.signature)
					.map_err(|_| tg::error!("invalid signature"))?;
			},
		}
		if now > self.body.expires_at {
			return Err(tg::error!("expired sync token"));
		}
		Ok(())
	}

	fn input(body: &Body, metadata: &tg::authorization::Metadata) -> tg::Result<String> {
		let body = serde_json::to_vec(body)
			.map_err(|error| tg::error!(!error, "failed to serialize the body"))
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let metadata = serde_json::to_vec(metadata)
			.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))
			.map(|bytes| data_encoding::BASE64.encode(&bytes))?;
		let input = format!("{VERSION}.{body}.{metadata}");
		Ok(input)
	}
}

impl Body {
	#[must_use]
	pub fn new(expires_at: i64) -> Self {
		let id = tg::id::ENCODING.encode(&uuid::Uuid::now_v7().into_bytes());
		Self { expires_at, id }
	}
}

impl std::fmt::Display for Token {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let input = Self::input(&self.body, &self.metadata).map_err(|_| std::fmt::Error)?;
		let signature = data_encoding::BASE64.encode(&self.signature);
		write!(f, "{input}.{signature}")
	}
}

impl std::str::FromStr for Token {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self> {
		let mut parts = value.split('.');
		let version = parts
			.next()
			.ok_or_else(|| tg::error!("missing the version"))?;
		if version != VERSION {
			return Err(tg::error!("unsupported sync token version"));
		}
		let body = parts.next().ok_or_else(|| tg::error!("missing the body"))?;
		let metadata = parts
			.next()
			.ok_or_else(|| tg::error!("missing the metadata"))?;
		let signature = parts
			.next()
			.ok_or_else(|| tg::error!("missing the signature"))?;
		if parts.next().is_some() {
			return Err(tg::error!("invalid sync token"));
		}
		let body = data_encoding::BASE64
			.decode(body.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid body"))?;
		let body =
			serde_json::from_slice(&body).map_err(|error| tg::error!(!error, "invalid body"))?;
		let metadata = data_encoding::BASE64
			.decode(metadata.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid metadata"))?;
		let metadata = serde_json::from_slice(&metadata)
			.map_err(|error| tg::error!(!error, "invalid metadata"))?;
		let signature = data_encoding::BASE64
			.decode(signature.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid signature"))?;
		let token = Self {
			body,
			metadata,
			signature,
		};
		Ok(token)
	}
}

#[cfg(test)]
mod tests {
	use {super::*, crate as tg};

	fn keys() -> (tg::authorization::PrivateKey, tg::authorization::PublicKey) {
		let private_key =
			tg::authorization::PrivateKey::generate("test", tg::authorization::Algorithm::Ed25519)
				.unwrap();
		let public_key = tg::authorization::PublicKey::from_private_key(&private_key).unwrap();
		(private_key, public_key)
	}

	#[test]
	fn unique() {
		let (private_key, _) = keys();
		let first = Token::sign(Body::new(i64::MAX), &private_key).unwrap();
		let second = Token::sign(Body::new(i64::MAX), &private_key).unwrap();
		assert_ne!(first, second);
	}

	#[test]
	fn roundtrip() {
		let (private_key, public_key) = keys();
		let token = Token::sign(Body::new(i64::MAX), &private_key).unwrap();
		assert_eq!(token.to_string().parse::<Token>().unwrap(), token);
		let json = serde_json::to_string(&token).unwrap();
		assert_eq!(serde_json::from_str::<Token>(&json).unwrap(), token);
		let bytes = tangram_serialize::to_vec(&token).unwrap();
		assert_eq!(
			tangram_serialize::from_slice::<Token>(&bytes).unwrap(),
			token
		);
		token.verify_at(&public_key, 0).unwrap();
	}

	#[test]
	fn rejects_invalid_tokens() {
		let (private_key, public_key) = keys();
		let (_, other_public_key) = keys();
		let token = Token::sign(Body::new(1), &private_key).unwrap();
		assert!(token.verify_at(&public_key, 2).is_err());
		assert!(token.verify_at(&other_public_key, 0).is_err());
		let mut forged = token.clone();
		forged.body.id = "forged".to_owned();
		assert!(forged.verify_at(&public_key, 0).is_err());
		for value in ["", "!", "000", "1.a.b.c"] {
			assert!(value.parse::<Token>().is_err());
		}
	}
}
