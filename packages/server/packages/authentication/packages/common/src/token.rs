use {
	tangram_client::prelude::*,
	tangram_util::varint::{ReadExt as _, WriteExt as _},
};

const VERSION: u8 = 0;

#[derive(Clone, Debug)]
pub enum Token {
	Signed(Signed),
	Unsigned(Unsigned),
}

#[derive(Clone, Debug)]
pub struct Signed {
	pub body: Body,
	pub secret_id: u64,
	pub signature: Vec<u8>,
}

#[derive(Clone, Debug)]
pub struct Body {
	pub expires_at: i64,
	pub principal: Principal,
}

#[derive(Clone, Debug)]
pub enum Principal {
	Process(tg::process::Id),
	Sandbox(tg::sandbox::Id),
}

#[derive(Clone, Debug)]
pub struct Unsigned {
	pub bytes: [u8; 32],
	pub kind: tg::principal::Kind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum Kind {
	Signed = 0,
	Unsigned = 1,
}

/// Create an unsigned token for a principal of the given kind.
#[must_use]
pub fn create(kind: tg::principal::Kind) -> (tg::token::Id, String) {
	let id = tg::token::Id::new();
	let token = Token::Unsigned(Unsigned::generate(kind)).to_string();

	(id, token)
}

#[must_use]
pub fn hash(token: &str) -> String {
	blake3::Hash::from_bytes(hash_bytes(token))
		.to_hex()
		.to_string()
}

#[must_use]
pub fn hash_bytes(token: &str) -> [u8; 32] {
	*blake3::hash(token.as_bytes()).as_bytes()
}

#[must_use]
pub fn matches(actual: &str, expected: &str) -> bool {
	let actual = blake3::hash(actual.as_bytes());
	let expected = blake3::hash(expected.as_bytes());
	aws_lc_rs::constant_time::verify_slices_are_equal(actual.as_bytes(), expected.as_bytes())
		.is_ok()
}

impl Token {
	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Self::Signed(_) => Kind::Signed,
			Self::Unsigned(_) => Kind::Unsigned,
		}
	}

	pub fn to_bytes(&self) -> tg::Result<Vec<u8>> {
		match self {
			Self::Signed(signed) => signed.to_bytes(),
			Self::Unsigned(unsigned) => Ok(unsigned.to_bytes()),
		}
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		// Read the version and the kind.
		let [version, kind, ..] = bytes else {
			return Err(tg::error!("invalid token"));
		};
		if *version != VERSION {
			return Err(tg::error!("unsupported token version"));
		}
		let kind = Kind::try_from(*kind)?;

		// Read the rest of the token.
		let token = match kind {
			Kind::Signed => Self::Signed(Signed::from_slice(bytes)?),
			Kind::Unsigned => Self::Unsigned(Unsigned::from_slice(bytes)?),
		};

		Ok(token)
	}
}

impl Signed {
	pub fn sign(body: Body, secret: &tg::authorization::Secret) -> tg::Result<Self> {
		let input = body.to_bytes()?;
		let signature = secret.sign(&input);
		let signed = Self {
			body,
			secret_id: secret.id,
			signature,
		};

		Ok(signed)
	}

	pub fn verify_at(&self, secret: &tg::authorization::Secret, now: i64) -> tg::Result<()> {
		if self.secret_id != secret.id {
			return Err(tg::error!("invalid secret"));
		}
		let input = self.body.to_bytes()?;
		secret.verify(&input, &self.signature)?;
		self.body.validate_at(now)?;

		Ok(())
	}

	/// Encode the token as the signed body, followed by the secret ID and the signature.
	pub fn to_bytes(&self) -> tg::Result<Vec<u8>> {
		let mut bytes = self.body.to_bytes()?;
		bytes.write_uvarint(self.secret_id).unwrap();
		bytes.extend_from_slice(&self.signature);
		Ok(bytes)
	}

	fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		// Read the expiration and the principal.
		let mut reader = bytes.get(2..).unwrap_or_default();
		let expires_at = reader
			.read_uvarint()
			.map_err(|error| tg::error!(!error, "failed to read the expiration"))?;
		let expires_at = i64::try_from(expires_at).map_err(|_| tg::error!("invalid expiration"))?;
		let principal = tg::Id::from_reader(&mut reader)?;
		let principal = match principal.kind() {
			tg::id::Kind::Process => Principal::Process(principal.try_into()?),
			tg::id::Kind::Sandbox => Principal::Sandbox(principal.try_into()?),
			_ => {
				return Err(tg::error!("invalid principal"));
			},
		};

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
			principal,
		};
		let signed = Self {
			body,
			secret_id,
			signature,
		};

		Ok(signed)
	}
}

impl Body {
	/// Encode the version, the kind, the expiration, and the principal, which are the bytes that the signature covers.
	pub fn to_bytes(&self) -> tg::Result<Vec<u8>> {
		let expires_at =
			u64::try_from(self.expires_at).map_err(|_| tg::error!("invalid expiration"))?;
		let principal: tg::Id = match &self.principal {
			Principal::Process(id) => id.clone().into(),
			Principal::Sandbox(id) => id.clone().into(),
		};
		let mut bytes = vec![VERSION, Kind::Signed.into()];
		bytes.write_uvarint(expires_at).unwrap();
		principal.to_writer(&mut bytes)?;
		Ok(bytes)
	}

	fn validate_at(&self, now: i64) -> tg::Result<()> {
		if now > self.expires_at {
			return Err(tg::error!("expired token"));
		}

		Ok(())
	}
}

impl Unsigned {
	#[must_use]
	pub fn generate(kind: tg::principal::Kind) -> Self {
		let bytes = rand::random();
		Self { bytes, kind }
	}

	#[must_use]
	pub fn to_bytes(&self) -> Vec<u8> {
		let mut bytes = vec![VERSION, Kind::Unsigned.into(), self.kind.into()];
		bytes.extend_from_slice(&self.bytes);
		bytes
	}

	fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		let [_, _, kind, bytes @ ..] = bytes else {
			return Err(tg::error!("invalid token"));
		};
		let kind = tg::principal::Kind::try_from(*kind)?;
		let bytes = bytes.try_into().map_err(|_| tg::error!("invalid token"))?;
		let unsigned = Self { bytes, kind };

		Ok(unsigned)
	}
}

impl From<Kind> for u8 {
	fn from(value: Kind) -> Self {
		value as u8
	}
}

impl TryFrom<u8> for Kind {
	type Error = tg::Error;

	fn try_from(value: u8) -> tg::Result<Self> {
		match value {
			0 => Ok(Self::Signed),
			1 => Ok(Self::Unsigned),
			_ => Err(tg::error!(%value, "invalid token kind")),
		}
	}
}

impl From<Principal> for tg::Principal {
	fn from(value: Principal) -> Self {
		match value {
			Principal::Process(process) => Self::Process(process),
			Principal::Sandbox(sandbox) => Self::Sandbox(sandbox),
		}
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

	fn from_str(value: &str) -> tg::Result<Self> {
		let bytes = tg::id::ENCODING
			.decode(value.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid token"))?;
		let token = Self::from_slice(&bytes)?;

		Ok(token)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn signed_token_round_trips_and_verifies() {
		let secret = tg::authorization::Secret::generate(3, tg::authorization::Algorithm::Blake3);
		let process = tg::process::Id::new();
		let body = Body {
			expires_at: 20,
			principal: Principal::Process(process.clone()),
		};
		let token = Token::Signed(Signed::sign(body, &secret).unwrap());

		let parsed = token.to_string().parse::<Token>().unwrap();

		let Token::Signed(signed) = parsed else {
			panic!("expected a signed token");
		};
		assert_eq!(signed.secret_id, 3);
		assert!(matches!(&signed.body.principal, Principal::Process(id) if *id == process));
		signed.verify_at(&secret, 15).unwrap();
		assert_eq!(
			signed.verify_at(&secret, 21).unwrap_err().to_string(),
			"expired token"
		);
	}

	#[test]
	fn signed_token_rejects_a_different_secret() {
		let secret = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
		let other = tg::authorization::Secret::generate(0, tg::authorization::Algorithm::Blake3);
		let body = Body {
			expires_at: 20,
			principal: Principal::Sandbox(tg::sandbox::Id::new()),
		};
		let signed = Signed::sign(body, &secret).unwrap();

		let error = signed.verify_at(&other, 15).unwrap_err();

		assert_eq!(error.to_string(), "invalid signature");
	}

	#[test]
	fn unsigned_token_round_trips() {
		let (_, token) = create(tg::principal::Kind::User);

		let parsed = token.parse::<Token>().unwrap();

		let Token::Unsigned(unsigned) = parsed else {
			panic!("expected an unsigned token");
		};
		assert_eq!(unsigned.kind, tg::principal::Kind::User);
		assert_eq!(Token::Unsigned(unsigned).to_string(), token);
	}

	#[test]
	fn token_rejects_malformed_bytes() {
		let (_, token) = create(tg::principal::Kind::Runner);
		let bytes = tg::id::ENCODING.decode(token.as_bytes()).unwrap();

		assert!(Token::from_slice(&bytes[..bytes.len() - 1]).is_err());
		let mut version = bytes.clone();
		version[0] = 1;
		assert!(Token::from_slice(&version).is_err());
		let mut kind = bytes;
		kind[1] = 2;
		assert!(Token::from_slice(&kind).is_err());
		assert!(Token::from_slice(&[]).is_err());
	}
}
