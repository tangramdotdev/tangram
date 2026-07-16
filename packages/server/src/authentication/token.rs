use tangram_client::prelude::*;

const CLOCK_SKEW: i64 = 60;
const DOMAIN: &str = "authentication";
const VERSION: &str = "0";

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(super) struct Token {
	pub body: Body,
	pub metadata: tg::grant::Metadata,
	pub signature: Vec<u8>,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub(super) struct Body {
	pub expires_at: i64,
	pub issued_at: i64,
	pub principal: Principal,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub(super) enum Principal {
	Process(tg::process::Id),
	Sandbox(tg::sandbox::Id),
}

impl Token {
	pub fn sign(body: Body, private_key: &tg::grant::PrivateKey) -> tg::Result<Self> {
		body.validate()?;
		let metadata = tg::grant::Metadata {
			algorithm: private_key.algorithm,
			key: private_key.name.clone(),
		};
		let input = Self::input(&body, &metadata)?;
		let signature = match metadata.algorithm {
			tg::grant::Algorithm::Ed25519 => {
				let key =
					aws_lc_rs::signature::Ed25519KeyPair::from_seed_unchecked(&private_key.bytes)
						.map_err(|_| tg::error!("invalid private key"))?;
				key.sign(input.as_bytes()).as_ref().to_vec()
			},
		};
		let token = Self {
			body,
			metadata,
			signature,
		};

		Ok(token)
	}

	pub fn verify(&self, public_key: &tg::grant::PublicKey) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		self.verify_at(public_key, now)
	}

	pub fn verify_at(&self, public_key: &tg::grant::PublicKey, now: i64) -> tg::Result<()> {
		if self.metadata.algorithm != public_key.algorithm {
			return Err(tg::error!("invalid algorithm"));
		}
		if self.metadata.key != public_key.name {
			return Err(tg::error!("invalid key"));
		}
		let input = Self::input(&self.body, &self.metadata)?;
		match self.metadata.algorithm {
			tg::grant::Algorithm::Ed25519 => {
				let key = aws_lc_rs::signature::UnparsedPublicKey::new(
					&aws_lc_rs::signature::ED25519,
					&public_key.bytes,
				);
				key.verify(input.as_bytes(), &self.signature)
					.map_err(|_| tg::error!("invalid signature"))?;
			},
		}
		self.body.validate_at(now)?;

		Ok(())
	}

	#[must_use]
	pub fn has_prefix(value: &str) -> bool {
		value.starts_with(&format!("{DOMAIN}.{VERSION}."))
	}

	pub fn validate(&self) -> tg::Result<()> {
		let now = time::OffsetDateTime::now_utc().unix_timestamp();
		self.body.validate_at(now)
	}

	fn input(body: &Body, metadata: &tg::grant::Metadata) -> tg::Result<String> {
		let body = serde_json::to_vec(body)
			.map_err(|error| tg::error!(!error, "failed to serialize the body"))
			.map(|bytes| tg::id::ENCODING.encode(&bytes))?;
		let metadata = serde_json::to_vec(metadata)
			.map_err(|error| tg::error!(!error, "failed to serialize the metadata"))
			.map(|bytes| tg::id::ENCODING.encode(&bytes))?;
		let input = format!("{DOMAIN}.{VERSION}.{body}.{metadata}");

		Ok(input)
	}
}

impl Body {
	fn validate(&self) -> tg::Result<()> {
		if self.expires_at < self.issued_at {
			return Err(tg::error!("invalid expiration"));
		}

		Ok(())
	}

	fn validate_at(&self, now: i64) -> tg::Result<()> {
		self.validate()?;
		if self.issued_at > now.saturating_add(CLOCK_SKEW) {
			return Err(tg::error!("invalid issuance time"));
		}
		if now > self.expires_at {
			return Err(tg::error!("expired token"));
		}

		Ok(())
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
		let input = Self::input(&self.body, &self.metadata).map_err(|_| std::fmt::Error)?;
		let signature = tg::id::ENCODING.encode(&self.signature);
		write!(f, "{input}.{signature}")
	}
}

impl std::str::FromStr for Token {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self> {
		let mut parts = value.split('.');
		let domain = parts
			.next()
			.ok_or_else(|| tg::error!("missing the domain"))?;
		if domain != DOMAIN {
			return Err(tg::error!("unsupported token domain"));
		}
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
		let body = tg::id::ENCODING
			.decode(body.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid body"))?;
		let body =
			serde_json::from_slice(&body).map_err(|error| tg::error!(!error, "invalid body"))?;
		let metadata = tg::id::ENCODING
			.decode(metadata.as_bytes())
			.map_err(|error| tg::error!(!error, "invalid metadata"))?;
		let metadata = serde_json::from_slice(&metadata)
			.map_err(|error| tg::error!(!error, "invalid metadata"))?;
		let signature = tg::id::ENCODING
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
