use {crate::prelude::*, std::time::Duration};

#[cfg(test)]
mod tests;

/// The key derivation context for a process ID that a runner derives.
const CONTEXT: &str = "tangram process id v1";

#[derive(
	Clone,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	derive_more::Debug,
	derive_more::Display,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[debug(r#"tg::process::Id("{_0}")"#)]
#[serde(into = "tg::Id", try_from = "tg::Id")]
#[tangram_serialize(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::Id);

/// A proof that a runner derived a process ID.
#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(content = "value", rename_all = "snake_case", tag = "kind")]
pub enum Proof {
	#[tangram_serialize(id = 0)]
	V1(ProofV1),
}

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct ProofV1 {
	#[tangram_serialize(id = 0)]
	pub salt: Salt,
}

/// A salt that a runner hashes with its ID to derive a process ID. A salt is 16 bytes, written with the ID encoding.
#[derive(
	Clone,
	Copy,
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
pub struct Salt([u8; 16]);

impl tg::process::Id {
	#[expect(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(tg::Id::new_uuidv7(tg::id::Kind::Process))
	}

	/// Derive a process ID from a runner's ID, the current time, and a random salt, and return it with the proof.
	#[must_use]
	pub fn with_runner(runner: &tg::runner::Id) -> (Self, Proof) {
		let salt = Salt::new();
		let timestamp = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap_or_default()
			.as_millis()
			.try_into()
			.unwrap_or(u64::MAX);
		let body = derive(runner, timestamp, &salt);
		let id = Self(tg::Id::new(
			tg::id::Kind::Process,
			tg::id::Body::UuidV7(body),
		));
		let proof = Proof::V1(ProofV1 { salt });
		(id, proof)
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		tg::Id::from_reader(bytes)?.try_into()
	}

	/// Get the unix timestamp in milliseconds of a process ID whose body is a UUIDv7.
	#[must_use]
	pub fn timestamp(&self) -> Option<u64> {
		let tg::id::Body::UuidV7(body) = self.body() else {
			return None;
		};
		let uuid = uuid::Uuid::from_bytes(*body);
		if uuid.get_version_num() != 7 {
			return None;
		}
		let mut bytes = [0u8; 8];
		bytes[2..].copy_from_slice(&body[..6]);
		let timestamp = u64::from_be_bytes(bytes);
		Some(timestamp)
	}
}

impl Proof {
	/// Verify that the runner derived the process ID.
	pub fn verify(&self, id: &tg::process::Id, runner: &tg::runner::Id) -> tg::Result<()> {
		let Self::V1(proof) = self;
		let timestamp = id
			.timestamp()
			.ok_or_else(|| tg::error!(%id, "the process id is not a uuidv7"))?;
		let body = derive(runner, timestamp, &proof.salt);
		if id.body().as_slice() != body.as_slice() {
			return Err(tg::error!(%id, %runner, "the runner did not derive the process id"));
		}
		Ok(())
	}

	/// Verify that the runner derived the process ID, and that the process ID's timestamp is within the window of now.
	pub fn verify_recent(
		&self,
		id: &tg::process::Id,
		runner: &tg::runner::Id,
		now: u64,
		window: Duration,
	) -> tg::Result<()> {
		self.verify(id, runner)?;
		let timestamp = id
			.timestamp()
			.ok_or_else(|| tg::error!(%id, "the process id is not a uuidv7"))?;
		let window = window.as_millis().try_into().unwrap_or(u64::MAX);
		if timestamp.abs_diff(now) > window {
			return Err(tg::error!(%id, "the process id's timestamp is outside the window"));
		}
		Ok(())
	}
}

impl Salt {
	/// Generate a new random salt.
	#[expect(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(rand::random())
	}

	#[must_use]
	pub fn as_bytes(&self) -> &[u8; 16] {
		&self.0
	}
}

impl std::ops::Deref for tg::process::Id {
	type Target = tg::Id;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<tg::process::Id> for tg::Id {
	fn from(value: tg::process::Id) -> Self {
		value.0
	}
}

impl TryFrom<tg::Id> for tg::process::Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Process {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl TryFrom<Vec<u8>> for tg::process::Id {
	type Error = tg::Error;

	fn try_from(value: Vec<u8>) -> tg::Result<Self> {
		Self::from_slice(&value)
	}
}

impl std::str::FromStr for tg::process::Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}

impl TryFrom<String> for tg::process::Id {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self> {
		value.parse()
	}
}

impl std::fmt::Display for Salt {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", tg::id::ENCODING.encode(&self.0))
	}
}

impl std::str::FromStr for Salt {
	type Err = tg::Error;

	fn from_str(value: &str) -> tg::Result<Self, Self::Err> {
		let bytes = tg::id::ENCODING
			.decode(value.as_bytes())
			.ok()
			.and_then(|bytes| <[u8; 16]>::try_from(bytes).ok())
			.ok_or_else(|| tg::error!(%value, "invalid salt"))?;
		let salt = Self(bytes);
		Ok(salt)
	}
}

/// Derive a UUIDv7 body from a timestamp and the hash of a runner's ID and a salt.
fn derive(runner: &tg::runner::Id, timestamp: u64, salt: &Salt) -> [u8; 16] {
	let mut hasher = blake3::Hasher::new_derive_key(CONTEXT);
	hasher.update(&runner.to_bytes());
	hasher.update(salt.as_bytes());
	let hash = hasher.finalize();
	let bytes = hash.as_bytes()[..10].try_into().unwrap();
	uuid::Builder::from_unix_timestamp_millis(timestamp, &bytes)
		.into_uuid()
		.into_bytes()
}
