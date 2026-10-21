use {crate::prelude::*, std::time::Duration};

/// The key derivation context for a sandbox ID that a runner derives.
const CONTEXT: &str = "tangram sandbox id v1";

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
#[debug(r#"tg::sandbox::Id("{_0}")"#)]
#[serde(into = "tg::Id", try_from = "tg::Id")]
#[tangram_serialize(into = "tg::Id", try_from = "tg::Id")]
pub struct Id(tg::Id);

/// A proof that a runner derived a sandbox ID.
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
	pub salt: tg::process::id::Salt,
}

impl tg::sandbox::Id {
	#[expect(clippy::new_without_default)]
	#[must_use]
	pub fn new() -> Self {
		Self(tg::Id::new_uuidv7(tg::id::Kind::Sandbox))
	}

	/// Derive a sandbox ID from a runner's ID, the current time, and a random salt, and return it with the proof.
	#[must_use]
	pub fn with_runner(runner: &tg::runner::Id) -> (Self, Proof) {
		let salt = tg::process::id::Salt::new();
		let timestamp = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.unwrap_or_default()
			.as_millis()
			.try_into()
			.unwrap_or(u64::MAX);
		let body = derive(runner, timestamp, &salt);
		let id = Self(tg::Id::new(
			tg::id::Kind::Sandbox,
			tg::id::Body::UuidV7(body),
		));
		let proof = Proof::V1(ProofV1 { salt });
		(id, proof)
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		tg::Id::from_reader(bytes)?.try_into()
	}

	/// Get the unix timestamp in milliseconds of a sandbox ID whose body is a `UUIDv7`.
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
	/// Verify that the runner derived the sandbox ID.
	pub fn verify(&self, id: &tg::sandbox::Id, runner: &tg::runner::Id) -> tg::Result<()> {
		let Self::V1(proof) = self;
		let timestamp = id
			.timestamp()
			.ok_or_else(|| tg::error!(%id, "the sandbox id is not a uuidv7"))?;
		let body = derive(runner, timestamp, &proof.salt);
		if id.body().as_slice() != body.as_slice() {
			return Err(tg::error!(%id, %runner, "the runner did not derive the sandbox id"));
		}
		Ok(())
	}

	/// Verify that the runner derived the sandbox ID, and that the sandbox ID's timestamp is within the window of now.
	pub fn verify_recent(
		&self,
		id: &tg::sandbox::Id,
		runner: &tg::runner::Id,
		now: u64,
		window: Duration,
	) -> tg::Result<()> {
		self.verify(id, runner)?;
		let timestamp = id
			.timestamp()
			.ok_or_else(|| tg::error!(%id, "the sandbox id is not a uuidv7"))?;
		let window = window.as_millis().try_into().unwrap_or(u64::MAX);
		if timestamp.abs_diff(now) > window {
			return Err(tg::error!(%id, "the sandbox id's timestamp is outside the window"));
		}
		Ok(())
	}
}

impl std::ops::Deref for tg::sandbox::Id {
	type Target = tg::Id;

	fn deref(&self) -> &Self::Target {
		&self.0
	}
}

impl From<tg::sandbox::Id> for tg::Id {
	fn from(value: tg::sandbox::Id) -> Self {
		value.0
	}
}

impl TryFrom<tg::Id> for tg::sandbox::Id {
	type Error = tg::Error;

	fn try_from(value: tg::Id) -> tg::Result<Self, Self::Error> {
		if value.kind() != tg::id::Kind::Sandbox {
			return Err(tg::error!(%value, "invalid kind"));
		}
		Ok(Self(value))
	}
}

impl TryFrom<Vec<u8>> for tg::sandbox::Id {
	type Error = tg::Error;

	fn try_from(value: Vec<u8>) -> tg::Result<Self> {
		Self::from_slice(&value)
	}
}

impl std::str::FromStr for tg::sandbox::Id {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		tg::Id::from_str(s)?.try_into()
	}
}

impl TryFrom<String> for tg::sandbox::Id {
	type Error = tg::Error;

	fn try_from(value: String) -> tg::Result<Self> {
		value.parse()
	}
}

/// Derive a `UUIDv7` body from a timestamp and the hash of a runner's ID and a salt.
fn derive(runner: &tg::runner::Id, timestamp: u64, salt: &tg::process::id::Salt) -> [u8; 16] {
	let mut hasher = blake3::Hasher::new_derive_key(CONTEXT);
	hasher.update(&runner.to_bytes());
	hasher.update(salt.as_bytes());
	let hash = hasher.finalize();
	let bytes = hash.as_bytes()[..10].try_into().unwrap();
	uuid::Builder::from_unix_timestamp_millis(timestamp, &bytes)
		.into_uuid()
		.into_bytes()
}

#[cfg(test)]
mod tests {
	use {super::*, std::time::Duration};

	// A derived sandbox id is a uuidv7 that its proof verifies, and the proof fails for another runner, another salt, or a minted id.
	#[test]
	fn a_derived_id_verifies_only_with_its_runner_and_salt() {
		let runner = tg::runner::Id::new();
		let (id, proof) = tg::sandbox::Id::with_runner(&runner);
		let tg::id::Body::UuidV7(body) = id.body() else {
			panic!("expected a uuidv7 body");
		};
		assert_eq!(uuid::Uuid::from_bytes(*body).get_version_num(), 7);
		proof.verify(&id, &runner).unwrap();
		assert!(proof.verify(&id, &tg::runner::Id::new()).is_err());
		let other = Proof::V1(ProofV1 {
			salt: tg::process::id::Salt::new(),
		});
		assert!(other.verify(&id, &runner).is_err());
		assert!(proof.verify(&tg::sandbox::Id::new(), &runner).is_err());
	}

	// A recent proof verifies only within the window of its id's timestamp.
	#[test]
	fn a_recent_proof_verifies_only_within_the_window() {
		let runner = tg::runner::Id::new();
		let (id, proof) = tg::sandbox::Id::with_runner(&runner);
		let timestamp = id.timestamp().unwrap();
		let window = Duration::from_mins(1);
		for now in [timestamp, timestamp + 60_000, timestamp - 60_000] {
			proof.verify_recent(&id, &runner, now, window).unwrap();
		}
		for now in [timestamp + 60_001, timestamp - 60_001] {
			assert!(proof.verify_recent(&id, &runner, now, window).is_err());
		}
	}

	// A sandbox id derived by a process's derivation does not verify, because the contexts differ.
	#[test]
	fn a_process_derivation_does_not_verify() {
		let runner = tg::runner::Id::new();
		let (process, tg::process::id::Proof::V1(proof)) = tg::process::Id::with_runner(&runner);
		let id = tg::Id::new(tg::id::Kind::Sandbox, process.body().clone());
		let id = tg::sandbox::Id::try_from(id).unwrap();
		let proof = Proof::V1(ProofV1 { salt: proof.salt });
		assert!(proof.verify(&id, &runner).is_err());
	}

	// A sandbox id round-trips through its byte representation.
	#[test]
	fn roundtrip_bytes() {
		let id = super::Id::new();
		let roundtrip = super::Id::from_slice(&id.to_bytes()).expect("failed to parse the id");
		assert_eq!(roundtrip, id);
	}

	// A sandbox id round-trips through its string representation and uses the sbx_ prefix.
	#[test]
	fn roundtrip_string() {
		let id = super::Id::new();
		let roundtrip = id
			.to_string()
			.parse::<super::Id>()
			.expect("failed to parse the id");
		assert_eq!(roundtrip, id);
		assert!(id.to_string().starts_with("sbx_"));
	}
}
