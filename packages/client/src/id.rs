use crate::{return_error, Error, Result, WrapErr};
use derive_more::{From, Into};

/// An ID.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[serde(into = "String", try_from = "String")]
pub enum Id {
	V0(V0),
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct V0 {
	kind: Kind,
	hash: Hash,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum Kind {
	Leaf,
	Branch,
	Directory,
	File,
	Symlink,
	Lock,
	Target,
	Build,
	User,
	Login,
	Token,
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Hash {
	Random32([u8; 32]),
	Blake3([u8; 32]),
}

const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
	symbols: "abcdefghijklmnopqrstuvwxyz234567",
};

impl Id {
	#[must_use]
	pub fn new_random(kind: Kind) -> Self {
		let hash = Hash::Random32(rand::random());
		Self::V0(V0 { kind, hash })
	}

	#[must_use]
	pub fn new_hashed(kind: Kind, bytes: &[u8]) -> Self {
		let hash = blake3::hash(bytes);
		let hash = Hash::Blake3(*hash.as_bytes());
		Self::V0(V0 { kind, hash })
	}

	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Id::V0(v0) => v0.kind,
		}
	}
}

impl std::fmt::Debug for Id {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_tuple("Id").field(&self.to_string()).finish()
	}
}

impl std::fmt::Display for Id {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let kind = self.kind();
		let version = match self {
			Self::V0(_) => "0",
		};
		let algorithm = match self {
			Self::V0(v0) => match v0.hash {
				Hash::Random32(_) => "0",
				Hash::Blake3(_) => "1",
			},
		};
		let hash = match self {
			Self::V0(v0) => match v0.hash {
				Hash::Random32(bytes) | Hash::Blake3(bytes) => ENCODING.encode(&bytes),
			},
		};
		write!(f, "{kind}_{version}{algorithm}{hash}")?;
		Ok(())
	}
}

impl std::str::FromStr for Id {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let kind = s.get(0..=2).wrap_err("Invalid ID.")?.parse()?;
		let version = s.chars().nth(4).wrap_err("Invalid ID.")?;
		if version != '0' {
			return_error!("Invalid version.");
		}
		let algorithm = s.chars().nth(5).wrap_err("Invalid ID.")?;
		let hash = s.get(6..).wrap_err("Invalid ID.")?;
		let hash = match algorithm {
			'0' => Hash::Random32(
				ENCODING
					.decode(hash.as_bytes())
					.wrap_err("Invalid hash.")?
					.try_into()
					.ok()
					.wrap_err("Invalid hash.")?,
			),
			'1' => Hash::Blake3(
				ENCODING
					.decode(hash.as_bytes())
					.wrap_err("Invalid hash.")?
					.try_into()
					.ok()
					.wrap_err("Invalid hash.")?,
			),
			_ => return_error!("Invalid ID."),
		};
		Ok(Self::V0(V0 { kind, hash }))
	}
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let kind = match self {
			Kind::Leaf => "lef",
			Kind::Branch => "bch",
			Kind::Directory => "dir",
			Kind::File => "fil",
			Kind::Symlink => "sym",
			Kind::Lock => "lok",
			Kind::Target => "tgt",
			Kind::Build => "bld",
			Kind::User => "usr",
			Kind::Login => "lgn",
			Kind::Token => "tok",
		};
		write!(f, "{kind}")?;
		Ok(())
	}
}

impl std::str::FromStr for Kind {
	type Err = Error;

	fn from_str(s: &str) -> std::prelude::v1::Result<Self, Self::Err> {
		Ok(match s {
			"lef" => Kind::Leaf,
			"bch" => Kind::Branch,
			"dir" => Kind::Directory,
			"fil" => Kind::File,
			"sym" => Kind::Symlink,
			"lok" => Kind::Lock,
			"tgt" => Kind::Target,
			"bld" => Kind::Build,
			"usr" => Kind::User,
			"lgn" => Kind::Login,
			"tok" => Kind::Token,
			_ => return_error!("Invalid kind."),
		})
	}
}

impl From<Id> for String {
	fn from(value: Id) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Id {
	type Error = Error;

	fn try_from(value: String) -> Result<Self, Self::Error> {
		value.parse()
	}
}
