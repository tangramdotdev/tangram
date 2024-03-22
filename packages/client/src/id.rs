use derive_more::From;
use tangram_error::{error, Error, Result};

/// An ID.
#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[serde(into = "String", try_from = "String")]
pub enum Id {
	V0(V0),
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct V0 {
	kind: Kind,
	body: Body,
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
	Request,
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Body {
	UuidV7([u8; 16]),
	Blake3([u8; 32]),
}

const ENCODING: data_encoding::Encoding = data_encoding_macro::new_encoding! {
	symbols: "0123456789abcdefghjkmnpqrstvwxyz",
};

impl Id {
	#[must_use]
	pub fn new_uuidv7(kind: Kind) -> Self {
		let uuid = uuid::Uuid::now_v7();
		let body = Body::UuidV7(uuid.into_bytes());
		Self::V0(V0 { kind, body })
	}

	#[must_use]
	pub fn new_blake3(kind: Kind, bytes: &[u8]) -> Self {
		let hash = blake3::hash(bytes);
		let body = Body::Blake3(*hash.as_bytes());
		Self::V0(V0 { kind, body })
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
			Self::V0(v0) => match v0.body {
				Body::UuidV7(_) => "0",
				Body::Blake3(_) => "1",
			},
		};
		let body = match self {
			Self::V0(v0) => match v0.body {
				Body::UuidV7(body) => ENCODING.encode(&body),
				Body::Blake3(body) => ENCODING.encode(&body),
			},
		};
		write!(f, "{kind}_{version}{algorithm}{body}")?;
		Ok(())
	}
}

impl std::str::FromStr for Id {
	type Err = Error;

	fn from_str(id: &str) -> Result<Self, Self::Err> {
		let kind = id
			.get(0..=2)
			.ok_or_else(|| error!(%id, "invalid ID"))?
			.parse()?;
		let version = id.chars().nth(4).ok_or_else(|| error!(%id, "invalid ID"))?;
		if version != '0' {
			return Err(error!(%version, "invalid version"));
		}
		let algorithm = id.chars().nth(5).ok_or_else(|| error!(%id, "invalid ID"))?;
		let body = id.get(6..).ok_or_else(|| error!(%id, "invalid ID"))?;
		let body = match algorithm {
			'0' => Body::UuidV7(
				ENCODING
					.decode(body.as_bytes())
					.map_err(|source| error!(!source, "invalid body"))?
					.try_into()
					.ok()
					.ok_or_else(|| error!("invalid body"))?,
			),
			'1' => Body::Blake3(
				ENCODING
					.decode(body.as_bytes())
					.map_err(|source| error!(!source, "invalid body"))?
					.try_into()
					.ok()
					.ok_or_else(|| error!("invalid body"))?,
			),
			_ => return Err(error!(%id, "invalid ID")),
		};
		Ok(Self::V0(V0 { kind, body }))
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
			Kind::Request => "req",
		};
		write!(f, "{kind}")?;
		Ok(())
	}
}

impl std::str::FromStr for Kind {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
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
			"req" => Kind::Request,
			kind => return Err(error!(%kind, "invalid kind")),
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
