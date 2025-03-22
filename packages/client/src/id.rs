use crate as tg;
use byteorder::{ReadBytesExt as _, WriteBytesExt as _};

/// An ID.
#[derive(
	Clone,
	Eq,
	Hash,
	Ord,
	PartialEq,
	PartialOrd,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
)]
pub enum Id {
	V0(V0),
}

#[derive(Clone, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct V0 {
	pub kind: Kind,
	pub body: Body,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum Kind {
	Leaf,
	Branch,
	Directory,
	File,
	Symlink,
	Graph,
	Command,
	Process,
	Pipe,
	User,
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

	#[must_use]
	pub fn to_bytes(&self) -> Vec<u8> {
		let mut bytes = Vec::new();
		self.to_writer(&mut bytes).unwrap();
		bytes
	}

	pub fn from_slice(bytes: &[u8]) -> tg::Result<Self> {
		Self::from_reader(bytes)
	}

	pub fn to_writer(&self, mut writer: impl std::io::Write) -> tg::Result<()> {
		match self {
			Self::V0(v0) => {
				writer
					.write_u8(0)
					.map_err(|source| tg::error!(!source, "failed to write the version"))?;

				writer
					.write_u8(0)
					.map_err(|source| tg::error!(!source, "failed to write the padding"))?;

				let kind = match v0.kind {
					Kind::Leaf => 0,
					Kind::Branch => 1,
					Kind::Directory => 2,
					Kind::File => 3,
					Kind::Symlink => 4,
					Kind::Graph => 5,
					Kind::Command => 6,
					Kind::Process => 7,
					Kind::Pipe => 8,
					Kind::User => 9,
					Kind::Token => 10,
					Kind::Request => 11,
				};
				writer
					.write_u8(kind)
					.map_err(|source| tg::error!(!source, "failed to write the kind"))?;

				let algorithm = match v0.body {
					Body::UuidV7(_) => 0,
					Body::Blake3(_) => 1,
				};
				writer
					.write_u8(algorithm)
					.map_err(|source| tg::error!(!source, "failed to write the algorithm"))?;

				let body = match &v0.body {
					Body::UuidV7(body) => body.as_slice(),
					Body::Blake3(body) => body.as_slice(),
				};
				writer
					.write_all(body)
					.map_err(|source| tg::error!(!source, "failed to write the body"))?;

				Ok(())
			},
		}
	}

	pub fn from_reader(mut reader: impl std::io::Read) -> tg::Result<Self> {
		let version = reader
			.read_u8()
			.map_err(|source| tg::error!(!source, "failed to read the version"))?;

		if version != 0 {
			return Err(tg::error!(%version, "invalid version"));
		}

		let _padding = reader
			.read_u8()
			.map_err(|source| tg::error!(!source, "failed to read the padding"))?;

		let kind = reader
			.read_u8()
			.map_err(|source| tg::error!(!source, "failed to read the kind"))?;
		let kind = match kind {
			0 => Kind::Leaf,
			1 => Kind::Branch,
			2 => Kind::Directory,
			3 => Kind::File,
			4 => Kind::Symlink,
			5 => Kind::Graph,
			6 => Kind::Command,
			7 => Kind::Process,
			8 => Kind::Pipe,
			9 => Kind::User,
			10 => Kind::Token,
			11 => Kind::Request,
			_ => return Err(tg::error!(%kind, "invalid kind")),
		};

		let algorithm = reader
			.read_u8()
			.map_err(|source| tg::error!(!source, "failed to read the algorithm"))?;

		let body = match algorithm {
			0 => {
				let mut body = [0u8; 16];
				reader
					.read_exact(&mut body)
					.map_err(|source| tg::error!(!source, "failed to read the body"))?;
				Body::UuidV7(body)
			},
			1 => {
				let mut body = [0u8; 32];
				reader
					.read_exact(&mut body)
					.map_err(|source| tg::error!(!source, "failed to read the body"))?;
				Body::Blake3(body)
			},
			_ => return Err(tg::error!(%algorithm, "invalid algorithm")),
		};

		Ok(Self::V0(V0 { kind, body }))
	}

	#[must_use]
	pub fn body(&self) -> &Body {
		match self {
			Self::V0(s) => &s.body,
		}
	}
}

impl Body {
	#[must_use]
	pub fn as_slice(&self) -> &[u8] {
		match self {
			Self::UuidV7(s) => s.as_slice(),
			Self::Blake3(s) => s.as_slice(),
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
		match self {
			Self::V0(v0) => {
				let kind = self.kind();
				let version = "0";
				let algorithm = match v0.body {
					Body::UuidV7(_) => "0",
					Body::Blake3(_) => "1",
				};
				let body = match v0.body {
					Body::UuidV7(body) => ENCODING.encode(&body),
					Body::Blake3(body) => ENCODING.encode(&body),
				};
				write!(f, "{kind}_{version}{algorithm}{body}")?;
			},
		}
		Ok(())
	}
}

impl std::str::FromStr for Id {
	type Err = tg::Error;

	fn from_str(id: &str) -> tg::Result<Self, Self::Err> {
		let kind = id
			.get(0..=2)
			.ok_or_else(|| tg::error!(%id, "invalid ID"))?
			.parse()?;
		let version = id
			.chars()
			.nth(4)
			.ok_or_else(|| tg::error!(%id, "invalid ID"))?;
		if version != '0' {
			return Err(tg::error!(%version, "invalid version"));
		}
		let algorithm = id
			.chars()
			.nth(5)
			.ok_or_else(|| tg::error!(%id, "invalid ID"))?;
		let body = id.get(6..).ok_or_else(|| tg::error!(%id, "invalid ID"))?;
		let body = match algorithm {
			'0' => Body::UuidV7(
				ENCODING
					.decode(body.as_bytes())
					.map_err(|source| tg::error!(!source, "invalid body"))?
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("invalid body"))?,
			),
			'1' => Body::Blake3(
				ENCODING
					.decode(body.as_bytes())
					.map_err(|source| tg::error!(!source, "invalid body"))?
					.try_into()
					.ok()
					.ok_or_else(|| tg::error!("invalid body"))?,
			),
			_ => {
				return Err(tg::error!(%id, "invalid ID"));
			},
		};
		Ok(Self::V0(V0 { kind, body }))
	}
}

impl std::fmt::Display for Kind {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let kind = match self {
			Self::Leaf => "lef",
			Self::Branch => "bch",
			Self::Directory => "dir",
			Self::File => "fil",
			Self::Symlink => "sym",
			Self::Graph => "gph",
			Self::Command => "cmd",
			Self::Process => "pcs",
			Self::Pipe => "pip",
			Self::User => "usr",
			Self::Token => "tok",
			Self::Request => "req",
		};
		write!(f, "{kind}")?;
		Ok(())
	}
}

impl std::str::FromStr for Kind {
	type Err = tg::Error;

	fn from_str(s: &str) -> tg::Result<Self, Self::Err> {
		Ok(match s {
			"lef" | "leaf" => Self::Leaf,
			"bch" | "branch" => Self::Branch,
			"dir" | "directory" => Self::Directory,
			"fil" | "file" => Self::File,
			"sym" | "symlink" => Self::Symlink,
			"gph" | "graph" => Self::Graph,
			"cmd" | "command" => Self::Command,
			"pcs" | "process" => Self::Process,
			"pip" | "pipe" => Self::Pipe,
			"usr" | "user" => Self::User,
			"tok" | "token" => Self::Token,
			"req" | "request" => Self::Request,
			_ => {
				return Err(tg::error!(%s, "invalid kind"));
			},
		})
	}
}
