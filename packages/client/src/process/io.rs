use crate as tg;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Io {
	Pipe(tg::pipe::Id),
	Pty(tg::pty::Id),
}

impl std::fmt::Display for Io {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Pipe(id) => write!(f, "{id}"),
			Self::Pty(id) => write!(f, "{id}"),
		}
	}
}

impl std::str::FromStr for Io {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let id = tg::Id::from_str(s)?;
		match id.kind() {
			tg::id::Kind::Pipe => Ok(Io::Pipe(id.try_into().unwrap())),
			tg::id::Kind::Pty => Ok(Io::Pty(id.try_into().unwrap())),
			_ => Err(tg::error!("invalid kind")),
		}
	}
}
