use std::str::FromStr;

use crate as tg;

#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Io {
	Pipe(tg::pipe::Id),
	Pty(tg::pty::Id),
}

impl ToString for Io {
	fn to_string(&self) -> String {
		match self {
			Io::Pipe(id) => id.to_string(),
			Io::Pty(id) => id.to_string(),
		}
	}
}

impl FromStr for Io {
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
