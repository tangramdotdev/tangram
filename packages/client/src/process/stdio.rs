use crate as tg;

#[derive(
	Clone,
	Debug,
	PartialEq,
	Eq,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[serde(untagged)]
pub enum Stdio {
	#[tangram_serialize(id = 0)]
	Pipe(tg::pipe::Id),
	#[tangram_serialize(id = 1)]
	Pty(tg::pty::Id),
}

impl std::fmt::Display for Stdio {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Pipe(id) => write!(f, "{id}"),
			Self::Pty(id) => write!(f, "{id}"),
		}
	}
}

impl std::str::FromStr for Stdio {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let id = tg::Id::from_str(s)?;
		match id.kind() {
			tg::id::Kind::Pipe => Ok(Stdio::Pipe(id.try_into().unwrap())),
			tg::id::Kind::Pty => Ok(Stdio::Pty(id.try_into().unwrap())),
			_ => Err(tg::error!("invalid kind")),
		}
	}
}
