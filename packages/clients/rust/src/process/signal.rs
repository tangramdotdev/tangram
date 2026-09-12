use crate::prelude::*;

pub mod post;

#[derive(Clone, Debug, Default)]
pub struct Options {
	pub location: Option<tg::location::Arg>,
}

#[derive(
	Clone,
	Copy,
	Debug,
	Eq,
	PartialEq,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[repr(u8)]
pub enum Signal {
	#[tangram_serialize(id = 6)]
	SIGABRT = 6,
	#[tangram_serialize(id = 14)]
	SIGALRM = 14,
	#[tangram_serialize(id = 8)]
	SIGFPE = 8,
	#[tangram_serialize(id = 1)]
	SIGHUP = 1,
	#[tangram_serialize(id = 4)]
	SIGILL = 4,
	#[tangram_serialize(id = 2)]
	SIGINT = 2,
	#[tangram_serialize(id = 9)]
	SIGKILL = 9,
	#[tangram_serialize(id = 13)]
	SIGPIPE = 13,
	#[tangram_serialize(id = 3)]
	SIGQUIT = 3,
	#[tangram_serialize(id = 11)]
	SIGSEGV = 11,
	#[tangram_serialize(id = 15)]
	SIGTERM = 15,
	#[tangram_serialize(id = 10)]
	SIGUSR1 = 10,
	#[tangram_serialize(id = 12)]
	SIGUSR2 = 12,
}

impl std::str::FromStr for Signal {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"ABRT" => Ok(Self::SIGABRT),
			"ALRM" => Ok(Self::SIGALRM),
			"FPE" => Ok(Self::SIGFPE),
			"HUP" => Ok(Self::SIGHUP),
			"ILL" => Ok(Self::SIGILL),
			"INT" => Ok(Self::SIGINT),
			"KILL" => Ok(Self::SIGKILL),
			"PIPE" => Ok(Self::SIGPIPE),
			"QUIT" => Ok(Self::SIGQUIT),
			"SEGV" => Ok(Self::SIGSEGV),
			"TERM" => Ok(Self::SIGTERM),
			"USR1" => Ok(Self::SIGUSR1),
			"USR2" => Ok(Self::SIGUSR2),
			_ => Err(tg::error!(signal = %s, "unknown signal")),
		}
	}
}

impl std::fmt::Display for Signal {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::SIGABRT => write!(f, "ABRT"),
			Self::SIGALRM => write!(f, "ALRM"),
			Self::SIGFPE => write!(f, "FPE"),
			Self::SIGHUP => write!(f, "HUP"),
			Self::SIGILL => write!(f, "ILL"),
			Self::SIGINT => write!(f, "INT"),
			Self::SIGKILL => write!(f, "KILL"),
			Self::SIGPIPE => write!(f, "PIPE"),
			Self::SIGQUIT => write!(f, "QUIT"),
			Self::SIGSEGV => write!(f, "SEGV"),
			Self::SIGTERM => write!(f, "TERM"),
			Self::SIGUSR1 => write!(f, "USR1"),
			Self::SIGUSR2 => write!(f, "USR2"),
		}
	}
}
