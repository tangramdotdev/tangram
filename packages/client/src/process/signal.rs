use crate as tg;

pub mod get;
pub mod post;

#[derive(
	Clone, Copy, Debug, PartialEq, Eq, serde_with::DeserializeFromStr, serde_with::SerializeDisplay,
)]
#[repr(u8)]
pub enum Signal {
	SIGABRT = 6,
	SIGALRM = 14,
	SIGFPE = 8,
	SIGHUP = 1,
	SIGILL = 4,
	SIGINT = 2,
	SIGKILL = 9,
	SIGPIPE = 13,
	SIGQUIT = 3,
	SIGSEGV = 11,
	SIGTERM = 15,
	SIGUSR1 = 10,
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
			_ => Err(tg::error!(%signal = s, "unknown signal")),
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
