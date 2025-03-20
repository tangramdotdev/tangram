use crate as tg;
use std::str::FromStr;

pub mod get;
pub mod post;

#[derive(Clone, Copy, Debug, serde_with::DeserializeFromStr, serde_with::SerializeDisplay)]
#[repr(i32)]
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

impl FromStr for Signal {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		match s {
			"sigabrt" => Ok(Self::SIGABRT),
			"sigalrm" => Ok(Self::SIGALRM),
			"sigfpe" => Ok(Self::SIGFPE),
			"sighup" => Ok(Self::SIGHUP),
			"sigill" => Ok(Self::SIGILL),
			"sigint" => Ok(Self::SIGINT),
			"sigkill" => Ok(Self::SIGKILL),
			"sigpipe" => Ok(Self::SIGPIPE),
			"sigquit" => Ok(Self::SIGQUIT),
			"sigsegv" => Ok(Self::SIGSEGV),
			"sigterm" => Ok(Self::SIGTERM),
			"sigusr1" => Ok(Self::SIGUSR1),
			"sigusr2" => Ok(Self::SIGUSR2),
			_ => Err(tg::error!(%signal = s, "unknown signal")),
		}
	}
}

impl std::fmt::Display for Signal {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::SIGABRT => write!(f, "sigabrt"),
			Self::SIGALRM => write!(f, "sigalrm"),
			Self::SIGFPE => write!(f, "sigfpe"),
			Self::SIGHUP => write!(f, "sighup"),
			Self::SIGILL => write!(f, "sigill"),
			Self::SIGINT => write!(f, "sigint"),
			Self::SIGKILL => write!(f, "sigkill"),
			Self::SIGPIPE => write!(f, "sigpipe"),
			Self::SIGQUIT => write!(f, "sigquit"),
			Self::SIGSEGV => write!(f, "sigsegv"),
			Self::SIGTERM => write!(f, "sigterm"),
			Self::SIGUSR1 => write!(f, "sigusr1"),
			Self::SIGUSR2 => write!(f, "sigusr2"),
		}
	}
}
