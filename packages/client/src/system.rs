use crate::{return_error, Error};

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize)]
#[serde(into = "String", try_from = "String")]
pub struct System {
	arch: Arch,
	os: Os,
}

#[derive(
	Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(into = "String", try_from = "String")]
pub enum Arch {
	Aarch64,
	Js,
	X8664,
}

#[derive(
	Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(into = "String", try_from = "String")]
pub enum Os {
	Darwin,
	Js,
	Linux,
}

impl System {
	#[must_use]
	pub fn new(arch: Arch, os: Os) -> System {
		System { arch, os }
	}

	#[must_use]
	pub fn js() -> System {
		System {
			arch: Arch::Js,
			os: Os::Js,
		}
	}

	pub fn host() -> Result<System, Error> {
		let host = if false {
			unreachable!()
		} else if cfg!(all(target_arch = "aarch64", target_os = "linux")) {
			System {
				arch: Arch::Aarch64,
				os: Os::Linux,
			}
		} else if cfg!(all(target_arch = "aarch64", target_os = "macos")) {
			System {
				arch: Arch::Aarch64,
				os: Os::Darwin,
			}
		} else if cfg!(all(target_arch = "x86_64", target_os = "linux")) {
			System {
				arch: Arch::X8664,
				os: Os::Linux,
			}
		} else if cfg!(all(target_arch = "x86_64", target_os = "macos")) {
			System {
				arch: Arch::X8664,
				os: Os::Darwin,
			}
		} else {
			return_error!("Unsupported host system.");
		};
		Ok(host)
	}

	#[must_use]
	pub fn arch(&self) -> Arch {
		self.arch
	}

	#[must_use]
	pub fn os(&self) -> Os {
		self.os
	}
}

impl std::fmt::Display for System {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}-{}", self.arch, self.os)?;
		Ok(())
	}
}

impl std::str::FromStr for System {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let mut components = s.split('-');
		let (Some(arch), Some(os), None) =
			(components.next(), components.next(), components.next())
		else {
			return_error!("Unexpected number of components.");
		};
		let arch = arch.parse()?;
		let os = os.parse()?;
		Ok(System { arch, os })
	}
}

impl From<System> for String {
	fn from(value: System) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for System {
	type Error = Error;

	fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
		value.parse()
	}
}

impl std::fmt::Display for Arch {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let arch = match self {
			Arch::Aarch64 => "aarch64",
			Arch::Js => "js",
			Arch::X8664 => "x86_64",
		};
		write!(f, "{arch}")?;
		Ok(())
	}
}

impl std::str::FromStr for Arch {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let system = match s {
			"aarch64" => Arch::Aarch64,
			"js" => Arch::Js,
			"x86_64" => Arch::X8664,
			_ => return_error!(r#"Invalid arch "{s}"."#),
		};
		Ok(system)
	}
}

impl From<Arch> for String {
	fn from(value: Arch) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Arch {
	type Error = Error;

	fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
		value.parse()
	}
}

impl std::fmt::Display for Os {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let os = match self {
			Os::Darwin => "darwin",
			Os::Js => "js",
			Os::Linux => "linux",
		};
		write!(f, "{os}")?;
		Ok(())
	}
}

impl std::str::FromStr for Os {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let os = match s {
			"darwin" => Os::Darwin,
			"js" => Os::Js,
			"linux" => Os::Linux,
			_ => return_error!(r#"Invalid os "{s}"."#),
		};
		Ok(os)
	}
}

impl From<Os> for String {
	fn from(value: Os) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Os {
	type Error = Error;

	fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
		value.parse()
	}
}
