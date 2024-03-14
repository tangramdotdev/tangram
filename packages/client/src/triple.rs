use crate::{error, Error};
use itertools::Itertools;
use std::str::FromStr;

#[derive(
	Clone, Debug, Default, Eq, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
#[serde(into = "String", try_from = "String")]
pub struct Triple {
	string: String,
	arch: Option<Arch>,
	os: Option<Os>,
	vendor: Option<String>,
	environment: Option<Environment>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Arch {
	Aarch64,
	Js,
	X8664,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Os {
	Darwin,
	Linux,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Environment {
	Gnu,
	Musl,
}

impl Triple {
	pub fn arch_os(arch: impl std::fmt::Display, os: impl std::fmt::Display) -> Triple {
		let arch_str = arch.to_string();
		let os_str = os.to_string();
		let data = format!("{arch_str}-{os_str}");
		let maybe_arch = Arch::from_str(&arch_str).ok();
		let maybe_os = Os::parse_name(&os_str);
		Triple {
			string: data,
			arch: maybe_arch,
			os: maybe_os,
			vendor: None,
			environment: None,
		}
	}

	#[must_use]
	pub fn js() -> Triple {
		Triple {
			string: "js".to_string(),
			arch: Some(Arch::Js),
			os: None,
			vendor: None,
			environment: None,
		}
	}

	pub fn host() -> Result<Triple, Error> {
		let host = if false {
			unreachable!()
		} else if cfg!(all(target_arch = "aarch64", target_os = "linux")) {
			Triple::arch_os(Arch::Aarch64, Os::Linux)
		} else if cfg!(all(target_arch = "aarch64", target_os = "macos")) {
			Triple::arch_os(Arch::Aarch64, Os::Darwin)
		} else if cfg!(all(target_arch = "x86_64", target_os = "linux")) {
			Triple::arch_os(Arch::X8664, Os::Linux)
		} else if cfg!(all(target_arch = "x86_64", target_os = "macos")) {
			Triple::arch_os(Arch::X8664, Os::Darwin)
		} else {
			return Err(error!("unsupported host triple"));
		};
		Ok(host)
	}

	#[must_use]
	pub fn arch(&self) -> Option<Arch> {
		self.arch.clone()
	}

	#[must_use]
	pub fn environment(&self) -> Option<Environment> {
		self.environment.clone()
	}

	// e.g. "2.39" from "gnu2.39"
	#[must_use]
	pub fn environment_version(&self) -> Option<&str> {
		// Split data
		let components = self.string.split('-').collect_vec();
		if components.len() < 3 {
			return None;
		}
		// Find a part that contains the os, if any.
		if let Some(environment) = self.environment() {
			let environment = environment.to_string();
			for component in components {
				if component.starts_with(&environment) {
					// Strip the environment prefix.
					let suffix = component.strip_prefix(&environment).unwrap();
					if !suffix.is_empty() {
						return Some(suffix);
					}
				}
			}
		}
		None
	}

	#[must_use]
	pub fn os(&self) -> Option<Os> {
		self.os.clone()
	}

	// e.g. "22.3.0" from "darwin22.3.0"
	#[must_use]
	pub fn os_version(&self) -> Option<&str> {
		// Split data
		let components = self.string.split('-').collect_vec();
		if components.len() == 1 {
			return None;
		}
		// Find a part that contains the os, if any.
		if let Some(os) = self.os() {
			let os = os.to_string();
			for component in components {
				if component.starts_with(&os) {
					// Strip the os prefix.
					let suffix = component.strip_prefix(&os).unwrap();
					if !suffix.is_empty() {
						return Some(suffix);
					}
				}
			}
		}
		None
	}

	#[must_use]
	pub fn vendor(&self) -> Option<String> {
		self.vendor.clone()
	}
}

impl std::fmt::Display for Triple {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}", self.string)
	}
}

impl std::str::FromStr for Triple {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		if s.is_empty() {
			return Ok(Triple::default());
		}

		let components = s.split('-').collect_vec();
		if components.len() > 4 {
			let triple = s;
			return Err(error!(%triple, "invalid triple"));
		}

		// Use the first component as the architecture.
		let arch = Arch::from_str(components[0]).ok();

		let mut os = None;
		let mut vendor = None;
		let mut environment = None;

		if components.len() == 4 {
			// The positions are unambiguous, just assign them.
			vendor = Some(components[1].to_string());
			os = Os::from_str(components[2]).ok();
			environment = Environment::parse_name(components[3]);
		} else {
			// We have to try to figure it out. The second component could be either a vendor or an OS.
			if let Some(component) = components.get(1) {
				if let Some(parsed) = Os::parse_name(component) {
					os = Some(parsed);
				} else {
					vendor = Some((*component).to_string());
				}
			}
			if let Some(component) = components.get(2) {
				if os.is_none() {
					os = Os::parse_name(component);
				} else {
					environment = Environment::parse_name(component);
				}
			}
		}

		Ok(Triple {
			string: s.to_string(),
			arch,
			vendor,
			os,
			environment,
		})
	}
}

impl From<Triple> for String {
	fn from(value: Triple) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Triple {
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
			"aarch64" | "arm64" => Arch::Aarch64,
			"js" => Arch::Js,
			"x86_64" => Arch::X8664,
			arch => return Err(error!(%arch, "unknown architecture")),
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

impl Environment {
	#[must_use]
	pub fn parse_name(s: &str) -> Option<Self> {
		let all = vec!["gnu", "musl"];
		for name in all {
			if s.starts_with(name) {
				return Some(Environment::from_str(name).unwrap());
			}
		}
		None
	}
}

impl std::fmt::Display for Environment {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let environment = match self {
			Environment::Gnu => "gnu",
			Environment::Musl => "musl",
		};
		write!(f, "{environment}")?;
		Ok(())
	}
}

impl std::str::FromStr for Environment {
	type Err = Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let os = match s {
			"gnu" => Environment::Gnu,
			"musl" => Environment::Musl,
			environment => return Err(error!(%environment, "unknown environment")),
		};
		Ok(os)
	}
}

impl From<Environment> for String {
	fn from(value: Environment) -> Self {
		value.to_string()
	}
}

impl TryFrom<String> for Environment {
	type Error = Error;

	fn try_from(value: String) -> std::result::Result<Self, Self::Error> {
		value.parse()
	}
}

impl Os {
	#[must_use]
	pub fn parse_name(s: &str) -> Option<Self> {
		let all = vec!["darwin", "linux"];
		for name in all {
			if s.starts_with(name) {
				return Some(Os::from_str(name).unwrap());
			}
		}
		None
	}
}

impl std::fmt::Display for Os {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		let os = match self {
			Os::Darwin => "darwin",
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
			"linux" => Os::Linux,
			os => return Err(error!(%os, "unknown OS")),
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

#[cfg(test)]
mod tests {
	use super::*;
	use std::str::FromStr;

	#[test]
	fn test_triple_from_empty_string() {
		let triple = Triple::from_str("").unwrap();
		assert_eq!(triple, Triple::default());
	}

	#[test]
	fn test_js_triple() {
		let triple = Triple::js();
		assert_eq!(triple.arch(), Some(Arch::Js));
		assert_eq!(triple.os(), None);
		assert_eq!(triple.vendor(), None);
		assert_eq!(triple.environment(), None);
		assert_eq!(triple.to_string(), "js".to_string());
	}

	#[test]
	fn test_triple_from_one_component() {
		let triple = Triple::from_str("aarch64").unwrap();
		assert_eq!(triple.arch(), Some(Arch::Aarch64));
	}

	#[test]
	fn test_triple_from_two_components() {
		let triple = Triple::from_str("aarch64-darwin").unwrap();
		assert_eq!(triple.arch(), Some(Arch::Aarch64));
		assert_eq!(triple.os(), Some(Os::Darwin));

		let triple = Triple::from_str("x86_64-linux").unwrap();
		assert_eq!(triple.arch(), Some(Arch::X8664));
		assert_eq!(triple.os(), Some(Os::Linux));
	}

	#[test]
	fn test_triple_from_three_components() {
		let triple = Triple::from_str("aarch64-apple-darwin").unwrap();
		assert_eq!(triple.arch(), Some(Arch::Aarch64));
		assert_eq!(triple.vendor(), Some("apple".to_string()));
		assert_eq!(triple.os(), Some(Os::Darwin));

		let triple = Triple::from_str("arm64-apple-darwin23.3.0").unwrap(); // the result of `gcc -dumpmachine` on macOS.
		assert_eq!(triple.arch(), Some(Arch::Aarch64));
		assert_eq!(triple.vendor(), Some("apple".to_string()));
		assert_eq!(triple.os(), Some(Os::Darwin));
		assert_eq!(triple.os_version(), Some("23.3.0"));

		let triple = Triple::from_str("x86_64-linux-gnu").unwrap();
		assert_eq!(triple.arch(), Some(Arch::X8664));
		assert_eq!(triple.os(), Some(Os::Linux));
		assert_eq!(triple.environment(), Some(Environment::Gnu));
		assert_eq!(triple.environment_version(), None);

		let triple = Triple::from_str("x86_64-linux-gnu2.39").unwrap();
		assert_eq!(triple.arch(), Some(Arch::X8664));
		assert_eq!(triple.os(), Some(Os::Linux));
		assert_eq!(triple.environment(), Some(Environment::Gnu));
		assert_eq!(triple.environment_version(), Some("2.39"));
	}

	#[test]
	fn test_triple_from_four_components() {
		let triple = Triple::from_str("x86_64-pc-linux-gnu").unwrap();
		assert_eq!(triple.arch(), Some(Arch::X8664));
		assert_eq!(triple.os(), Some(Os::Linux));
		assert_eq!(triple.vendor(), Some("pc".to_string()));
		assert_eq!(triple.environment(), Some(Environment::Gnu));
		assert_eq!(triple.environment_version(), None);
	}

	#[test]
	fn test_environment_version() {
		let triple = Triple::from_str("x86_64-linux-gnu").unwrap();
		assert_eq!(triple.environment(), Some(Environment::Gnu));
		assert_eq!(triple.environment_version(), None);

		let triple = Triple::from_str("x86_64-linux-gnu2.39").unwrap();
		assert_eq!(triple.environment(), Some(Environment::Gnu));
		assert_eq!(triple.environment_version(), Some("2.39"));
	}

	#[test]
	fn test_os_version() {
		let triple = Triple::from_str("aarch64-apple-darwin").unwrap();
		assert_eq!(triple.os(), Some(Os::Darwin));
		assert_eq!(triple.os_version(), None);

		let triple = Triple::from_str("aarch64-apple-darwin22.3.0").unwrap();
		assert_eq!(triple.os(), Some(Os::Darwin));
		assert_eq!(triple.os_version(), Some("22.3.0"));
	}
}
