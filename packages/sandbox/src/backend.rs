use std::path::PathBuf;

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case", tag = "kind")]
pub enum Backend {
	Builtin,
	Bubblewrap { path: PathBuf },
	CloudHypervisor(crate::CloudHypervisor),
	Gvisor { path: PathBuf, platform: Platform },
	Seatbelt,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Platform {
	#[default]
	Kvm,
	Systrap,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kind {
	#[default]
	Builtin,
	Bubblewrap,
	CloudHypervisor,
	Gvisor,
	Seatbelt,
}

impl Backend {
	#[must_use]
	pub fn kind(&self) -> Kind {
		match self {
			Self::Builtin => Kind::Builtin,
			Self::Bubblewrap { .. } => Kind::Bubblewrap,
			Self::CloudHypervisor(_) => Kind::CloudHypervisor,
			Self::Gvisor { .. } => Kind::Gvisor,
			Self::Seatbelt => Kind::Seatbelt,
		}
	}

	#[must_use]
	pub fn is_linux(&self) -> bool {
		matches!(
			self,
			Self::Builtin | Self::Bubblewrap { .. } | Self::Gvisor { .. }
		)
	}
}

impl Platform {
	#[must_use]
	pub const fn name(self) -> &'static str {
		match self {
			Self::Kvm => "kvm",
			Self::Systrap => "systrap",
		}
	}
}

impl Kind {
	#[must_use]
	pub const fn name(self) -> &'static str {
		match self {
			Self::Builtin => "builtin",
			Self::Bubblewrap => "bubblewrap",
			Self::CloudHypervisor => "cloud-hypervisor",
			Self::Gvisor => "gvisor",
			Self::Seatbelt => "seatbelt",
		}
	}
}
