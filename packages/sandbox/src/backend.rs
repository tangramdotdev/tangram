use {std::path::PathBuf, tangram_client::prelude::*};

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

impl std::str::FromStr for Kind {
	type Err = tg::Error;
	fn from_str(value: &str) -> tg::Result<Self> {
		match value {
			"builtin" => Ok(Self::Builtin),
			"bubblewrap" => Ok(Self::Bubblewrap),
			"cloud-hypervisor" => Ok(Self::CloudHypervisor),
			"gvisor" => Ok(Self::Gvisor),
			"seatbelt" => Ok(Self::Seatbelt),
			_ => Err(tg::error!(%value, "invalid sandbox backend")),
		}
	}
}

impl std::fmt::Display for Kind {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str(match self {
			Self::Builtin => "builtin",
			Self::Bubblewrap => "bubblewrap",
			Self::CloudHypervisor => "cloud-hypervisor",
			Self::Gvisor => "gvisor",
			Self::Seatbelt => "seatbelt",
		})
	}
}

impl std::str::FromStr for Platform {
	type Err = tg::Error;
	fn from_str(value: &str) -> tg::Result<Self> {
		match value {
			"kvm" => Ok(Self::Kvm),
			"systrap" => Ok(Self::Systrap),
			_ => Err(tg::error!(%value, "invalid gvisor platform")),
		}
	}
}

impl std::fmt::Display for Platform {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter.write_str(match self {
			Self::Kvm => "kvm",
			Self::Systrap => "systrap",
		})
	}
}
