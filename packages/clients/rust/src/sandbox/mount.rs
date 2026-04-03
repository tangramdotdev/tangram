use {crate::prelude::*, std::path::PathBuf};

#[derive(
	Clone,
	Debug,
	serde_with::DeserializeFromStr,
	serde_with::SerializeDisplay,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
#[tangram_serialize(display, from_str)]
pub struct Mount {
	pub source: PathBuf,

	pub target: PathBuf,

	pub readonly: bool,
}

impl std::fmt::Display for Mount {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(f, "{}:{}", self.source.display(), self.target.display())?;
		if self.readonly {
			write!(f, ",ro")?;
		}
		Ok(())
	}
}

impl std::str::FromStr for Mount {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let (s, readonly) = if let Some((s, ro)) = s.split_once(',') {
			if ro == "ro" {
				(s, Some(true))
			} else if ro == "rw" {
				(s, Some(false))
			} else {
				return Err(tg::error!("unknown option: {ro:#?}"));
			}
		} else {
			(s, None)
		};
		let (source, target) = s
			.split_once(':')
			.ok_or_else(|| tg::error!("expected a target path"))?;
		let target = PathBuf::from(target);
		if !target.is_absolute() {
			return Err(tg::error!(target = %target.display(), "expected an absolute path"));
		}
		let source = source.into();
		let readonly = readonly.unwrap_or(false);
		Ok(Self {
			source,
			target,
			readonly,
		})
	}
}
