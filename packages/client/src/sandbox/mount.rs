use {
	crate::prelude::*,
	std::path::PathBuf,
	tangram_util::serde::{is_true, return_true},
};

#[derive(
	Clone,
	Debug,
	serde::Deserialize,
	serde::Serialize,
	tangram_serialize::Deserialize,
	tangram_serialize::Serialize,
)]
pub struct Mount {
	#[tangram_serialize(id = 0)]
	pub source: tg::Either<PathBuf, tg::artifact::Id>,

	#[tangram_serialize(id = 1)]
	pub target: PathBuf,

	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	#[tangram_serialize(id = 2, default = "return_true", skip_serializing_if = "is_true")]
	pub readonly: bool,
}

impl std::fmt::Display for Mount {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match &self.source {
			tg::Either::Left(path) => write!(f, "{}", path.display())?,
			tg::Either::Right(id) => write!(f, "{id}")?,
		}
		write!(f, ":{}", self.target.display())?;
		if self.readonly {
			write!(f, ",ro")?;
		}
		Ok(())
	}
}

impl std::str::FromStr for Mount {
	type Err = tg::Error;

	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let (s, readonly) = if let Some((s, ro)) = s.rsplit_once(',') {
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
		let source = if let Ok(id) = source.parse::<tg::artifact::Id>() {
			tg::Either::Right(id)
		} else {
			tg::Either::Left(source.into())
		};
		let readonly = readonly.unwrap_or(false);
		Ok(Self {
			source,
			target,
			readonly,
		})
	}
}
