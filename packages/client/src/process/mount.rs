use crate as tg;
use std::{path::PathBuf, str::FromStr};

#[derive(Clone, Debug)]
pub struct Mount {
	pub source: PathBuf,
	pub target: PathBuf,
	pub readonly: bool,
}

impl Mount {
	#[must_use]
	pub fn children(&self) -> Vec<tg::Object> {
		Vec::new()
	}

	#[must_use]
	pub fn data(&self) -> tg::process::data::Mount {
		let source = self.source.clone();
		let target = self.target.clone();
		let readonly = self.readonly;
		tg::process::data::Mount {
			source,
			target,
			readonly,
		}
	}
}

impl From<tg::process::data::Mount> for Mount {
	fn from(value: tg::process::data::Mount) -> Self {
		Self {
			source: value.source,
			target: value.target,
			readonly: value.readonly,
		}
	}
}

impl FromStr for Mount {
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
			return Err(tg::error!(%target = target.display(), "expected an absolute path"));
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

#[cfg(test)]
mod tests {
	#[test]
	fn parse() {
		let _mount = "./source:/target,ro"
			.parse::<super::Mount>()
			.expect("failed to parse");
		let _mount = "./source:/target,rw"
			.parse::<super::Mount>()
			.expect("failed to parse");
		let _mount = "./source:/target"
			.parse::<super::Mount>()
			.expect("failed to parse");
	}
}
