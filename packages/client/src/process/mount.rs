use crate as tg;
use crate::util::serde::{is_true, return_true};
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Mount {
	pub source: Source,
	pub target: PathBuf,
	#[serde(default = "return_true", skip_serializing_if = "is_true")]
	pub readonly: bool,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[serde(untagged)]
pub enum Source {
	Artifact(tg::artifact::Id),
	Path(PathBuf),
}

impl FromStr for Mount {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		let (s, readonly) = if let Some((s, ro)) = s.split_once(",") {
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
		let source = source
			.parse()
			.map(Source::Artifact)
			.unwrap_or_else(|_| Source::Path(source.into()));
		let readonly = match (&source, readonly) {
			(Source::Artifact(_), None | Some(true)) => true,
			(Source::Artifact(_), Some(false)) => {
				return Err(tg::error!("cannot mount artifacts read-write"));
			},
			(Source::Path(_), readonly) => readonly.unwrap_or(false),
		};

		Ok(Self {
			source,
			target: target.into(),
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
