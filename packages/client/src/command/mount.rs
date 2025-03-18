use crate as tg;
use std::collections::BTreeSet;
use std::path::PathBuf;
use std::str::FromStr;

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct Mount {
	pub source: tg::artifact::Id,
	pub target: PathBuf,
}

impl Mount {
	#[must_use]
	pub fn children(&self) -> BTreeSet<tg::object::Id> {
		let object_id: tg::object::Id = self.source.clone().into();
		std::iter::once(object_id).collect()
	}
}

impl FromStr for Mount {
	type Err = tg::Error;
	fn from_str(s: &str) -> Result<Self, Self::Err> {
		// Handle the full syntax supported by tg::process::Mount, rejecting read-write.
		let s = if let Some((s, ro)) = s.split_once(",") {
			if ro == "ro" {
				s
			} else if ro == "rw" {
				return Err(tg::error!("cannot mount artifacts read-write"));
			} else {
				return Err(tg::error!("unknown option: {ro:#?}"));
			}
		} else {
			s
		};
		let (source, target) = s
			.split_once(':')
			.ok_or_else(|| tg::error!("expected a target path"))?;
		let target = PathBuf::from(target);
		if !target.is_absolute() {
			return Err(tg::error!(%target = target.display(), "expected an absolute path"));
		}
		let source = source.parse()?;
		Ok(Self {
			source,
			target: target.into(),
		})
	}
}
